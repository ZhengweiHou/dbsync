package transfer

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/viant/dsc"
)

const maxTaskHistory = 10

//Service represents a transfer service
type Service struct {
	mux      *sync.RWMutex
	tasks    map[int]*Task
	callback func(task *Task)
}

//Tasks returns a tasks
func (s *Service) Tasks() *TasksResponse {
	var response = &TasksResponse{
		Tasks: make([]*Task, 0),
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	var taskCount = len(s.tasks)
	for k, task := range s.tasks {
		if taskCount > maxTaskHistory && task.CanEvict() {
			delete(s.tasks, k)
		}
		response.Tasks = append(response.Tasks, task)
	}
	sort.Sort(response.Tasks)
	return response
}

//Task returns a task for ID
func (s *Service) Task(id int, writer http.ResponseWriter) *Task {
	s.mux.RLock()
	defer s.mux.RUnlock()
	response, ok := s.tasks[id]
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return nil
	}
	return response
}

//hzw trans 1 Transfer transfer data
func (s *Service) Transfer(request *Request) *Response {
	log.Printf("%#v \n", s)
	log.Println("trans Transfer")
	var response = &Response{Status: StatusOk}
	rand.Seed((time.Now().UTC().UnixNano()))
	response.TaskID = int(rand.Int31())
	var task *Task
	var err error
	if err = request.Init(); err == nil {
		if err = request.Validate(); err == nil {
			log.Println("通过request创建task")
			task, err = NewTask(request) // hzw 通过request创建task
		}
	}
	if err != nil {
		response.SetError(err)
		return response
	}
	s.mux.Lock()
	task.ID = response.TaskID
	s.tasks[task.ID] = task
	s.mux.Unlock()
	task.Request = request
	if !request.Async {
		s.transfer(request, response, task) // 执行transfer
	} else {
		go s.transfer(request, response, task)
	}
	return response
}

func (s *Service) transfer(request *Request, response *Response, task *Task) {
	log.Println("transfer")
	var err error
	defer func() {
		var endTime = time.Now()
		task.EndTime = &endTime
		task.TimeTakenMs = int(task.EndTime.Sub(task.StartTime) / time.Millisecond)
		if response.Error == "" {
			task.Status = StatusDone
			response.Status = StatusDone
			response.WriteCount = int(task.WriteCount)
		} else {
			task.Status = StatusError
			response.Status = StatusError
		}
		_ = task.dest.ConnectionProvider().Close()
		_ = task.source.ConnectionProvider().Close()
	}()
	if err != nil {
		response.SetError(err)
	}

	// log.Println("readData")
	// err = s.readData(request, response, task)

	log.Println("writeData...")
	for i := 0; i < request.WriterThreads; i++ {
		log.Println("(goroutine)writeData i:", i)
		go s.writeData(request, response, task, task.transfers.transfers[i]) // 每个transfer对应一个写协程
	}

	log.Println("readData")
	err = s.readData(request, response, task)
	response.SetError(err)
	log.Println("WaitWriteCompleted...")
	task.isWriteCompleted.Wait() // hzw 等所有的task完成(为什么这里执行了库中的表才有数据？)
	log.Println("WriteCompleted!")
	if s.callback != nil {
		s.callback(task)
	}
}

func (s *Service) getTargetTable(request *Request, task *Task, batch *transferBatch) (*dsc.TableDescriptor, error) {
	table := task.dest.TableDescriptorRegistry().Get(request.Dest.Table)
	if table == nil {
		return nil, fmt.Errorf("target table %v not found", request.Dest.Table)
	}
	if len(table.Columns) == 0 && batch.size > 0 {
		table.Columns = []string{}
		for _, field := range batch.fields {
			table.Columns = append(table.Columns, field.Name)
		}
	}
	return table, nil
}

func (s *Service) writeData(request *Request, response *Response, task *Task, transfer *transfer) {
	var err error
	task.isWriteCompleted.Add(1) // 主线程会wait，添加等待数
	var count = 0
	defer func() {
		task.isWriteCompleted.Done()
		if err != nil {
			task.SetError(err)
			response.SetError(err)
			transfer.close()
		}
	}()
	var persist func(batch *transferBatch) error
	log.Println("第一次读取数据")
	batch := transfer.getBatch() // 第一次读取数据
	var table *dsc.TableDescriptor
	table, err = s.getTargetTable(request, task, batch)
	if err != nil {
		return
	}
	dmlProvider := dsc.NewMapDmlProvider(table) // 获取 date manage langrage 提供者
	sqlProvider := func(item interface{}) *dsc.ParametrizedSQL {
		return dmlProvider.Get(dsc.SQLTypeInsert, item)
	}
	connection, err := task.dest.ConnectionProvider().Get()
	if err != nil {
		return
	}
	defer func() {
		_ = connection.Close()
	}()

	persist = func(batch *transferBatch) error {
		if batch.size == 0 {
			return nil
		}
		log.Println("持久化数据 记录数", batch.size) // 调用dsc的持久化逻辑
		_, err = task.dest.PersistData(connection, batch.ranger, request.Dest.Table, dmlProvider, sqlProvider)
		if err == nil {
			atomic.AddUint64(&task.WriteCount, uint64(batch.size))
		}
		count += batch.size
		return err
	}

	if err = persist(batch); err != nil {
		return
	}
	for {
		if task.HasError() {
			break
		}
		// 循环读取数据
		log.Println("循环读取数据")
		batch := transfer.getBatch()
		if batch.size == 0 && !task.IsReading() {
			break
		}
		if err = persist(batch); err != nil {
			return
		}
	}

	log.Println("最后一次读取数据")
	err = persist(transfer.getBatch()) // 最后一次读取数据
}

func (s *Service) readData(request *Request, response *Response, task *Task) error {
	atomic.StoreInt32(&task.isReadCompleted, 0)
	var err error
	defer func() {
		atomic.StoreInt32(&task.isReadCompleted, 1)
		if err != nil {
			task.SetError(err)
			response.SetError(err)
		}
		for _, transfer := range task.transfers.transfers {
			transfer.close() // 会调用flush，通知write协程
		}
	}()

	upperCaseData := false
	lowerCaseData := false
	table := task.dest.TableDescriptorRegistry().Get(request.Dest.Table)

	if table != nil && len(table.Columns) > 0 {
		upperCaseData = isUpperCaseTable(table.Columns)
		lowerCaseData = isLowerCaseTable(table.Columns)
	}
	log.Println("source.ReadAllWithHandler...")
	err = task.source.ReadAllWithHandler(request.Source.Query, nil, func(scanner dsc.Scanner) (bool, error) {
		log.Println("ReadAllWithHandler")
		if task.HasError() {
			return false, nil
		}
		var record = make(map[string]interface{})
		task.ReadCount++

		err := scanner.Scan(&record) //从数据库读取一条数据
		if err != nil {

			return false, errors.Wrap(err, "failed to scan")
		}
		var transformed = make(map[string]interface{})
		for k, v := range record {
			if strings.ToUpper(k) == k {
				if lowerCaseData {
					transformed[strings.ToLower(k)] = v
				}
			} else if upperCaseData {
				transformed[strings.ToUpper(k)] = v
			}
		}
		if len(transformed) > 0 {
			record = transformed
		}
		log.Println("task.transfers.push(record)   transfers.index:", task.transfers.index)
		err = task.transfers.push(record) // push数据后writ协程中才能读取到数据
		return err == nil, err
	})

	return err
}

//New creates a new transfer service
func New(callback func(task *Task)) *Service {
	return &Service{
		callback: callback,
		mux:      &sync.RWMutex{},
		tasks:    make(map[int]*Task),
	}
}
