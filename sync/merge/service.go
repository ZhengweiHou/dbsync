package merge

import (
	"dbsync/sync/contract"
	"dbsync/sync/core"
	"dbsync/sync/dao"
	"dbsync/sync/shared"
	"dbsync/sync/sql"
	"fmt"
	"strings"

	"github.com/viant/dsc"
)

//Service represents a merge service
type MergeService interface {
	//Merge merges transferred transferable into dest table
	Merge(ctx *shared.Context, transferable *core.Transferable) error
	//Delete removes data from dest table for supplied filter, filter can not be emtpty or error
	Delete(ctx *shared.Context, filter map[string]interface{}) error

	SaveIdsTable(ctx *shared.Context) error
	SyncFlashback(ctx *shared.Context) error
}

type mergeService struct {
	Sync *contract.Sync
	dao  dao.DaoService
	*sql.Builder
	*shared.Mutex
}

func (s *mergeService) delete(ctx *shared.Context, transferable *core.Transferable) error {
	deleteFilter := shared.CloneMap(transferable.Filter)
	DML, err := s.Builder.DML(shared.DMLDelete, transferable.Suffix, deleteFilter)
	if err != nil {
		return err
	}
	transferable.DML = DML
	return s.dao.ExecSQL(ctx, DML)
}

func (s *mergeService) append(ctx *shared.Context, transferable *core.Transferable) error {
	DML := s.Builder.AppendDML(transferable.Suffix, transferable.OwnerSuffix)
	var err error
	transferable.DML = DML
	if err = s.dao.ExecSQL(ctx, DML); err == nil {
		err = s.dao.DropTransientTable(ctx, transferable.Suffix)
	}
	return err
}

//dedupeAppend removed all record from transient table that exist in dest, then appends only new
func (s *mergeService) dedupeAppend(ctx *shared.Context, transferable *core.Transferable) (err error) {
	if len(s.Sync.IDColumns) == 0 {
		return s.append(ctx, transferable)
	}
	if transferable.OwnerSuffix == "" {
		DML, _ := s.Builder.DML(shared.TransientDMLDelete, transferable.Suffix, shared.CloneMap(transferable.Filter))
		transferable.DML = DML
		if err = s.dao.ExecSQL(ctx, DML); err != nil {
			return err
		}
	}
	return s.append(ctx, transferable)
}

func (s *mergeService) merge(ctx *shared.Context, transferable *core.Transferable) error {
	DML, err := s.Builder.DML(s.MergeStyle, transferable.Suffix, shared.CloneMap(transferable.Filter))
	if err != nil {
		return err
	}
	transferable.DML = DML
	if err = s.dao.ExecSQL(ctx, DML); err == nil {
		err = s.dao.DropTransientTable(ctx, transferable.Suffix)
	}
	return err
}

//Delete delete data from dest table for supplied filter
func (s *mergeService) Delete(ctx *shared.Context, filter map[string]interface{}) error {
	DML, _ := s.Builder.DML(shared.DMLFilteredDelete, "", filter)
	return s.dao.ExecSQL(ctx, DML)
}

//Merge merges data for supplied transferable
func (s *mergeService) Merge(ctx *shared.Context, transferable *core.Transferable) (err error) {
	if transferable.IsDirect {
		return fmt.Errorf("transferable was direct")
	}
	if s.AppendOnly {

		if transferable.ShouldDelete() {
			if ctx.UseLock {
				s.Mutex.Lock(s.Sync.Table)
			}
			err = s.delete(ctx, transferable)
			if ctx.UseLock {
				s.Mutex.Unlock(s.Sync.Table)
			}
		}
		if err == nil {
			err = s.dedupeAppend(ctx, transferable)
		}
		return err
	} else if s.AppendUpdateOnly {
		return s.merge(ctx, transferable)
	}
	switch transferable.Method {
	case shared.SyncMethodDeleteInsert:
		if ctx.UseLock {
			s.Mutex.Lock(s.Sync.Table)
		}
		err = s.delete(ctx, transferable)
		if ctx.UseLock {
			s.Mutex.Unlock(s.Sync.Table)
		}
		if err == nil {
			err = s.dedupeAppend(ctx, transferable)
		}
		return err
	case shared.SyncMethodDeleteMerge:
		if ctx.UseLock {
			s.Mutex.Lock(s.Sync.Table)
		}
		if err = s.delete(ctx, transferable); err == nil {
			err = s.merge(ctx, transferable)
		}
		if ctx.UseLock {
			s.Mutex.Unlock(s.Sync.Table)
		}
		return err
	case shared.SyncMethodMerge:
		if ctx.UseLock {
			s.Mutex.Lock(s.Sync.Table)
		}
		err = s.merge(ctx, transferable)
		if ctx.UseLock {
			s.Mutex.Unlock(s.Sync.Table)
		}
		return err
	case shared.SyncMethodInsert:
		return s.dedupeAppend(ctx, transferable)
	}
	return fmt.Errorf("unknown transfer method: %v", transferable.Method)
}

func (s *mergeService) SaveIdsTable(ctx *shared.Context) (err error) {
	DML, _ := s.Builder.DML(shared.DMLInsertSelectWithKeyColumns, shared.IdsTableSuffix, nil)
	return s.dao.ExecSQL(ctx, DML)
}

func (s *mergeService) SyncFlashback(ctx *shared.Context) error {

	strings.Join(s.IDColumns, ",")

	dcSource := s.dao.Source()
	batchSize := s.PartitionConf.BatchSize
	batchRecords := make([][]interface{}, 0)

	dcDest := s.dao.Dest()
	destCon, err := dcDest.DB.ConnectionProvider().Get()
	if err != nil {
		return err
	}

	deletetemp, valuestr := s.Builder.DMLDeleteByKeyInValues()

	defer destCon.Close()
	err = destCon.Begin() // 回删任务保持一个事务
	if err != nil {
		return err
	}
	// 查询待回删的主键列表
	selectsql := s.Builder.DDLFromFlashbackSelect(shared.IdsTableSuffix)
	ctx.Log(selectsql)
	err = dcSource.DB.ReadAllWithHandler(selectsql, nil, func(scanner dsc.Scanner) (toContinue bool, err error) {
		var record = make([]interface{}, 2)
		var recordPointers = make([]interface{}, 2)
		for i := range record {
			recordPointers[i] = &record[i] // 将原切片中元素的指针取出
		}
		err = scanner.Scan(recordPointers...)
		if err != nil {
			return false, err
		}

		// 格式转换
		for i := range record {
			rawValue := record[i]
			b, ok := rawValue.([]byte)
			if ok {
				record[i] = string(b)
			}
		}

		batchRecords = append(batchRecords, record)
		if len(batchRecords) >= batchSize {
			err = s.flashbackInIds(ctx, &destCon, batchRecords, deletetemp, valuestr)
			if err != nil {
				return false, err
			}
			batchRecords = make([][]interface{}, 0)
		}
		return true, nil
	})
	if err != nil {
		destCon.Rollback()
		return err
	}

	err = s.flashbackInIds(ctx, &destCon, batchRecords, deletetemp, valuestr)
	if err != nil {
		destCon.Rollback()
		return err
	}
	return destCon.Commit() // 所有删除都正常则提交事务
}

func (s *mergeService) flashbackInIds(ctx *shared.Context, con *dsc.Connection, batchIds [][]interface{}, deletetemp, valuestr string) error {
	if len(batchIds) == 0 {
		return nil
	}

	var valuestrs = make([]string, 0)
	var params = make([]interface{}, 0)
	for _, idRecod := range batchIds {
		valuestrs = append(valuestrs, valuestr)
		params = append(params, idRecod...)
	}

	flashbackSQL := fmt.Sprintf(deletetemp, strings.Join(valuestrs, ","))

	ctx.Log(flashbackSQL)
	result, err := s.dao.Dest().DB.ExecuteOnConnection(*con, flashbackSQL, params)
	if err != nil {
		return err
	}
	ctx.Log(result)

	return nil
}

//New creates a new
func New(sync *contract.Sync, dao dao.DaoService, mutex *shared.Mutex) MergeService {
	return &mergeService{
		Sync:    sync,
		dao:     dao,
		Builder: dao.Builder(),
		Mutex:   mutex,
	}
}
