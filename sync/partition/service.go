package partition

import (
	"dbsync/sync/chunk"
	"dbsync/sync/contract"
	"dbsync/sync/contract/strategy"
	"dbsync/sync/core"
	"dbsync/sync/criteria"
	"dbsync/sync/dao"
	"dbsync/sync/diff"
	"dbsync/sync/history"
	"dbsync/sync/jobs"
	"dbsync/sync/merge"
	"dbsync/sync/shared"
	"dbsync/sync/transfer"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

//Service represents partitin service
type PartitionService interface {
	//Build build sync partitions
	Build(ctx *shared.Context) error

	//Sync transfer and merges build sync partitions
	Sync(ctx *shared.Context) error

	//Init initializes service
	Init(ctx *shared.Context) error

	//Closes sync resource (db connections)
	Close() error
}

//service represents partition
type partitionService struct {
	Partitions *core.Partitions
	Mutex      *shared.Mutex
	*core.Comparator
	diff.Service
	Transfer transfer.Service
	Merger   merge.Service
	DbSync   *contract.Sync
	job      jobs.Service
	history  history.Service
	*strategy.Strategy
	dao      dao.Service
	toRemove []*core.Partition
}

//Close closes this service
func (s *partitionService) Close() error {
	s.Partitions.Close()
	return s.dao.Close()
}

//Build build partition sync status
func (s *partitionService) Build(ctx *shared.Context) (err error) {
	if !s.IsOptimized() {
		return nil // strategy.force=true
	}
	log.Println("--buildBatched")
	if err = s.buildBatched(ctx); err == nil {
		log.Println("--buildIndividual")
		err = s.buildIndividual(ctx)
	}
	return err
}

func (s *partitionService) Sync(ctx *shared.Context) (err error) {
	if s.Partitions == nil {
		return fmt.Errorf("partitions were empy")
	}
	log.Println("--:Partition.SyncMode:", s.Partition.SyncMode)
	switch s.Partition.SyncMode {
	case shared.SyncModeBatch:
		log.Println("--syncInBatches")
		err = s.syncInBatches(ctx)
	default:
		log.Println("--syncIndividually")
		err = s.syncIndividually(ctx, s.Partitions)
	}
	return err
}

func (s *partitionService) syncInBatches(ctx *shared.Context) error {
	batchMap := criteria.NewBatchMap(s.Partition.BatchSize)
	_ = s.Partitions.Range(func(partition *core.Partition) error {
		inSync, err := partition.Status.InSync() // TODO 为什么？？？是true
		log.Printf("inSync:%v\n", inSync)
		if err != nil {
			return err
		}
		if inSync {
			s.job.Get(ctx.ID).Add(&partition.Transferable)
			return nil
		}
		batchMap.Add(partition.Status.Method, partition.Filter)
		return nil
	})

	var partitionData = make([]*core.Partition, 0)
	_ = batchMap.Range(func(key string, batch *criteria.Batch) error {
		items := batch.Get()

		for i := range items {
			partition := core.NewPartition(s.Strategy, items[i])
			partition.InitWithMethod(key, fmt.Sprintf("_%s%05d", key[0:1], i+1))
			partitionData = append(partitionData, partition)

			source, dest, err := s.FetchAll(ctx, items[i])
			if err == nil {
				partition.Source = source.Signature(s.IDColumn())
				partition.Dest = dest.Signature(s.IDColumn())
			}
			status := partition.Status
			ctx.Log(fmt.Sprintf("(%v): in sync: %v, %v\n", partition.Filter, status.InSync, status.Method))

		}
		return nil
	})
	partitions := core.NewPartitions(partitionData, s.Strategy)
	return s.syncIndividually(ctx, partitions)
}

func (s *partitionService) mergeBatch(ctx *shared.Context, partitions *core.Partitions) (err error) {
	transferable := partitions.BatchTransferable()
	return s.Merger.Merge(ctx, transferable)
}

func (s *partitionService) onSyncDone(ctx *shared.Context, partitions *core.Partitions) (err error) {
	isBatched := s.Partition.SyncMode == shared.SyncModeBatch
	if isBatched {
		err = s.mergeBatch(ctx, partitions)
	}
	if err == nil {
		err = s.removePartitions(ctx)
	}
	return err
}

func (s *partitionService) syncIndividually(ctx *shared.Context, partitions *core.Partitions) (err error) {
	isChunked := s.Chunk.Size > 0
	isBatched := s.Partition.SyncMode == shared.SyncModeBatch
	log.Println("---:isChunked:", isChunked, " isBatched:", isBatched)
	defer func() {
		if err == nil {
			err = s.onSyncDone(ctx, partitions)
		}
	}()

	if isBatched {
		if err = s.dao.RecreateTransientTable(ctx, shared.TransientTableSuffix); err != nil {
			return err
		}
	}
	total := uint32(0)
	inSync := uint32(0)
	//This run with multi go routines
	log.Println("---partitions.Range() 执行 syncPartition")
	err = partitions.Range(func(partition *core.Partition) error {

		log.Printf("----partitions.Range syncIndividually handler %v\n", partition.Status)
		if partition.Status == nil {
			return nil
		}
		if atomic.AddUint32(&total, 1)%10 == 0 {
			ctx.Log(fmt.Sprintf("InSync: %v/%v\n", inSync, total))
		}
		isSync, err := partition.InSync()
		if err != nil {
			partition.SetError(err)
			return err
		}
		if isSync {
			atomic.AddUint32(&inSync, 1)
			s.job.Get(ctx.ID).Add(&partition.Transferable)
			return nil
		}

		if partition.Status.InSyncWithID > 0 {
			partition.SetMinID(s.IDColumn(), partition.Status.InSyncWithID+1)
		}

		if isChunked {
			return s.syncPartitionChunks(ctx, partition)
		}

		//Added retries
		for i := 0; i < shared.MaxRetries; i++ {

			log.Println("----syncPartition")
			if err = s.syncPartition(ctx, partition); err == nil { // hzw 执行数据同步 发送请求到transf
				return err
			}
			time.Sleep(time.Second)
		}
		partition.SetError(err)
		return err
	})
	return err
}

func (s *partitionService) syncPartitionChunks(ctx *shared.Context, partition *core.Partition) (err error) {
	chunker := chunk.New(s.DbSync, partition, s.dao, s.Mutex, s.job, s.Transfer)
	if err = chunker.Sync(ctx); err == nil {
		err = partition.Error()
	}
	return err
}

func (s *partitionService) syncPartition(ctx *shared.Context, partition *core.Partition) (err error) {
	isBatchMode := s.Partition.SyncMode == shared.SyncModeBatch
	if s.DirectAppend && partition.Transferable.Method == shared.SyncMethodInsert {
		partition.Transferable.Suffix = ""
		partition.IsDirect = true
	}
	log.Println("s.Transfer.NewRequest")
	request := s.Transfer.NewRequest(ctx, &partition.Transferable) // hzw 创建Transfer的请求
	job := s.job.Get(ctx.ID)
	if job == nil {
		return fmt.Errorf("job was empty: %v", ctx.ID)
	}
	job.Add(&partition.Transferable)

	log.Println("s.Transfer.Post")
	if err = s.Transfer.Post(ctx, request, &partition.Transferable); err != nil {
		return err
	}
	if partition.IsDirect {
		return nil
	}
	transferable := partition.Transferable.Clone()
	//Only merge/append can be batched
	if isBatchMode && !transferable.ShouldDelete() {
		transferable.OwnerSuffix = shared.TransientTableSuffix
		transferable.Method = shared.SyncMethodInsert
	}

	log.Println("s.Merger.Merge")
	err = s.Merger.Merge(ctx, transferable)
	return err
}

func (s *partitionService) buildBatched(ctx *shared.Context) (err error) {
	log.Println("---Partitions.Criteria")
	partitionsCriteria := s.Partitions.Criteria()
	for i := range partitionsCriteria {
		log.Println("---buildBatch")
		if err = s.buildBatch(ctx, partitionsCriteria[i]); err != nil {
			break
		}
	}
	return err
}

//build build status to all  partition, ideally al should be done in batch
func (s *partitionService) buildIndividual(ctx *shared.Context) (err error) {
	err = s.Partitions.Range(func(partition *core.Partition) error {
		log.Println("----partitions.Range buildIndividual handler")
		if partition.Status != nil {
			return nil
		}
		source, dest, err := s.Fetch(ctx, partition.Filter)
		if err != nil {
			return err
		}
		if partition.Status, err = s.Check(ctx, source, dest, partition.Filter); err == nil {
			partition.Init()
		}
		log.Println("=build build status to partition status:", partition.Status)

		return err
	})
	return err

}

func (s *partitionService) buildBatch(ctx *shared.Context, filter map[string]interface{}) error {
	isGlobalPartition := len(s.Partitions.Source) <= 1
	syncOnlyNewID := s.Strategy.Diff.NewIDOnly && s.IDColumn() != ""
	shouldUseNewIDGlobalFilter := isGlobalPartition && syncOnlyNewID

	var destSignatures core.Records

	if shouldUseNewIDGlobalFilter {
		if dest, err := s.dao.Signature(ctx, contract.ResourceKindDest, filter); err == nil {
			destSignature := core.NewSignatureFromRecord(s.Strategy.IDColumn(), dest)
			filter[s.Strategy.IDColumn()] = criteria.NewGraterThan(destSignature.Max())
			destSignatures = append(destSignatures, dest)
		}
	}

	log.Println("---- get sourceSignatures")
	sourceSignatures, err := s.dao.Signatures(ctx, contract.ResourceKindSource, filter) // hzw 查询源数据聚合结果，可能会有多个记录吗？
	if err != nil {
		return err
	}
	if len(sourceSignatures) == 0 {
		return nil //nothing to sync
	}
	if len(destSignatures) == 0 {

		log.Println("---- get destSignatures")
		destSignatures, err = s.dao.Signatures(ctx, contract.ResourceKindDest, filter) // hzw 查询目标数据聚合结果
		if err != nil {
			return err
		}
	}
	core.AlignRecords(sourceSignatures, destSignatures) // 对齐记录值数据类型
	dateLayout := s.Partitions.FindDateLayout(sourceSignatures[0])
	log.Println("---- 聚合结果匹配，产生core.Index")
	index, err := s.match(ctx, sourceSignatures, destSignatures, dateLayout) // hzw 聚合结果匹配
	if err != nil {
		return err
	}
	if err = s.validate(ctx, index); err != nil {
		return err
	}

	log.Println("---- Index.source遍历 len(index.Source):", len(index.Source))
	for key := range index.Source { // hzw source聚合结果集遍历(会有多个结果集？？)
		log.Println("range index.Source key:", key)
		source := index.Source[key]
		dest := index.Dest[key]
		partition := s.Partitions.Get(key)
		if partition == nil {
			if key == "" {
				partition = s.Partitions.Source[0]

			} else {
				continue
			}
		}
		if syncOnlyNewID && len(destSignatures) > 0 {
			partition.Status = core.NewStatusWithNewID(s.IDColumn(), source, dest)
			partition.AddCriteria(s.IDColumn(), criteria.NewGraterThan(partition.Status.InSyncWithID))
			partition.Init()
			continue
		}
		log.Printf("---- 聚合结果集比对检查,%v\n", partition.Filter)
		partition.Status, err = s.Check(ctx, source, dest, partition.Filter) // hzw 聚合结果集比对检查
		tmpInsync, _ := partition.Status.InSync()
		log.Printf("---- 聚合结果集比对检查, filter:%v 结果:%v\n", partition.Filter, tmpInsync)
		if err != nil {
			return err
		}
		log.Println("---- partition.Init")
		partition.Init()
	}
	return nil
}

func (s *partitionService) validate(ctx *shared.Context, index *core.Index) error {
	for key := range index.Source {
		source := index.Source[key]
		dest := index.Dest[key]
		if err := s.Partitions.Validate(ctx, s.Comparator, source, dest); err != nil {
			return err
		}
	}
	return nil

}

func (s *partitionService) match(ctx *shared.Context, source, dest core.Records, dateLayout string) (*core.Index, error) {
	// result := core.NewIndex()
	// if len(source) == 1 { // TODO houzw: 此处为什么这么控制？分片处理时有问题
	// if len(source) == 9999 { // TODO 临时测试
	// 	result.Source[""] = source[0]
	// 	result.Dest[""] = nil
	// 	if len(dest) > 0 {
	// 		result.Dest[""] = dest[0]
	// 	}
	// 	return result, nil
	// }
	indexer := core.NewIndexer(s.Partition.Columns, dateLayout)
	index := indexer.Index(source, dest)
	if hasDest := len(index.Dest) > 0; hasDest {
		return index, nil
	}

	//if no even one batch dest has been found, try to get individual dest
	for key := range index.Source {
		partition := s.Partitions.Get(key)
		if partition == nil {
			continue
		}
		if dest, _ := s.dao.Signatures(ctx, contract.ResourceKindDest, partition.Filter); len(dest) == 1 {
			index.Dest[key] = dest[0]
		}
	}
	return index, nil
}

func (s *partitionService) Init(ctx *shared.Context) error {
	if s.Strategy == nil {
		return fmt.Errorf("strategy was empty")
	}
	return s.loadPartitions(ctx)
}

func (s *partitionService) fetchPartitionValues(ctx *shared.Context, kind contract.ResourceKind) ([]*core.Partition, error) {
	var result = make([]*core.Partition, 0)
	values, err := s.dao.Partitions(ctx, kind)
	if err != nil {
		return nil, err
	}
	for i := range values {
		result = append(result, core.NewPartition(s.Strategy, values[i]))
	}
	return result, nil
}

func (s *partitionService) loadPartitions(ctx *shared.Context) (err error) {
	var source = make([]*core.Partition, 0)
	var dest = make([]*core.Partition, 0)

	if s.DbSync.Source.PartitionSQL != "" {
		if source, err = s.fetchPartitionValues(ctx, contract.ResourceKindSource); err != nil {
			return err
		}

		if s.DbSync.Dest.PartitionSQL != "" {
			if dest, err = s.fetchPartitionValues(ctx, contract.ResourceKindDest); err != nil {
				return err
			}
		}

		if len(source) == 0 {
			if len(dest) == 0 {
				return errors.New("source partitions query returned empty set")
			}
			source = append(source, dest[0])
		}

	} else if s.Force {
		partition := core.NewPartition(s.Strategy, map[string]interface{}{})
		partition.Status = &core.Status{
			Method: shared.SyncMethodDeleteMerge,
			Source: &core.Signature{},
			Dest:   &core.Signature{},
		}
		if s.DbSync.AppendOnly {
			partition.Status.Method = shared.SyncMethodInsert
		}
		partition.Init()
		source = append(source, partition)
	}

	s.Partitions = core.NewPartitions(source, s.Strategy)
	s.Partitions.Init()
	s.buildRemoved(source, dest)
	return nil
}

func (s *partitionService) buildRemoved(source []*core.Partition, dest []*core.Partition) {
	if s.DbSync.Dest.PartitionSQL == "" {
		return
	}
	s.toRemove = make([]*core.Partition, 0)
	var index = make(map[string]bool)
	for i := range source {
		index[source[i].Suffix] = true
	}

	for i := range dest {
		dest[i].Init()
		_, has := index[dest[i].Suffix]
		if !has {
			s.toRemove = append(s.toRemove, dest[i])
		}
	}
}

func (s *partitionService) removePartitions(ctx *shared.Context) error {
	if len(s.toRemove) == 0 {
		return nil
	}
	for i := range s.toRemove {
		if len(s.toRemove[i].Filter) == 0 {
			continue
		}
		if err := s.Merger.Delete(ctx, s.toRemove[i].Filter); err != nil {
			return err
		}
	}
	return nil
}

func newService(sync *contract.Sync, dao dao.Service, mutex *shared.Mutex, jobbService jobs.Service, historyService history.Service) *partitionService {
	return &partitionService{
		dao:        dao,
		Service:    diff.New(sync, dao),
		Comparator: core.NewComparator(&sync.Diff),
		Merger:     merge.New(sync, dao, mutex),
		Transfer:   transfer.New(sync, dao),
		DbSync:     sync,
		Strategy:   &sync.Strategy,
		Mutex:      mutex,
		job:        jobbService,
		history:    historyService,
	}
}

//New creates new partition service
func New(sync *contract.Sync, dao dao.Service, mutex *shared.Mutex, jobService jobs.Service, historyService history.Service) PartitionService {
	return newService(sync, dao, mutex, jobService, historyService)
}
