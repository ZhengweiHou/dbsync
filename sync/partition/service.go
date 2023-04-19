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
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
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
	diff.DiffService
	Transfer transfer.TransferService
	Merger   merge.MergeService
	DbSync   *contract.Sync
	job      jobs.JobService
	history  history.HistoryService
	*strategy.Strategy
	dao      dao.DaoService
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
		return nil
	}
	if err = s.buildBatched(ctx); err == nil {
		err = s.buildIndividual(ctx)
	}
	return err
}

func (s *partitionService) Sync(ctx *shared.Context) (err error) {
	if s.Partitions == nil {
		return fmt.Errorf("partitions were empy")
	}
	switch s.PartitionConf.SyncMode {
	case shared.SyncModeBatch:
		err = s.syncInBatches(ctx)
	default:
		err = s.syncIndividually(ctx, s.Partitions)
	}
	return err
}

func (s *partitionService) syncInBatches(ctx *shared.Context) error {
	batchMap := criteria.NewBatchMap(s.PartitionConf.BatchSize)
	_ = s.Partitions.Range(func(partition *core.Partition) error {
		inSync, err := partition.Status.InSync()
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
	isBatched := s.PartitionConf.SyncMode == shared.SyncModeBatch
	if isBatched {
		err = s.mergeBatch(ctx, partitions) // batch模式下最终目标表转移
	}
	if s.SaveIdsTable { // 保存指定Id字段小表数据
		err = s.saveIdsTable(ctx)
	}
	if err == nil {
		err = s.removePartitions(ctx)
	}

	return err
}

func (s *partitionService) syncIndividually(ctx *shared.Context, partitions *core.Partitions) (err error) {
	isChunked := s.Chunk.Size > 0
	isBatched := s.PartitionConf.SyncMode == shared.SyncModeBatch
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
	err = partitions.Range(func(partition *core.Partition) error {
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
			if err = s.syncPartition(ctx, partition); err == nil {
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
	isBatchMode := s.PartitionConf.SyncMode == shared.SyncModeBatch
	if s.DirectAppend && partition.Transferable.Method == shared.SyncMethodInsert {
		partition.Transferable.Suffix = ""
		partition.IsDirect = true
	}
	request := s.Transfer.NewRequest(ctx, &partition.Transferable)
	job := s.job.Get(ctx.ID)
	if job == nil {
		return fmt.Errorf("job was empty: %v", ctx.ID)
	}
	job.Add(&partition.Transferable)
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
	err = s.Merger.Merge(ctx, transferable)
	return err
}

func (s *partitionService) buildBatched(ctx *shared.Context) (err error) {
	partitionsCriteria := s.Partitions.Criteria()
	for i := range partitionsCriteria {
		if err = s.buildBatch(ctx, partitionsCriteria[i]); err != nil {
			break
		}
	}
	return err
}

//build build status to all  partition, ideally al should be done in batch
func (s *partitionService) buildIndividual(ctx *shared.Context) (err error) {
	err = s.Partitions.Range(func(partition *core.Partition) error {
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

	sourceSignatures, err := s.dao.Signatures(ctx, contract.ResourceKindSource, filter)
	if err != nil {
		return err
	}
	if len(sourceSignatures) == 0 {
		return nil //nothing to sync
	}
	if len(destSignatures) == 0 {
		destSignatures, err = s.dao.Signatures(ctx, contract.ResourceKindDest, filter)
		if err != nil {
			return err
		}
	}
	core.AlignRecords(sourceSignatures, destSignatures)
	dateLayout := s.Partitions.FindDateLayout(sourceSignatures[0])
	index, err := s.match(ctx, sourceSignatures, destSignatures, dateLayout)
	if err != nil {
		return err
	}
	if err = s.validate(ctx, index); err != nil {
		return err
	}

	for key := range index.Source {
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
		partition.Status, err = s.Check(ctx, source, dest, partition.Filter)
		if err != nil {
			return err
		}
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
	// 此处在分片处理时存在问题，临时注释 by:houzw
	// result := core.NewIndex()
	// if len(source) == 1 {
	// 	result.Source[""] = source[0]
	// 	result.Dest[""] = nil
	// 	if len(dest) > 0 {
	// 		result.Dest[""] = dest[0]
	// 	}
	// 	return result, nil
	// }
	indexer := core.NewIndexer(s.PartitionConf.Columns, dateLayout)
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

func (s *partitionService) saveIdsTable(ctx *shared.Context) error {
	// 判断keyColumn是否为空 TODO
	if len(s.IDColumns) < 1 {
		logrus.Warn("saveIdsTable, IDColumns is nil, can't saveIdsTable")
		return nil
	}
	_ = s.dao.RecreateIdsTable(ctx, shared.IdsTableSuffix)
	// 2.creat select
	return s.Merger.SaveIdsTable(ctx)
}

func newService(sync *contract.Sync, dao dao.DaoService, mutex *shared.Mutex, jobService jobs.JobService, historyService history.HistoryService) *partitionService {
	return &partitionService{
		dao:         dao,
		DiffService: diff.New(sync, dao),
		Comparator:  core.NewComparator(&sync.Diff),
		Merger:      merge.New(sync, dao, mutex),
		Transfer:    transfer.New(sync, dao),
		DbSync:      sync,
		Strategy:    &sync.Strategy,
		Mutex:       mutex,
		job:         jobService,
		history:     historyService,
	}
}

//New creates new partition service
func New(sync *contract.Sync, dao dao.DaoService, mutex *shared.Mutex, jobService jobs.JobService, historyService history.HistoryService) PartitionService {
	return newService(sync, dao, mutex, jobService, historyService)
}
