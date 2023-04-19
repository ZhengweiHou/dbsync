package merge

import (
	"dbsync/sync/contract"
	"dbsync/sync/core"
	"dbsync/sync/dao"
	"dbsync/sync/shared"
	"dbsync/sync/sql"
	"fmt"
)

//Service represents a merge service
type MergeService interface {
	//Merge merges transferred transferable into dest table
	Merge(ctx *shared.Context, transferable *core.Transferable) error
	//Delete removes data from dest table for supplied filter, filter can not be emtpty or error
	Delete(ctx *shared.Context, filter map[string]interface{}) error

	SaveIdsTable(ctx *shared.Context) error
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
	// idColumns := s.Sync.IDColumns
	DML, _ := s.Builder.DML(shared.DMLInsertSelectWithKeyColumns, shared.IdsTableSuffix, nil)
	return s.dao.ExecSQL(ctx, DML)
	// insert into student2_keystmp (ID ,NaME) select ID ,NaME from STUDENT2
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
