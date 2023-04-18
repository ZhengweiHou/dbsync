package diff

import (
	"dbsync/sync/contract"
	"dbsync/sync/core"
	"dbsync/sync/criteria"
	"dbsync/sync/dao"
	"dbsync/sync/shared"
	"fmt"
	"github.com/pkg/errors"
)

//Service represents a diff service
type DiffService interface {
	//Check compares source and dest record and returns sync status
	Check(ctx *shared.Context, source, dest core.Record, filter map[string]interface{}) (*core.Status, error)

	//UpdateStatus updates sync status for supplied source and dest, it uses narrowInSyncSubset to find MAX ID in dest that is in sync with the source
	UpdateStatus(ctx *shared.Context, status *core.Status, source, dest core.Record, filter map[string]interface{}, narrowInSyncSubset bool) (err error)

	//Fetch returns source and dest raw signature data or error
	Fetch(ctx *shared.Context, filter map[string]interface{}) (source, dest core.Record, err error)

	//FetchAll returns source and dest raw signature data or error
	FetchAll(ctx *shared.Context, filter map[string]interface{}) (source, dest core.Records, err error)
}

//service finds source and dest difference status
type diffService struct {
	dao dao.DaoService
	*contract.Sync
	*core.Comparator
}

//Check checks source and dest difference status
func (d *diffService) Check(ctx *shared.Context, source, dest core.Record, filter map[string]interface{}) (*core.Status, error) {
	result := &core.Status{}
	err := d.UpdateStatus(ctx, result, source, dest, filter, true)
	return result, err
}

//Fetch reads source and dest signature records for supplied filter
func (d *diffService) Fetch(ctx *shared.Context, filter map[string]interface{}) (source, dest core.Record, err error) {

	if d.Diff.NewIDOnly && d.IDColumn() != "" {
		key := d.IDColumn()
		_, hasIDCriteria := filter[key]
		if hasIDCriteria {
			dest = core.Record{}
			source, err = d.dao.Signature(ctx, contract.ResourceKindSource, filter)
			return source, dest, nil
		}

		if dest, err = d.dao.Signature(ctx, contract.ResourceKindDest, filter); err == nil {
			destSignature := core.NewSignatureFromRecord(d.IDColumn(), dest)
			if len(filter) == 0 {
				filter = make(map[string]interface{})
			}
			key := d.IDColumn()
			if _, has := filter[key]; has {
				key += " "
			}
			filter[key] = criteria.NewGraterThan(destSignature.Max())
		}
	}

	if source, err = d.dao.Signature(ctx, contract.ResourceKindSource, filter); err != nil {
		return nil, nil, err
	}
	dest, err = d.dao.Signature(ctx, contract.ResourceKindDest, filter)
	return source, dest, err
}

//Fetch reads source and dest signature records for supplied filter
func (d *diffService) FetchAll(ctx *shared.Context, filter map[string]interface{}) (source, dest core.Records, err error) {
	if source, err = d.dao.Signatures(ctx, contract.ResourceKindSource, filter); err != nil {
		return nil, nil, err
	}
	dest, err = d.dao.Signatures(ctx, contract.ResourceKindDest, filter)
	return source, dest, err
}

func (d *diffService) UpdateStatus(ctx *shared.Context, status *core.Status, source, dest core.Record, filter map[string]interface{}, narrowInSyncSubset bool) (err error) {
	inSync := false
	defer func() {
		ctx.Log(fmt.Sprintf("(%v): in sync: %v, %v\n", filter, inSync, status.Method))
	}()
	inSync = d.Comparator.IsInSync(ctx, source, dest)
	status.SetInSync(inSync)
	if inSync {
		return nil
	}
	idColumn := d.Sync.IDColumn()
	hasID := idColumn != ""
	status.Source = core.NewSignatureFromRecord(idColumn, source)
	status.Dest = core.NewSignatureFromRecord(idColumn, dest)

	if err := status.Dest.ValidateIDConsistency(); err != nil {
		return errors.Wrap(err, "dest inconsistency")
	}

	if len(dest) == 0 || status.Dest.Count() == 0 {
		status.Method = shared.SyncMethodInsert
		return nil
	}

	if !hasID {
		status.Method = shared.SyncMethodDeleteInsert
		return nil
	}

	if status.Dest.Max() > status.Source.Max() || status.Dest.Count() > status.Source.Count() ||
		(status.Dest.Min() > 0 && status.Dest.Min() < status.Source.Min()) {
		status.Method = shared.SyncMethodDeleteMerge
		return nil
	}
	if !narrowInSyncSubset {
		return nil
	}

	if status.Dest.Max() <= 0 {
		status.Method = shared.SyncMethodMerge
		return nil
	}

	checker := func() error {
		narrowFilter := shared.CloneMap(filter)
		narrowFilter[idColumn] = criteria.NewLessOrEqual(status.Dest.Max())
		if d.isInSync(ctx, narrowFilter) {
			status.Method = shared.SyncMethodInsert
			status.SetInSyncWithID(status.Dest.Max())
			return nil
		}
		inSyncWithID := 0

		status.Method = shared.SyncMethodMerge
		idRange := core.NewIDRange(status.Source.Min(), status.Dest.Max())
		inSyncWithID, err = d.findMaxIDInSync(ctx, idRange, filter)
		if inSyncWithID > 0 {
			status.SetInSyncWithID(inSyncWithID)
		}
		return err
	}
	status.SyncChecker(checker)
	return err
}

func (d *diffService) isInSync(ctx *shared.Context, filter map[string]interface{}) bool {
	candidate := &core.Status{}

	if source, dest, err := d.Fetch(ctx, filter); err == nil {
		if err = d.UpdateStatus(ctx, candidate, source, dest, filter, false); err == nil {
			inSync, _ := candidate.InSync()
			return inSync
		}
	}
	return false
}

func (d *diffService) findMaxIDInSync(ctx *shared.Context, idRange *core.IDRange, filter map[string]interface{}) (int, error) {
	filter = shared.CloneMap(filter)
	inSyncDestMaxID := 0
	candidateID := idRange.Next(false)
	for i := 0; i < d.Diff.Depth; i++ {
		if idRange.Max <= 0 {
			break
		}
		filter[d.Sync.IDColumn()] = criteria.NewLessOrEqual(candidateID)
		inSync := d.isInSync(ctx, filter)
		if inSync {
			inSyncDestMaxID = candidateID
		}
		candidateID = idRange.Next(inSync)
	}
	return inSyncDestMaxID, nil
}

//New creates a new service computing signature difference
func New(sync *contract.Sync, dao dao.DaoService) DiffService {
	return &diffService{Sync: sync, dao: dao, Comparator: core.NewComparator(&sync.Diff)}
}
