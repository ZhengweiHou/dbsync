package transfer

import (
	"dbsync/sync/core"
	"dbsync/sync/shared"
)

type faker struct {
	TransferService
	transferred int
	err         error
}

func (f *faker) Post(ctx *shared.Context, request *Request, transferable *core.Transferable) error {
	if f.err != nil {
		return f.err
	}
	transferable.SetTransferred(f.transferred)
	return nil
}

//NewFaker returns new post faker
func NewFaker(service TransferService, transferred int, err error) TransferService {
	return &faker{
		TransferService: service,
		transferred:     transferred,
		err:             err,
	}
}
