package contract

import (
	"fmt"
	"sync/atomic"
)

//Transfer represents transferData config
type Transfer struct {
	BatchSize     int
	WriterThreads int
	EndpointIP    string
	EndpointIPS   []string
	MaxRetries    int
	TempDatabase  string
	Suffix        string
	DriverName    string
	Descriptor    string
	ipLoadCounter int32
}

//Init initializes transfer
func (t *Transfer) Init() error {
	if t.MaxRetries == 0 {
		t.MaxRetries = 2
	}
	return nil
}

func (t *Transfer) SelectEndpointIP() (endIp string, err error) {
	if len(t.EndpointIP) > 0 {
		endIp = t.EndpointIP
	} else if len(t.EndpointIPS) > 0 {
		index := atomic.AddInt32(&t.ipLoadCounter, 1)
		ip := t.EndpointIPS[int(index)%len(t.EndpointIPS)]
		endIp = ip
	} else {
		err = fmt.Errorf("can't find transfer EndpointIp!")
	}
	err = nil
	return
}
