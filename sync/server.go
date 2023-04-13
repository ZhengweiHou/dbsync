package sync

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/sirupsen/logrus"
)

//Server represents a server
type SyncServer struct {
	*http.Server
	termination chan bool
}

func (s *SyncServer) shutdown() { // 该方法未被调用？
	<-s.termination
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Server.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}

//StopOnSiginals stops server on siginal
func (s *SyncServer) StopOnSiginals(siginals ...os.Signal) {
	notification := make(chan os.Signal, 1)
	signal.Notify(notification, siginals...)
	// var aa os.Signal
	aa := <-notification
	fmt.Println("=======stop")
	logrus.Info(aa)
	s.Stop()
}

//Stop stop server
func (s *SyncServer) Stop() {
	s.termination <- true
	s.shutdown() // 手动调用关闭逻辑 add by: houzw
}

//NewServer creates a new server
func NewServer(service SyncService, port int) *SyncServer {
	router := NewRouter(service)
	return &SyncServer{
		termination: make(chan bool, 1),
		Server: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: router,
		},
	}
}
