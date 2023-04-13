package main

import (
	"dbsync/sync"
	"dbsync/sync/shared"
	"flag"
	"fmt"

	//	_ "github.com/alexbrainman/odbc"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/gops/agent"
	_ "github.com/ibmdb/go_ibm_db"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	_ "github.com/vertica/vertica-sql-go"

	//	_ "github.com/mattn/go-oci8"
	"log"
	"os"

	_ "github.com/viant/asc"
	_ "github.com/viant/bgc"
)

//Version app version
var Version string

var port = flag.Int("port", 8080, "service port")
var url = flag.String("url", "cron", "schedule URL")
var debug = flag.Bool("debug", false, "debug flag")
var scheduleURLRefreshMs = flag.Int("urlRefresh", 100, "scheduleURL refresh in ms")
var statsHistory = flag.Int("statsHistory", 10, "max stats history")

// func init() {
// 	flag.IntVar("port", 8080, "service port")
// 	flag.StringVar("url", "cron", "schedule URL")
// 	flag.BoolVar("debug", false, "debug flag")
// 	flag.IntVar("urlRefresh", 100, "scheduleURL refresh in ms")
// 	flag.IntVar("statsHistory", 10, "max stats history")
// }

func main() {
	pwd, _ := os.Getwd()
	// log.SetFlags(log.Lshortfile)
	log.SetFlags(log.Ltime | log.Llongfile)
	log.Println("日志测试,pwd:", pwd, *debug)

	flag.Parse()

	if *debug {
		logrus.SetLevel(logrus.DebugLevel)
	}
	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Fatal(err)
		}
	}()

	config := &shared.Config{
		Debug:                *debug,
		ScheduleURL:          *url,
		ScheduleURLRefreshMs: *scheduleURLRefreshMs,
		MaxHistory:           *statsHistory,
	}
	service, err := sync.New(config)
	if err != nil {
		log.Fatal(err)
	}
	server := sync.NewServer(service, *port)
	go server.StopOnSiginals(os.Interrupt) // 监听中断信号
	fmt.Printf("dbsync %v listening on :%d\n", Version, *port)
	log.Fatal(server.ListenAndServe()) // 启动http服务
}

// CREATE TABLE `student` (
// 	`id` int NOT NULL AUTO_INCREMENT,
// 	`name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
// 	`age` int DEFAULT NULL,
// 	`grades` decimal(7,2) DEFAULT NULL,
// 	`modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
// 	PRIMARY KEY (`id`)
//   )
