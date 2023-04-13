module dbsync

go 1.12

require (
	github.com/aerospike/aerospike-client-go v2.9.0+incompatible // indirect
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/go-errors/errors v1.4.2
	github.com/go-logr/logr v1.2.3
	github.com/go-sql-driver/mysql v1.7.0
	github.com/google/gops v0.3.7
	github.com/ibmdb/go_ibm_db v0.4.2
	github.com/lib/pq v1.3.0
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.27.2 // indirect
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.9.0
	github.com/stretchr/testify v1.8.2
	github.com/vertica/vertica-sql-go v0.2.1
	github.com/viant/asc v0.5.0
	github.com/viant/assertly v0.9.0
	github.com/viant/bgc v0.8.0
	github.com/viant/dsc v0.16.2
	github.com/viant/dsunit v0.10.10
	github.com/viant/toolbox v0.34.5
	github.com/yuin/gopher-lua v1.1.0 // indirect
)

replace github.com/viant/dsc => ../dsc

replace github.com/viant/toolbox => ../toolbox
