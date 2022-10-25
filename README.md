# go-mysql-postgresql
Golang language integration of mysql and postgresql operations

## Table of contents:
- [Get Started](#get-started)
- [Examples](#examples)

### Get Started
#### Installation

```sh
$ go get github.com/go-sql-driver/mysql
$ go get github.com/bonjovis/go-mysql
```


#### Examples

```go
var db *gosql.DbPool

func init() {
	conf, _ = config.ReadDefault("config.cfg")
	dbHost := conf.Cstring("db", "dbuser") + ":" + conf.Cstring("db", "dbpwd") + "@" + conf.Cstring("db", "dbhost")
	fmt.Printf("dbHost: %v\n", dbHost)
	maxOpenConns := 200
	maxIdleConns := 100
	dbPool := gosql.NewDatabaseConnectionPool(dbHost, maxOpenConns, maxIdleConns)
	db = dbPool
}

func main() {
	//query
	list := db.FindAll("select * from user")
	log.Printf("list: %v\n", list)
  
   //counts
  counts := db.Counts("select count(1) from user")
  
  //update
  var vals = []interface{}{}
  dbl.Update("update test set abc=1",vals)
  
  //insert
  vo := make(map[string]interface{})
  tableName := "test"
  vo["id"] = 1
  vo["name"] = "test"
  ret := db.Insert(vo, tableName)
  
  //MultiInsert
  var list = []map[string]interface{}
  list = append(list, vo)
  ret = db.MultiInsert(list, tableName)
 }
 ```
 
### License
MIT
