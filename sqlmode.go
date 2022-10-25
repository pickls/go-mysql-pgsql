package sqlmode

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type DbPool struct {
	db *sql.DB
}

func NewDatabaseConnectionPool(dbHost string, maxOpenConns, maxIdleConns int) *DbPool {
	db, err := sql.Open("mysql", dbHost)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	log.Println("数据库连接成功!")
	dbPool := DbPool{db}
	return &dbPool
}

func NewPgDatabaseConnectionPool(dbHost string, maxOpenConns, maxIdleConns int) *DbPool {
	db, err := sql.Open("postgres", dbHost)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	log.Println("数据库连接成功!")
	dbPool := DbPool{db}
	return &dbPool
}

func (dbPool *DbPool) FindAll(st string) []map[string]interface{} {
	rows, err := dbPool.db.Query(st)
	defer rows.Close()
	checkErr(err)

	columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns))
	values := make([]string, len(columns))
	for j := range values {
		scanArgs[j] = &values[j]
	}

	citems := make([]map[string]interface{}, 0, 10)
	for rows.Next() {
		record := make(map[string]interface{})
		err = rows.Scan(scanArgs...)
		for i, col := range values {
			record[columns[i]] = col
		}
		citems = append(citems, record)
	}
	return citems
}

func (dbPool *DbPool) FindOne(st string) map[string]interface{} {
	rows, err := dbPool.db.Query(st)
	defer rows.Close()
	checkErr(err)
	columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns))
	values := make([]string, len(columns))
	for j := range values {
		scanArgs[j] = &values[j]
	}

	record := make(map[string]interface{})
	if rows.Next() {
		err = rows.Scan(scanArgs...)
		for i, col := range values {
			record[columns[i]] = col
		}
	}
	return record
}

func (dbPool *DbPool) Counts(sql string) int {
	cnt := 0
	_ = dbPool.db.QueryRow(sql).Scan(&cnt)
	return cnt
}

func (dbPool *DbPool) GetLatestId(sql string) int64 {
	id := 0
	_ = dbPool.db.QueryRow(sql).Scan(&id)
	return int64(id)
}

func (dbPool *DbPool) Update(sql string, vals []interface{}) int64 {
	stmt, err := dbPool.db.Prepare(sql)
	defer stmt.Close()
	checkErr(err)
	result, err := stmt.Exec(vals...)
	if checkErr(err) {
		return -1
	}
	if isNil(result) {
		return -1
	}
	affectLines, err := result.RowsAffected()
	checkErr(err)
	return affectLines
}

func (dbPool *DbPool) MultiInsert(param []map[string]interface{}, tablename string) int64 {
	var keys []string
	var vals = []interface{}{}
	if len(param) > 0 {
		for key, _ := range param[0] {
			keys = append(keys, key)
		}
		fileds := "`" + strings.Join(keys, "`,`") + "`"
		sqlStr := fmt.Sprintf("REPLACE INTO %v (%v) VALUES ", tablename, fileds)

		for _, row := range param {
			sqlStr += "("
			for _, v := range keys {
				sqlStr += "?,"
				value := row[v]
				if value != nil {
					switch value.(type) {
					case int:
						vals = append(vals, strconv.Itoa(value.(int)))
					case int32, int64:
						vals = append(vals, strconv.FormatInt(value.(int64), 10))
					case string:
						vals = append(vals, EscapeString(value.(string)))
					case float32, float64:
						vals = append(vals, strconv.FormatFloat(value.(float64), 'f', -1, 64))
					default:
						vals = append(vals, "")
					}
				} else {
					vals = append(vals, "")
				}
			}
			sqlStr = strings.TrimSuffix(sqlStr, ",")
			sqlStr += "),"
		}
		sqlStr = strings.TrimSuffix(sqlStr, ",")
		stmt, err := dbPool.db.Prepare(sqlStr)
		checkErr(err)
		result, err := stmt.Exec(vals...)
		if checkErr(err) {
			return -1
		}
		defer stmt.Close()
		if isNil(result) {
			return -1
		}
		affectLines, err := result.RowsAffected()
		checkErr(err)
		return affectLines
	}
	return 0
}

func (dbPool *DbPool) InsertN(param map[string]interface{}, tablename string) int64 {
	var keys []string
	var values []string
	for key, value := range param {
		keys = append(keys, key)
		if value != nil {
			switch value.(type) {
			case int:
				values = append(values, strconv.Itoa(value.(int)))
			case int32, int64:
				values = append(values, strconv.FormatInt(value.(int64), 10))
			case string:
				values = append(values, EscapeString(value.(string)))
			case float32, float64:
				values = append(values, strconv.FormatFloat(value.(float64), 'f', -1, 64))
			}
		} else {
			values = append(values, "")
		}
	}
	fileValue := "'" + strings.Join(values, "','") + "'"
	fileds := "`" + strings.Join(keys, "`,`") + "`"
	sql := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v)", tablename, fileds, fileValue)
	result, err := dbPool.db.Exec(sql)
	if checkErr(err) {
		return -1
	}
	if isNil(result) {
		return -1
	}
	lastId, err := result.LastInsertId()
	checkErr(err)
	return lastId
}

func (dbPool *DbPool) Insert(param map[string]interface{}, tablename string) int64 {
	var keys []string
	var values []string
	for key, value := range param {
		keys = append(keys, key)
		if value != nil {
			switch value.(type) {
			case int:
				values = append(values, strconv.Itoa(value.(int)))
			case int32, int64:
				values = append(values, strconv.FormatInt(value.(int64), 10))
			case string:
				values = append(values, EscapeString(value.(string)))
			case float32, float64:
				values = append(values, strconv.FormatFloat(value.(float64), 'f', -1, 64))
			}
		} else {
			values = append(values, "")
		}
	}
	fileValue := "'" + strings.Join(values, "','") + "'"
	fileds := "`" + strings.Join(keys, "`,`") + "`"
	sql := fmt.Sprintf("REPLACE INTO %v (%v) VALUES (%v)", tablename, fileds, fileValue)
	result, err := dbPool.db.Exec(sql)
	if checkErr(err) {
		return -1
	}
	if isNil(result) {
		return -1
	}
	lastId, err := result.LastInsertId()
	checkErr(err)
	return lastId
}

func (dbPool *DbPool) InsertInto(param map[string]interface{}, tablename string) int64 {
	var keys []string
	var values []string
	for key, value := range param {
		keys = append(keys, key)
		if value != nil {
			switch value.(type) {
			case int:
				values = append(values, strconv.Itoa(value.(int)))
			case int32, int64:
				values = append(values, strconv.FormatInt(value.(int64), 10))
			case string:
				values = append(values, EscapeString(value.(string)))
			case float32, float64:
				values = append(values, strconv.FormatFloat(value.(float64), 'f', -1, 64))
			}
		} else {
			values = append(values, "")
		}
	}
	fileValue := "'" + strings.Join(values, "','") + "'"
	fileds := "`" + strings.Join(keys, "`,`") + "`"
	sql := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v)", tablename, fileds, fileValue)
	result, err := dbPool.db.Exec(sql)
	if checkErr(err) {
		return -1
	}
	if isNil(result) {
		return -1
	}
	lastId, err := result.LastInsertId()
	checkErr(err)
	return lastId
}

func (dbPool *DbPool) LoadData(path string, tablename string, fields string, enclosed string, lines string) int64 {
	mysql.RegisterLocalFile(path)
	result, err := dbPool.db.Exec("LOAD DATA LOCAL INFILE '" + path + "' INTO TABLE " + tablename + " FIELDS TERMINATED BY '" + fields + "' ENCLOSED BY '" + enclosed + "' LINES TERMINATED BY '" + lines + "' IGNORE 1 ROWS;")
	if checkErr(err) {
		return -1
	}
	affectLines, err := result.RowsAffected()
	checkErr(err)
	return affectLines
}

func EscapeString(sql string) string {
	dest := make([]byte, 0, 2*len(sql))
	var escape byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]

		escape = 0

		switch c {
		case '\\':
			escape = '\\'
			break
		case '\'':
			escape = '\''
			break
		}

		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}

	return string(dest)
}

func checkErr(err error) bool {
	if err != nil {
		log.Println(err)
		if strings.Index(err.Error(), "Deadlock found when trying to get lock; try restarting transaction") > -1 {
			time.Sleep(20000 * time.Millisecond)
			return true
		}
	}
	return false
}

func isNil(i interface{}) bool {
	return i == nil
}
