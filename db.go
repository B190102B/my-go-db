package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	readDB  *sql.DB
	writeDB *sql.DB
	logging bool
)

func One[T comparable](query string, args []interface{}) *T {
	st := time.Now()
	rows, err := GetDB().Query(query, args...)
	handleError("Error On Get Rows", err)

	if logging {
		defer log.Println(fmt.Sprintf("[%v] %s", time.Since(st), queryToString(query, args)))
	}

	var res []map[string]interface{}
	if rows.Next() {
		m := resultToMap(rows)
		res = append(res, m)
		structList := mapToStruct[T](res)
		return &structList[0]
	} else {
		return nil
	}
}

func All[T comparable](query string, args []interface{}) []T {
	st := time.Now()
	rows, err := GetDB().Query(query, args...)
	handleError("Error On Get Rows", err)

	if logging {
		defer log.Println(fmt.Sprintf("[%v] %s", time.Since(st), queryToString(query, args)))
	}

	var res []map[string]interface{}
	for rows.Next() {
		m := resultToMap(rows)
		res = append(res, m)
	}
	structList := mapToStruct[T](res)

	return structList
}

func GetRows(query string, args []interface{}) *sql.Rows {
	st := time.Now()
	rows, err := GetDB().Query(query, args...)
	handleError("Error On Get Rows", err)

	if logging {
		defer log.Println(fmt.Sprintf("[%v] %s", time.Since(st), queryToString(query, args)))
	}

	return rows
}

func Exec(query string, args []interface{}) sql.Result {
	st := time.Now()

	if logging {
		defer log.Println(fmt.Sprintf("[%v] %s", time.Since(st), queryToString(query, args)))
	}

	res, err := GetDB().Exec(query, args...)
	handleError("Error On Executes Query", err)

	return res
}

func SetLogging(isLogging bool) {
	logging = isLogging
}

func GetDB(readOnly ...bool) *sql.DB {
	if len(readOnly) == 0 {
		readOnly = append(readOnly, true)
	}

	if readOnly[0] {
		if readDB != nil {
			return readDB
		}
	} else {
		if writeDB != nil {
			return writeDB
		}
	}

	dbConfig := &mysql.Config{
		DBName:               getEnv("DATABASE_NAME"),
		Net:                  getEnv("DATABASE_MODE"),
		ParseTime:            true,
		AllowNativePasswords: true,
	}
	if readOnly[0] {
		dbConfig.User = getEnv("DATABASE_READ_USERNAME")
		dbConfig.Passwd = getEnv("DATABASE_READ_PASSWORD")
		dbConfig.Addr = getEnv("DATABASE_READ_HOST")
	}

	if dbConfig.User == "" || dbConfig.Passwd == "" || dbConfig.Addr == "" {
		dbConfig.User = getEnv("DATABASE_USERNAME")
		dbConfig.Passwd = getEnv("DATABASE_PASSWORD")
		dbConfig.Addr = getEnv("DATABASE_HOST")
	}

	db, err := sql.Open("mysql", dbConfig.FormatDSN())
	if err != nil {
		handleError("Error Open Connection DB", err)
	}

	// Check the connectivity by pinging the database
	if err := db.Ping(); err != nil {
		handleError("Error connecting to the database", err)
	}

	return db
}

func queryToString(query string, args []interface{}) string {
	if len(args) == 0 {
		return query
	}
	query = strings.Replace(query, "?", fmt.Sprintf("%v", args[0]), 1)
	return queryToString(query, args[1:])
}

func resultToMap(list *sql.Rows) map[string]interface{} {
	fields, _ := list.Columns()
	scans := make([]interface{}, len(fields))
	row := make(map[string]interface{})

	for i := range scans {
		scans[i] = &scans[i]
	}
	list.Scan(scans...)
	for i, v := range scans {
		var value = ""
		if v != nil {
			value = fmt.Sprintf("%s", v)
		}
		row[fields[i]] = value
	}
	return row
}

func mapToStruct[T comparable](mapList []map[string]interface{}) (structList []T) {
	for _, mapData := range mapList {
		jsonData, _ := json.Marshal(mapData)

		var structData T
		json.Unmarshal(jsonData, &structData)

		structList = append(structList, structData)
	}
	return
}

func getEnv(k string) string {
	v := os.Getenv(k)
	return v
}

func handleError(info string, err error) {
	if err != nil {
		msg := fmt.Sprintf("%s: %s", info, err.Error())
		panic(msg)
	}
}
