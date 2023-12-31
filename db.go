package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	logging bool
)

func One[T comparable](query string, args []interface{}) *T {
	defer timer(queryToString(query, args))()

	db := GetDB()
	defer db.Close()

	rows, err := db.Query(query, args...)
	defer rows.Close()
	handleError("Error On Get Rows", err)

	if rows.Next() {
		structData := resultToStruct[T](rows)
		return &structData
	} else {
		return nil
	}
}

func All[T comparable](query string, args []interface{}) []T {
	defer timer(queryToString(query, args))()

	db := GetDB()
	defer db.Close()

	rows, err := db.Query(query, args...)
	defer rows.Close()
	handleError("Error On Get Rows", err)

	var res []T
	for rows.Next() {
		structData := resultToStruct[T](rows)
		res = append(res, structData)
	}

	return res
}

func Count(query string, args []interface{}) int {
	return 0
}

func GetRows(query string, args []interface{}) *sql.Rows {
	defer timer(queryToString(query, args))()

	db := GetDB()
	defer db.Close()

	rows, err := db.Query(query, args...)
	handleError("Error On Get Rows", err)

	return rows
}

func Exec(query string, args []interface{}) sql.Result {
	defer timer(queryToString(query, args))()

	db := GetDB(false)
	defer db.Close()

	res, err := db.Exec(query, args...)
	handleError("Error On Executes Query", err)

	return res
}

func SetLogging(isLogging bool) {
	logging = isLogging
}

func GetIsLogging() bool {
	return logging
}

func GetDB(readOnly ...bool) *sql.DB {
	if len(readOnly) == 0 {
		readOnly = append(readOnly, true)
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

func resultToStruct[T comparable](list *sql.Rows) (structData T) {
	fields, _ := list.Columns()
	scans := make([]interface{}, len(fields))
	row := make(map[string]interface{})

	for i := range scans {
		scans[i] = &scans[i]
	}
	list.Scan(scans...)
	for i, v := range scans {
		if v != nil {
			if reflect.TypeOf(v).String() == "[]uint8" {
				v = fmt.Sprintf("%s", v)
			}
			row[fields[i]] = v
		}
	}

	jsonData, _ := json.Marshal(row)
	json.Unmarshal(jsonData, &structData)
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

func timer(query string) func() {
	if logging {
		st := time.Now()
		return func() { fmt.Printf("[%s] %s \n", time.Since(st), query) }
	}
	return func() {}
}
