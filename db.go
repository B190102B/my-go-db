package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cast"
)

var (
	logging bool
)

// Pls enhance the query by incorporating the 'limit 1' parameter to optimize speed.
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

// Deprecated: Unable to close the rows and database connection after the query is completed.
// This function will retain the database connection in the pool.
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

// The responsibility to close the database connection must be handled externally when calling this method.
// This function will not automatically close the rows and database connection after the query is executed.
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

	new := fmt.Sprintf("'%v'", args[0])
	switch value := args[0].(type) {
	case bool, int, float64:
		new = fmt.Sprintf("%v", value)
	}

	query = strings.Replace(query, "?", new, 1)
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
	typeMap := getStructTypeMap(structData)
	for i, v := range scans {
		if v != nil {
			row[fields[i]] = typeConvertor(v, typeMap[fields[i]])
		}
	}

	jsonData, _ := json.Marshal(row)
	json.Unmarshal(jsonData, &structData)
	return
}

func getStructTypeMap(s interface{}) map[string]reflect.Type {
	m := make(map[string]reflect.Type)

	value := reflect.ValueOf(s)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	if value.Kind() == reflect.Struct {
		typ := value.Type()

		for i := 0; i < value.NumField(); i++ {
			fieldName := typ.Field(i).Name
			fieldType := typ.Field(i).Type
			createdAtField, _ := typ.FieldByName(fieldName)
			jsonTag := createdAtField.Tag.Get("json")

			if jsonTag != "" {
				m[jsonTag] = fieldType
			} else {
				m[strings.ToLower(fieldName)] = fieldType
			}
		}
	}

	return m
}

func typeConvertor(value interface{}, targetType reflect.Type) interface{} {
	if targetType == nil {
		return value
	}

	if targetType.Kind() == reflect.Ptr {
		if value == "" {
			return nil
		}

		targetType = targetType.Elem()
	}

	switch targetType.Kind() {
	case reflect.Bool:
		return cast.ToBool(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return cast.ToInt(cast.ToString(value))
	case reflect.Float32, reflect.Float64:
		return cast.ToFloat64(cast.ToString(value))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return cast.ToUint(value)
	case reflect.String:
		return cast.ToString(value)
	case reflect.Map:
		return cast.ToStringMap(value)
	case reflect.Struct:
		switch targetType {
		case reflect.TypeOf(time.Time{}):
			return cast.ToTime(value)
		}
	}

	return value
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
		return func() { log.Printf("[%.2fms] %s \n", float64(time.Since(st).Milliseconds()), query) }
	}
	return func() {}
}
