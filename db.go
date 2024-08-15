package db

import (
	"database/sql"
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
func One[T any](query string, args []interface{}) *T {
	defer timer(queryToString(query, args))()

	db := GetDB()
	defer db.Close()

	rows, err := db.Query(query, args...)
	handleError("Error On Get Rows", err)
	defer rows.Close()

	if rows.Next() {
		// var structData T
		// mapToStruct(resultToMap(rows), &structData)
		structData := ScanStruct[T](rows)
		return &structData
	} else {
		return nil
	}
}

func All[T any](query string, args []interface{}) []T {
	defer timer(queryToString(query, args))()

	db := GetDB()
	defer db.Close()

	rows, err := db.Query(query, args...)
	handleError("Error On Get Rows", err)
	defer rows.Close()

	var res []T
	for rows.Next() {
		// var structData T
		// mapToStruct(resultToMap(rows), &structData)
		res = append(res, ScanStruct[T](rows))
	}

	return res
}

// Executes the query and returns the first column of the result
func Column(query string, args []interface{}, dest ...any) error {
	defer timer(queryToString(query, args))()

	db := GetDB()
	defer db.Close()

	row := db.QueryRow(query, args...)
	err := row.Scan(dest...)
	return err
}

// Executes the SQL statement and returns ALL rows at once
func QueryAll(query string, args []interface{}) []map[string]interface{} {
	defer timer(queryToString(query, args))()

	db := GetDB()
	defer db.Close()

	rows, err := db.Query(query, args...)
	handleError("Error On Get Rows", err)
	defer rows.Close()

	var res []map[string]interface{}
	for rows.Next() {
		res = append(res, resultToMap(rows))
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
//
// This function WILL NOT automatically close the rows and database connection after the query is executed.
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

func resultToMap(list *sql.Rows) map[string]interface{} {
	fields, _ := list.Columns()               // fieldName
	scans := make([]interface{}, len(fields)) // value
	row := make(map[string]interface{})       // result

	for i := range scans {
		scans[i] = &scans[i]
	}
	list.Scan(scans...)
	for i, v := range scans {
		if v != nil {
			row[fields[i]] = v
		}
	}

	return row
}

func mapToStruct(data map[string]interface{}, target interface{}) {
	rt := reflect.TypeOf(target).Elem()
	rv := reflect.ValueOf(target).Elem()

	for i := 0; i < rt.NumField(); i++ {
		fieldName := rt.Field(i).Name
		fieldType := rt.Field(i).Type
		createdAtField, _ := rt.FieldByName(fieldName)
		jsonTag := createdAtField.Tag.Get("json")

		if jsonTag != "" {
			fieldName = jsonTag
		} else {
			fieldName = strings.ToLower(fieldName)
		}

		if value, ok := data[fieldName]; ok {
			value = typeConvertor(value, fieldType)

			if fieldType.Kind() == reflect.Ptr && value != nil {
				switch fieldType.Elem().Kind() {
				case reflect.Bool:
					tmp := false
					rv.Field(i).Set(reflect.ValueOf(&tmp))
					rv.Field(i).Elem().Set(reflect.ValueOf(value))
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					tmp := 0
					rv.Field(i).Set(reflect.ValueOf(&tmp))
					rv.Field(i).Elem().Set(reflect.ValueOf(value))
				case reflect.Float32, reflect.Float64:
					tmp := 0.0
					rv.Field(i).Set(reflect.ValueOf(&tmp))
					rv.Field(i).Elem().Set(reflect.ValueOf(value))
				case reflect.String:
					tmp := ""
					rv.Field(i).Set(reflect.ValueOf(&tmp))
					rv.Field(i).Elem().Set(reflect.ValueOf(value))
				case reflect.Map:
					tmp := map[string]interface{}{}
					rv.Field(i).Set(reflect.ValueOf(&tmp))
					rv.Field(i).Elem().Set(reflect.ValueOf(value))
				}
			} else {
				rv.Field(i).Set(reflect.ValueOf(value))
			}
		}
	}
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

func ScanStruct[T any](row *sql.Rows) (structData T) {
	fields, _ := row.Columns()                // fieldName
	scans := make([]interface{}, len(fields)) // value

	for i := range scans {
		scans[i] = &scans[i]
	}

	rt := reflect.TypeOf(structData)
	rv := reflect.ValueOf(&structData).Elem()
	for i := 0; i < rt.NumField(); i++ {
		fieldName := rt.Field(i).Name
		createdAtField, _ := rt.FieldByName(fieldName)
		jsonTag := createdAtField.Tag.Get("json")

		if jsonTag != "" {
			fieldName = jsonTag
		} else {
			fieldName = strings.ToLower(fieldName)
		}

		idx := IndexOf(fieldName, fields)

		if idx < 0 {
			continue
		}

		scans[idx] = rv.Field(i).Addr().Interface()
	}

	row.Scan(scans...)
	return structData
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

func IndexOf(item string, array []string) int {
	for i, element := range array {
		if element == item {
			return i
		}
	}
	return -1
}
