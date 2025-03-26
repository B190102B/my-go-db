package db

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/iancoleman/strcase"
	"github.com/spf13/cast"
)

var (
	logging bool
)

// Pls enhance the query by incorporating the 'limit 1' parameter to optimize speed.
func One[T any](query string, args []interface{}) *T {
	defer timer(GenerateQueryString(query, args))()

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
	defer timer(GenerateQueryString(query, args))()

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
	defer timer(GenerateQueryString(query, args))()

	db := GetDB()
	defer db.Close()

	row := db.QueryRow(query, args...)
	err := row.Scan(dest...)
	return err
}

// ColumnSlice executes the query and returns all values from the first column as a slice
func ColumnSlice[T any](query string, args []interface{}) ([]T, error) {
	defer timer(GenerateQueryString(query, args))()

	db := GetDB()
	defer db.Close()

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("error on query execution: %w", err)
	}
	defer rows.Close()

	var res []T
	for rows.Next() {
		var dest T
		if err := rows.Scan(&dest); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}
		res = append(res, dest)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return res, nil
}

// Executes the SQL statement and returns ALL rows at once
func QueryAll(query string, args []interface{}) []map[string]interface{} {
	defer timer(GenerateQueryString(query, args))()

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
	defer timer(GenerateQueryString(query, args))()

	db := GetDB()
	defer db.Close()

	rows, err := db.Query(query, args...)
	handleError("Error On Get Rows", err)

	return rows
}

func Exec(query string, args []interface{}) (sql.Result, error) {
	defer timer(GenerateQueryString(query, args))()

	db := GetDB(false)
	defer db.Close()

	return db.Exec(query, args...)
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

func GenerateQueryString(query string, args []interface{}) string {
	if len(args) == 0 {
		return query
	}

	new := fmt.Sprintf("'%v'", args[0])
	switch value := args[0].(type) {
	case bool, int, float64:
		new = fmt.Sprintf("%v", value)
	case nil:
		new = "NULL"
	}

	query = strings.Replace(query, "?", new, 1)
	return GenerateQueryString(query, args[1:])
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
		scans[i] = new(interface{})
	}

	rt := reflect.TypeOf(structData)
	rv := reflect.ValueOf(&structData).Elem()
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		fieldName := field.Name

		// Get json tag if exists
		if jsonTag := field.Tag.Get("json"); jsonTag != "" {
			fieldName = jsonTag
		} else {
			// Conver to Snake case
			fieldName = strcase.ToSnake(fieldName)
		}

		idx := IndexOf(fieldName, fields)
		if idx < 0 {
			continue
		}

		// Only set the scan target if the field type can handle nil
		if isNullableType(field.Type) {
			scans[idx] = rv.Field(i).Addr().Interface()
		}
	}

	if err := row.Scan(scans...); err != nil {
		// Handle scan error, but we're already skipping problematic fields
		handleError("Error scan fields", err)
		return structData
	}

	// For fields we didn't set (because they might error), try to set them from the scanned interface{}
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		fieldName := field.Name

		if jsonTag := field.Tag.Get("json"); jsonTag != "" {
			fieldName = jsonTag
		} else {
			fieldName = strcase.ToSnake(fieldName)
		}

		idx := IndexOf(fieldName, fields)
		if idx < 0 {
			continue
		}

		if !isNullableType(field.Type) {
			// Try to set the value from the scanned interface{}
			scannedVal := *scans[idx].(*interface{})
			if scannedVal != nil {
				fv := rv.Field(i)
				if err := setFieldFromInterface(fv, scannedVal); err != nil {
					// Skip if we can't set the field
					continue
				}
			}
		}
	}

	return structData
}

// Helper function to check if a type can handle nil values
func isNullableType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map:
		return true
	default:
		// Check for sql.Null types
		if strings.HasPrefix(t.String(), "sql.Null") {
			return true
		}
		return false
	}
}

// Helper function to set a field from an interface{} value
func setFieldFromInterface(fv reflect.Value, val interface{}) error {
	if !fv.CanSet() {
		return fmt.Errorf("field cannot be set")
	}

	// Handle time.Time specifically
	if fv.Type() == reflect.TypeOf(time.Time{}) {
		if t, ok := val.(time.Time); ok {
			fv.Set(reflect.ValueOf(t))
			return nil
		}
		return fmt.Errorf("not a time.Time")
	}

	// Handle string specifically
	if fv.Type() == reflect.TypeOf("") {
		t, ok := val.(string)
		if !ok {
			t = fmt.Sprint(val)
		}
		fv.Set(reflect.ValueOf(t))
		return nil
	}

	// Handle other types
	valType := reflect.TypeOf(val)
	if valType.ConvertibleTo(fv.Type()) {
		fv.Set(reflect.ValueOf(val).Convert(fv.Type()))
		return nil
	}

	return fmt.Errorf("type mismatch")
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
		return func() { fmt.Printf("[%.2fms] %s \n", float64(time.Since(st).Milliseconds()), query) }
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
