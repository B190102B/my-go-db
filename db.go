package db

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/go-sql-driver/mysql"
	"github.com/iancoleman/strcase"
)

var (
	logging     bool
	readDBOnce  = sync.OnceValue(func() *sql.DB { return initDB(true) })
	writeDBOnce = sync.OnceValue(func() *sql.DB { return initDB(false) })
)

// Pls enhance the query by incorporating the 'limit 1' parameter to optimize speed.
func One[T any](query string, args []any) *T {
	defer timer(GenerateQueryString(query, args))()

	db := GetDB()
	rows, err := db.Query(query, args...)
	handleError("Error On Get Rows", err)
	defer rows.Close()

	if rows.Next() {
		structData := ScanStruct[T](rows)
		return &structData
	} else {
		return nil
	}
}

func All[T any](query string, args []any) []T {
	defer timer(GenerateQueryString(query, args))()

	db := GetDB()
	rows, err := db.Query(query, args...)
	handleError("Error On Get Rows", err)
	defer rows.Close()

	var res []T
	for rows.Next() {
		res = append(res, ScanStruct[T](rows))
	}

	return res
}

// Executes the query and returns the first column of the result
func Column(query string, args []any, dest ...any) error {
	defer timer(GenerateQueryString(query, args))()

	db := GetDB()
	row := db.QueryRow(query, args...)
	err := row.Scan(dest...)
	return err
}

// ColumnSlice executes the query and returns all values from the first column as a slice
func ColumnSlice[T any](query string, args []any) ([]T, error) {
	defer timer(GenerateQueryString(query, args))()

	db := GetDB()
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
func QueryAll(query string, args []any) []map[string]any {
	defer timer(GenerateQueryString(query, args))()

	db := GetDB()
	rows, err := db.Query(query, args...)
	handleError("Error On Get Rows", err)
	defer rows.Close()

	var res []map[string]any
	for rows.Next() {
		res = append(res, resultToMap(rows))
	}

	return res
}

// Deprecated: Unable to close the rows and database connection after the query is completed.
// This function will retain the database connection in the pool.
func GetRows(query string, args []any) *sql.Rows {
	defer timer(GenerateQueryString(query, args))()

	db := GetDB()
	rows, err := db.Query(query, args...)
	handleError("Error On Get Rows", err)

	return rows
}

func Exec(query string, args []any) (sql.Result, error) {
	defer timer(GenerateQueryString(query, args))()

	db := GetDB(false)
	return db.Exec(query, args...)
}

func SetLogging(isLogging bool) {
	logging = isLogging
}

func GetIsLogging() bool {
	return logging
}

// GetDB returns a shared connection pool (*sql.DB) for either read-only or read-write access.
//
//   - If `readOnly` is true or not specified, it returns the read-only database connection.
//   - If `readOnly` is false, it returns the write-enabled database connection.
//
// Connection pools are initialized once and reused for the lifetime of the Cloud Function instance.
// This improves performance by avoiding repeated connection creation.
//
// ⚠️ Note:
//   - Connections are NOT closed automatically — they remain open and reused across invocations.
//   - You can explicitly close the pooled connections by calling `CloseDB()` (e.g., in tests or graceful shutdowns).
//
// 🔄 GCP recommends globally scoped connection pools to maximize connection reuse
// and allow cleanup when the instance is evicted (auto-scaled down).
func GetDB(readOnly ...bool) *sql.DB {
	// Use write DB connection if explicitly requested
	if len(readOnly) > 0 && !readOnly[0] {
		return writeDBOnce()
	}

	// Default to read-only DB connection
	return readDBOnce()
}

func initDB(readOnly bool) *sql.DB {
	dbConfig := &mysql.Config{
		DBName:               getEnv("DATABASE_NAME"),
		Net:                  getEnv("DATABASE_MODE"),
		ParseTime:            true,
		AllowNativePasswords: true,
	}

	if readOnly {
		dbConfig.User = getEnv("DATABASE_READ_USERNAME")
		dbConfig.Passwd = getEnv("DATABASE_READ_PASSWORD")
		dbConfig.Addr = getEnv("DATABASE_READ_HOST") // Use unix socket

		// Use Cloud SQL Connector if configured
		if cloudSqlInstances := getEnv("DATABASE_READ_INSTANCES"); cloudSqlInstances != "" {
			network := "cloudsqlconn_read"
			if err := registerDial(cloudSqlInstances, network); err != nil {
				handleError("cloudsqlconn.NewDialer", err)
			}

			dbConfig.Net = network
			dbConfig.Addr = "localhost:3306"
		}
	}

	if dbConfig.User == "" || dbConfig.Passwd == "" || dbConfig.Addr == "" {
		dbConfig.User = getEnv("DATABASE_USERNAME")
		dbConfig.Passwd = getEnv("DATABASE_PASSWORD")
		dbConfig.Addr = getEnv("DATABASE_HOST") // Use unix socket

		// Use Cloud SQL Connector if configured
		if cloudSqlInstances := getEnv("DATABASE_INSTANCES"); cloudSqlInstances != "" {
			network := "cloudsqlconn_write"
			if err := registerDial(cloudSqlInstances, network); err != nil {
				handleError("cloudsqlconn.NewDialer", err)
			}

			dbConfig.Net = network
			dbConfig.Addr = "localhost:3306"
		}
	}

	db, err := sql.Open("mysql", dbConfig.FormatDSN())
	handleError("Error Open Connection DB", err)

	// Check the connectivity by pinging the database
	if err := db.Ping(); err != nil {
		handleError("Error connecting to the database", err)
	}

	// Optimize for Cloud Functions: single, short-lived connection
	db.SetConnMaxLifetime(5 * time.Second)
	db.SetConnMaxIdleTime(1 * time.Second)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return db
}

func registerDial(cloudSqlInstances, network string) error {
	dialer, err := cloudsqlconn.NewDialer(context.Background())
	if err != nil {
		return err
	}

	mysql.RegisterDialContext(network, func(ctx context.Context, addr string) (net.Conn, error) {
		return dialer.Dial(ctx, cloudSqlInstances)
	})

	return nil
}

// CloseDB explicitly closes the read and write *sql.DB connection pools if they exist.
//
// This is typically unnecessary in Google Cloud Functions, as connections are automatically
// cleaned up when the instance is shut down. However, this function can be useful for:
//
//   - Unit tests
//   - Graceful shutdown in long-lived services
//   - Manual cleanup between runs (e.g., CLI tools or dev scripts)
func CloseDB() error {
	if err := readDBOnce().Close(); err != nil {
		return err
	}
	if err := writeDBOnce().Close(); err != nil {
		return err
	}

	return nil
}

func GenerateQueryString(query string, args []any) string {
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

func resultToMap(list *sql.Rows) map[string]any {
	fields, _ := list.Columns()       // fieldName
	scans := make([]any, len(fields)) // value
	row := make(map[string]any)       // result

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

type fieldMap struct {
	structIdx int
	scanIdx   int
}

func ScanStruct[T any](row *sql.Rows) (structData T) {
	fields, _ := row.Columns()        // fieldName
	scans := make([]any, len(fields)) // value

	for i := range scans {
		scans[i] = new(any)
	}

	rt := reflect.TypeOf(structData)
	rv := reflect.ValueOf(&structData).Elem()
	numFields := rt.NumField()

	notNullFields := make([]fieldMap, 0, rt.NumField())
	for i := range numFields {
		field := rt.Field(i)
		fieldName := field.Name

		// Get json tag if exists
		if jsonTag := field.Tag.Get("json"); jsonTag != "" {
			fieldName = jsonTag
		} else {
			// Conver to Snake case
			fieldName = strcase.ToSnake(fieldName)
		}

		idx := slices.Index(fields, fieldName)
		if idx < 0 {
			continue
		}

		// Only set the scan target if the field type can handle nil
		if isNullableType(field.Type) {
			scans[idx] = rv.Field(i).Addr().Interface()
		} else {
			notNullFields = append(notNullFields, fieldMap{i, idx})
		}
	}

	if err := row.Scan(scans...); err != nil {
		// Handle scan error, but we're already skipping problematic fields
		handleError("Error scan fields", err)
		return structData
	}

	// For fields we didn't set (because they might error), try to set them from the scanned interface{}
	for _, m := range notNullFields {
		// Try to set the value from the scanned any
		scannedVal := *scans[m.scanIdx].(*any)
		if scannedVal != nil {
			fv := rv.Field(m.structIdx)
			if err := setFieldFromInterface(fv, scannedVal); err != nil {
				// Skip if we can't set the field
				continue
			}
		}
	}

	return structData
}

// Helper function to check if a type can handle nil values
func isNullableType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map:
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
func setFieldFromInterface(fv reflect.Value, val any) error {
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
	if fv.Type().Kind() == reflect.String {
		if _, ok := val.([]byte); !ok {
			val = fmt.Sprint(val)
		}
		fv.Set(reflect.ValueOf(val).Convert(fv.Type()))
		return nil
	}

	// Handle boolean types specifically
	if fv.Type().Kind() == reflect.Bool {
		switch v := val.(type) {
		case bool:
			fv.SetBool(v)
			return nil
		case int64:
			fv.SetBool(v != 0)
			return nil
		case int:
			fv.SetBool(v != 0)
			return nil
		case []byte:
			if len(v) == 1 {
				fv.SetBool(v[0] == '1')
				return nil
			}
		}
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
