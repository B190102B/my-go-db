# Go DB Connector for GCP Functions

A lightweight and reusable **database connector utility** for Google Cloud Functions, designed to simplify MySQL queries with support for **read-only / read-write pools**, **Cloud SQL Connector integration**, and **struct mapping**.

---

## 🚀 Features

* ✅ Connection pooling optimized for **Google Cloud Functions**
* ✅ Automatic **read-only vs write-enabled** connection handling
* ✅ Support for **Cloud SQL Connector** (`cloud.google.com/go/cloudsqlconn`)
* ✅ Helper functions for:
  * `One` → fetch single row into struct
  * `All` → fetch multiple rows into slice of structs
  * `Column` / `ColumnSlice` → fetch column values directly
  * `QueryAll` → return results as `[]map[string]interface{}`
  * `Exec` → execute write queries
* ✅ Struct mapping with **json tags** or **snake_case field matching**
* ✅ Simple query logging & execution timer
* ✅ Explicit `CloseDB()` for tests / graceful shutdown

---
## ⚙️ Environment Variables

The connector relies on environment variables for database credentials and connection configuration.
It supports **two connection modes**: **Unix socket** and **TCP with Cloud SQL Connector**.

| Variable                                            | Description                                               |
| --------------------------------------------------- | --------------------------------------------------------- |
| `DATABASE_NAME`                                     | Database name                                             |
| `DATABASE_MODE`                                     | Connection mode: `tcp` or `unix`                          |
| `DATABASE_USERNAME` / `DATABASE_PASSWORD`           | Write DB credentials                                      |
| `DATABASE_HOST`                                     | Write DB host (or socket path)                            |
| `DATABASE_READ_USERNAME` / `DATABASE_READ_PASSWORD` | Read-only DB credentials                                  |
| `DATABASE_READ_HOST`                                | Read-only DB host (or socket path)                        |
| `DATABASE_INSTANCES`                                | Cloud SQL instances (write, required in **TCP mode**)     |
| `DATABASE_READ_INSTANCES`                           | Cloud SQL instances (read-only, required in **TCP mode**) |

---

### 🔧 Connection Modes

#### 1. Using **Unix Socket**

* `DATABASE_INSTANCES` / `DATABASE_READ_INSTANCES` **not required**.
* Requires enabling **Cloud SQL Connections** in **Google Cloud Console**.
* Typically used when Cloud Functions are deployed in the same project/region as the Cloud SQL instance.

Example:

```env
DATABASE_MODE=unix
DATABASE_HOST=/cloudsql/my-project:region:instance
```

#### 2. Using **TCP + Cloud SQL Connector**

* `DATABASE_INSTANCES` / `DATABASE_READ_INSTANCES` **are required**.
* The connector will automatically register a secure dialer using the provided instance name(s).

Example:

```env
DATABASE_MODE=tcp
DATABASE_INSTANCES=my-project:region:instance
DATABASE_HOST=localhost:3306
```

---

### ⚠️ Read/Write Fallback Behavior

* If **read-only credentials are not configured**, the connector will **default to using the write DB**.
* This ensures queries won’t fail in environments where only a single DB user is configured.

---

## 🛠️ Usage

### Initialize a Query

```go
import "github.com/B190102B/db"

// Example struct
type User struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
}
```

### Fetch One Record

```go
user := db.One[User]("SELECT id, name FROM users WHERE id = ?", []interface{}{1})
if user != nil {
    fmt.Println("User:", user.Name)
}
```

### Fetch Multiple Records

```go
users := db.All[User]("SELECT id, name FROM users", nil)
for _, u := range users {
    fmt.Println(u.Name)
}
```

### Fetch Column Values

```go
ids, err := db.ColumnSlice[int]("SELECT id FROM users", nil)
if err == nil {
    fmt.Println("User IDs:", ids)
}
```

### Execute Write Query

```go
_, err := db.Exec("INSERT INTO users (name) VALUES (?)", []interface{}{"Alice"})
if err != nil {
    panic(err)
}
```

---

## 🧪 Testing & Cleanup

To close DB connections (useful in tests or long-running services):

```go
if err := db.CloseDB(); err != nil {
    fmt.Println("Error closing DB:", err)
}
```

---

## ⚡ Notes

* Connections are **not closed automatically** in Cloud Functions (they are reused by the platform).
* Connection pools are configured with:

  * `MaxOpenConns = 1`
  * `MaxIdleConns = 1`
  * Short connection lifetime for efficiency.
* For production, use **read-only connections** whenever possible.
