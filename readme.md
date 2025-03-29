# PostgreSQL Helper

[![Go Reference](https://pkg.go.dev/badge/github.com/songzhibin97/postgresql_helper.svg)](https://pkg.go.dev/github.com/songzhibin97/postgresql_helper)
[![Go Report Card](https://goreportcard.com/badge/github.com/songzhibin97/postgresql_helper)](https://goreportcard.com/report/github.com/songzhibin97/postgresql_helper)
[![License](https://img.shields.io/github/license/songzhibin97/postgresql_helper)](https://github.com/songzhibin97/postgresql_helper/blob/main/LICENSE)

PostgreSQL Helper 是一个用于简化 PostgreSQL 数据库操作的 Go 库，提供了直观的 API、动态表管理、丰富的查询构建功能和内置监控支持。

## 特性

- 🔄 **动态表管理**：无需预定义模型即可创建、修改和管理数据库表
- 🔍 **灵活的查询构建器**：支持链式调用的流畅 API
- 📊 **内置 Prometheus 监控**：自动跟踪操作次数、错误和持续时间
- 🔒 **事务支持**：简单的事务管理和上下文传递
- 📑 **数据迁移系统**：支持数据库的版本控制和演化
- 🚀 **高效分页**：支持基于游标的分页，适用于大数据集

## 安装

```bash
go get github.com/songzhibin97/postgresql_helper
```

## 快速开始

### 连接数据库

```go
package main

import (
    "context"
    "log"
    
    "github.com/songzhibin97/postgresql_helper"
)

func main() {
    // 使用默认连接池配置
    db, err := postgresql_helper.Connect("postgres://user:password@localhost/dbname?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // 或使用自定义连接池配置
    config := postgresql_helper.DBConfig{
        DSN:             "postgres://user:password@localhost/dbname?sslmode=disable",
        MaxOpenConns:    20,
        MaxIdleConns:    10,
        ConnMaxLifetime: 15 * time.Minute,
        ConnMaxIdleTime: 5 * time.Minute,
    }
    
    db, err = postgresql_helper.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // 检查连接
    ctx := context.Background()
    if err := db.Ping(ctx); err != nil {
        log.Fatal("数据库连接失败:", err)
    }
}
```

### 创建表

```go
func createTable(db postgresql_helper.DB) error {
    ctx := context.Background()
    schema := db.Schema()
    
    tableSchema := types.TableSchema{
        Name: "users",
        Columns: []types.ColumnDefinition{
            {Name: "id", Type: "SERIAL", PrimaryKey: true},
            {Name: "username", Type: "VARCHAR(255)", Nullable: false, Unique: true},
            {Name: "email", Type: "VARCHAR(255)", Nullable: false},
            {Name: "status", Type: "VARCHAR(50)", Nullable: false, Default: "'active'"},
            {Name: "created_at", Type: "TIMESTAMP", Nullable: false, Default: "NOW()"},
        },
        IfNotExists: true,
    }
    
    return schema.CreateTable(ctx, tableSchema)
}
```

### 插入数据

```go
type User struct {
    ID        int       `db:"id"`
    Username  string    `db:"username"`
    Email     string    `db:"email"`
    Status    string    `db:"status"`
    CreatedAt time.Time `db:"created_at"`
}

func insertUser(db postgresql_helper.DB) (int64, error) {
    ctx := context.Background()
    table := db.Table(ctx, "users")
    
    user := User{
        Username: "john_doe",
        Email:    "john@example.com",
        Status:   "active",
    }
    
    // 普通插入
    if err := table.Insert(ctx, user); err != nil {
        return 0, err
    }
    
    // 插入并返回ID
    return table.InsertAndGetID(ctx, user)
}
```

### 查询数据

```go
func queryUsers(db postgresql_helper.DB) ([]User, error) {
    ctx := context.Background()
    table := db.Table(ctx, "users")
    
    var users []User
    err := table.Query().
        Select("id", "username", "email", "status", "created_at").
        Where("status = ?", "active").
        OrderBy("created_at DESC").
        Limit(10).
        GetAll(ctx, &users)
    
    return users, err
}

func getUserByID(db postgresql_helper.DB, id int) (*User, error) {
    ctx := context.Background()
    table := db.Table(ctx, "users")
    
    var user User
    err := table.Query().
        Where("id = ?", id).
        Get(ctx, &user)
    
    if err != nil {
        if errors.Is(err, types.ErrRecordNotFound) {
            return nil, nil // 用户不存在
        }
        return nil, err
    }
    
    return &user, nil
}
```

### 游标分页

```go
func getUsersWithPagination(db postgresql_helper.DB, lastID int, pageSize int) (*types.PageResult, error) {
    ctx := context.Background()
    table := db.Table(ctx, "users")
    
    var users []User
    return table.Query().
        OrderBy("id ASC").
        PageByKeySince(ctx, &users, "id", lastID, pageSize, false)
}
```

### 更新和删除

```go
func updateUser(db postgresql_helper.DB, id int, status string) (int64, error) {
    ctx := context.Background()
    table := db.Table(ctx, "users")
    
    data := map[string]interface{}{
        "status": status,
    }
    
    args := map[string]interface{}{
        "id": id,
    }
    
    return table.Update(ctx, "id = :id", args, data)
}

func deleteUser(db postgresql_helper.DB, id int) (int64, error) {
    ctx := context.Background()
    table := db.Table(ctx, "users")
    
    args := map[string]interface{}{
        "id": id,
    }
    
    return table.Delete(ctx, "id = :id", args)
}
```

### 事务处理

```go
func transferFunds(db postgresql_helper.DB, fromID, toID int, amount float64) error {
    ctx := context.Background()
    
    return db.InTx(ctx, func(ctx context.Context) error {
        // 在事务中的所有操作
        accountsTable := db.Table(ctx, "accounts")
        
        // 减少发送方余额
        _, err := accountsTable.Update(
            ctx,
            "id = :id",
            map[string]interface{}{"id": fromID},
            map[string]interface{}{"balance": sqlx.Expr("balance - ?", amount)},
        )
        if err != nil {
            return err // 会自动回滚
        }
        
        // 增加接收方余额
        _, err = accountsTable.Update(
            ctx,
            "id = :id",
            map[string]interface{}{"id": toID},
            map[string]interface{}{"balance": sqlx.Expr("balance + ?", amount)},
        )
        if err != nil {
            return err // 会自动回滚
        }
        
        // 记录交易
        transactionsTable := db.Table(ctx, "transactions")
        err = transactionsTable.Insert(ctx, map[string]interface{}{
            "from_id": fromID,
            "to_id":   toID,
            "amount":  amount,
            "date":    time.Now(),
        })
        
        return err // 返回nil将提交事务，返回错误将回滚
    })
}
```

### 数据迁移

```go
func setupMigrations(db postgresql_helper.DB) error {
    migrator, err := postgresql_helper.NewMigrator(db)
    if err != nil {
        return err
    }
    
    // 添加迁移
    err = migrator.Register(postgresql_helper.TimestampMigration(
        "create_users_table",
        "Create users table",
        // 升级函数
        func(ctx context.Context, db postgresql_helper.DB) error {
            schema := db.Schema()
            return schema.CreateTable(ctx, types.TableSchema{
                Name: "users",
                Columns: []types.ColumnDefinition{
                    {Name: "id", Type: "SERIAL", PrimaryKey: true},
                    {Name: "username", Type: "VARCHAR(255)", Nullable: false, Unique: true},
                    {Name: "email", Type: "VARCHAR(255)", Nullable: false},
                    {Name: "created_at", Type: "TIMESTAMP", Nullable: false, Default: "NOW()"},
                },
                IfNotExists: true,
            })
        },
        // 回滚函数
        func(ctx context.Context, db postgresql_helper.DB) error {
            schema := db.Schema()
            return schema.DropTable(ctx, "users", false)
        },
    ))
    if err != nil {
        return err
    }
    
    // 执行迁移
    ctx := context.Background()
    _, err = migrator.MigrateUp(ctx)
    return err
}
```

## 高级用法

### 批量插入/更新

```go
func bulkUpsertUsers(db postgresql_helper.DB, users []User) (int64, error) {
    ctx := context.Background()
    table := db.Table(ctx, "users")
    
    data := make([]interface{}, len(users))
    for i := range users {
        data[i] = users[i]
    }
    
    return table.BulkUpsert(ctx, []string{"username"}, data)
}
```

### 复杂查询

```go
func complexQuery(db postgresql_helper.DB) ([]UserStats, error) {
    ctx := context.Background()
    table := db.Table(ctx, "users")
    
    var stats []UserStats
    err := table.Query().
        Select("status", "COUNT(*) as count", "MAX(created_at) as last_created").
        Join("LEFT JOIN user_logins ON users.id = user_logins.user_id").
        Where("created_at > ?", time.Now().AddDate(0, -1, 0)).
        GroupBy("status").
        Having("COUNT(*) > 5").
        OrderBy("count DESC").
        GetAll(ctx, &stats)
    
    return stats, err
}
```

### 原始 SQL 查询

```go
func rawQuery(db postgresql_helper.DB) ([]map[string]interface{}, error) {
    ctx := context.Background()
    
    rows, err := db.Query(ctx, `
        SELECT 
            u.id, u.username, COUNT(p.id) as post_count
        FROM 
            users u
        LEFT JOIN 
            posts p ON u.id = p.user_id
        GROUP BY 
            u.id, u.username
        HAVING 
            COUNT(p.id) > 0
        ORDER BY 
            post_count DESC
        LIMIT 10
    `)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var results []map[string]interface{}
    for rows.Next() {
        result := make(map[string]interface{})
        err := rows.MapScan(result)
        if err != nil {
            return nil, err
        }
        results = append(results, result)
    }
    
    return results, rows.Err()
}
```

## 监控集成

PostgreSQL Helper 内置了 Prometheus 监控支持，自动收集以下指标：

- 操作总数 (`pgsql_helper_pgsql_total_operate_count`)
- 错误总数 (`pgsql_helper_pgsql_total_error_count`) 
- 操作持续时间 (`pgsql_helper_pgsql_operate_duration_seconds`)
- 连接池状态 (`pgsql_helper_pgsql_connections_open`, `pgsql_helper_pgsql_connections_idle`, `pgsql_helper_pgsql_connections_in_use`)

您可以将这些指标导出到 Prometheus 并在 Grafana 等工具中创建仪表板。

```go
func setupMonitoring(db *postgresql_helper.DB) {
    // 添加连接池统计信息
    db.AddConnectionStats(prometheus.DefaultRegisterer)
    
    // 已注册的默认指标:
    // - pgsql_helper_pgsql_total_operate_count
    // - pgsql_helper_pgsql_total_error_count
    // - pgsql_helper_pgsql_operate_duration_seconds
}
```

## 贡献指南

欢迎贡献！请遵循以下步骤：

1. Fork 仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建一个 Pull Request

## 许可证

本项目采用 MIT 许可证 - 详情请参阅 [LICENSE](LICENSE) 文件。

## 致谢

- [sqlx](https://github.com/jmoiron/sqlx) - 用于增强 Go 数据库标准库
- [Prometheus](https://prometheus.io/) - 监控系统和时间序列数据库