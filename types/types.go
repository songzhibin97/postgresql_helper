package types

import (
	"context"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	ErrDuplicated       = errors.New("duplicated")
	ErrRecordNotFound   = errors.New("record not found")
	ErrInvalidStructure = errors.New("invalid table structure")
)

type (
	ColumnDefinition struct {
		Name       string      `json:"name"`
		Type       string      `json:"type"` // PostgreSQL type
		Nullable   bool        `json:"nullable"`
		Default    string      `json:"default"`
		PrimaryKey bool        `json:"primary_key"`
		Unique     bool        `json:"unique"`
		Index      bool        `json:"index"`
		Check      string      `json:"check"`
		ForeignKey *ForeignKey `json:"foreign_key"`
	}

	ForeignKey struct {
		ReferenceTable  string `json:"ref_table"`
		ReferenceColumn string `json:"ref_column"`
		OnDelete        string `json:"on_delete"` // CASCADE | RESTRICT | SET NULL
		OnUpdate        string `json:"on_update"`
	}

	TableSchema struct {
		Name        string             `json:"name"`
		Columns     []ColumnDefinition `json:"columns"`
		IfNotExists bool               `json:"if_not_exists"`
	}

	QueryConfig struct {
		SelectFields []string `json:"select_fields"`
		WhereClause  string   `json:"where_clause"`
		OrderBy      string   `json:"order_by"`
		Limit        int      `json:"limit"`
		Offset       int      `json:"offset"`
		JoinClauses  []string `json:"join_clauses"`
		GroupBy      string   `json:"group_by"`
		Having       string   `json:"having"`
		ForUpdate    bool     `json:"for_update"`
	}
)

// Cursor 表示分页游标
type Cursor struct {
	// 游标键值（通常是上一页最后一条记录的键值）
	KeyValue interface{} `json:"key_value"`
	// 游标方向（前向或后向）
	Forward bool `json:"forward"`
	// 每页大小
	Limit int `json:"limit"`
}

// PageResult 表示分页查询结果
type PageResult struct {
	// 当前页数据
	Data interface{} `json:"data"`
	// 总记录数（如果请求计数）
	TotalCount int64 `json:"total_count,omitempty"`
	// 下一页游标
	NextCursor *Cursor `json:"next_cursor,omitempty"`
	// 上一页游标
	PrevCursor *Cursor `json:"prev_cursor,omitempty"`
	// 是否有下一页
	HasNext bool `json:"has_next"`
	// 是否有上一页
	HasPrev bool `json:"has_prev"`
}

type CompositeCursor struct {
	// 多个字段的值
	KeyValues map[string]interface{} `json:"key_values"`
	// 排序字段及其方向
	OrderFields []struct {
		Name      string `json:"name"`
		Direction string `json:"direction"` // ASC 或 DESC
	} `json:"order_fields"`
	// 分页方向
	Forward bool `json:"forward"`
	// 每页大小
	Limit int `json:"limit"`
}

// Migration 表示单个数据库迁移
type Migration struct {
	Version     int64      `json:"version"`     // 迁移版本号（通常是时间戳）
	Name        string     `json:"name"`        // 迁移名称
	Description string     `json:"description"` // 迁移描述
	UpFn        MigrateFn  `json:"-"`           // 升级函数
	DownFn      MigrateFn  `json:"-"`           // 回滚函数
	AppliedAt   *time.Time `json:"applied_at"`  // 应用时间
}

// MigrateFn 迁移函数类型
type MigrateFn func(ctx context.Context, db DB) error

// MigrationResult 迁移执行结果
type MigrationResult struct {
	AppliedMigrations []Migration   `json:"applied_migrations"` // 已应用的迁移
	CurrentVersion    int64         `json:"current_version"`    // 当前版本
	Error             error         `json:"error,omitempty"`    // 错误信息（如果有）
	StartVersion      int64         `json:"start_version"`      // 起始版本
	EndVersion        int64         `json:"end_version"`        // 结束版本
	ExecutionTime     time.Duration `json:"execution_time"`     // 执行时间
}

type (
	DB interface {
		// Table 获取表操作接口
		Table(ctx context.Context, tableName string) Table

		// Schema 模式管理接口
		Schema() Schema

		// Ping 健康检查
		Ping(ctx context.Context) error

		// InTx 事务处理
		InTx(ctx context.Context, fn func(ctx context.Context) error) error

		// Close 关闭连接
		Close() error

		// Query 原始SQL执行
		Query(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	}

	Schema interface {
		// CreateTable 动态创建表
		CreateTable(ctx context.Context, schema TableSchema) error

		// AlterTable 修改表结构
		AlterTable(ctx context.Context, tableName string, alterations []string) error

		// DropTable 删除表
		DropTable(ctx context.Context, tableName string, cascade bool) error

		// TableExists 检查表是否存在
		TableExists(ctx context.Context, tableName string) (bool, error)

		// GetTableSchema 获取表结构
		GetTableSchema(ctx context.Context, tableName string) (*TableSchema, error)
	}

	Table interface {
		// Insert 插入记录
		Insert(ctx context.Context, data interface{}) error

		// Update 更新记录
		Update(ctx context.Context, whereClause string, args map[string]interface{}, data interface{}) (int64, error)

		// Delete 删除记录
		Delete(ctx context.Context, whereClause string, args map[string]interface{}) (int64, error)

		// Query 创建查询构建器
		Query() Query

		// AddColumn 添加列
		AddColumn(ctx context.Context, col ColumnDefinition) error

		// DropColumn 删除列
		DropColumn(ctx context.Context, columnName string) error

		// RenameColumn 重命名列
		RenameColumn(ctx context.Context, oldName, newName string) error

		// CreateIndex 创建索引
		CreateIndex(ctx context.Context, indexName string, columns []string, unique bool) error

		// DropIndex 删除索引
		DropIndex(ctx context.Context, indexName string) error

		// BulkUpsert 批量插入/更新
		BulkUpsert(ctx context.Context, conflictKey []string, data []interface{}) (int64, error)
	}

	Query interface {
		Select(fields ...string) Query
		Where(conditions string, args ...interface{}) Query
		OrderBy(fields string) Query
		Limit(n int) Query
		Offset(n int) Query
		Join(joinClause string) Query
		GroupBy(fields string) Query
		Having(conditions string) Query
		ForUpdate() Query

		Get(ctx context.Context, dest interface{}) error
		GetAll(ctx context.Context, dest interface{}) error
		Count(ctx context.Context) (int64, error)
		Exists(ctx context.Context) (bool, error)

		// WithCursor 应用游标分页
		// keyField: 用于分页的键字段（通常是主键）
		// cursor: 分页游标，可以是上一次查询返回的NextCursor或PrevCursor
		WithCursor(keyField string, cursor *Cursor) Query

		// GetPage 执行分页查询并返回结果
		// dest: 结果容器（应为切片指针）
		// withCount: 是否计算总记录数（可能影响性能）
		GetPage(ctx context.Context, dest interface{}, withCount bool) (*PageResult, error)

		// PageByKeySince 从指定键值开始分页
		PageByKeySince(ctx context.Context, dest interface{}, keyField string, keyValue interface{}, limit int, withCount bool) (*PageResult, error)

		// PageByKeyBefore 获取指定键值之前的分页
		PageByKeyBefore(ctx context.Context, dest interface{}, keyField string, keyValue interface{}, limit int, withCount bool) (*PageResult, error)

		WithCompositeCursor(cursor *CompositeCursor) Query
	}
)

// Migrator 数据库迁移管理器接口
type Migrator interface {
	// Register 注册迁移
	Register(migration Migration) error

	// MigrateUp 执行所有待应用的迁移
	MigrateUp(ctx context.Context) (*MigrationResult, error)

	// MigrateUpTo 迁移到指定版本（含）
	MigrateUpTo(ctx context.Context, targetVersion int64) (*MigrationResult, error)

	// MigrateDown 回滚最近的n个迁移
	MigrateDown(ctx context.Context, steps int) (*MigrationResult, error)

	// MigrateDownTo 回滚到指定版本（含）
	MigrateDownTo(ctx context.Context, targetVersion int64) (*MigrationResult, error)

	// GetCurrentVersion 获取当前迁移版本
	GetCurrentVersion(ctx context.Context) (int64, error)

	// GetAppliedMigrations 获取已应用的迁移列表
	GetAppliedMigrations(ctx context.Context) ([]Migration, error)

	// CreateMigrationsTable 创建迁移表（如果不存在）
	CreateMigrationsTable(ctx context.Context) error
}
