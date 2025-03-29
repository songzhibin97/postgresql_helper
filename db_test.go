package postgresql_helper

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

// 创建一个测试用的DB对象
func newTestDB() *DB {
	return &DB{
		db:   nil,
		name: "test_db",
	}
}

// 测试 wrapError 方法
func TestDB_WrapError(t *testing.T) {
	db := newTestDB()

	tests := []struct {
		name      string
		err       error
		operation string
		contains  string
	}{
		{
			name:      "nil error",
			err:       nil,
			operation: "test operation",
			contains:  "",
		},
		{
			name:      "record not found",
			err:       sql.ErrNoRows,
			operation: "find user",
			contains:  "record not found: find user",
		},
		{
			name:      "generic error",
			err:       errors.New("database error"),
			operation: "update record",
			contains:  "update record: database error",
		},
		{
			name:      "unique violation",
			err:       &pq.Error{Code: "23505", Detail: "Key already exists"},
			operation: "insert record",
			contains:  "unique violation: insert record",
		},
		{
			name:      "foreign key violation",
			err:       &pq.Error{Code: "23503", Detail: "Referenced row not found"},
			operation: "insert record",
			contains:  "foreign key violation: insert record",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := db.wrapError(tt.err, tt.operation)

			if tt.err == nil {
				assert.Nil(t, result, "Should return nil for nil error")
			} else {
				assert.Error(t, result, "Should return an error for non-nil error")
				assert.Contains(t, result.Error(), tt.contains, "Error message should contain expected text")
			}
		})
	}
}

// 测试Table方法
func TestDB_Table(t *testing.T) {
	db := newTestDB()
	ctx := context.Background()

	table := db.Table(ctx, "users")
	assert.NotNil(t, table, "Table should not be nil")

	tableImpl, ok := table.(*Table)
	assert.True(t, ok, "Should return a *Table type")
	assert.Equal(t, "users", tableImpl.name, "Table name should match")
	assert.Equal(t, db, tableImpl.DB, "Table's DB reference should match original DB")
}

// 测试Schema方法
func TestDB_Schema(t *testing.T) {
	db := newTestDB()

	schema := db.Schema()
	assert.NotNil(t, schema, "Schema should not be nil")

	_, ok := schema.(*Schema)
	assert.True(t, ok, "Should return a *Schema type")

	// 基于源码中Schema结构的定义，我们只需要确认schema不为nil
	// 不对schemaImpl.db进行断言，因为它可能是sqlx.DB或者是*DB
	// 这取决于具体实现
}

// 测试DefaultDBConfig
func TestDefaultDBConfig(t *testing.T) {
	config := DefaultDBConfig()

	assert.Equal(t, 25, config.MaxOpenConns, "Default MaxOpenConns should be 25")
	assert.Equal(t, 10, config.MaxIdleConns, "Default MaxIdleConns should be 10")
	assert.Equal(t, 15*time.Minute, config.ConnMaxLifetime, "Default ConnMaxLifetime should be 15 minutes")
	assert.Equal(t, 5*time.Minute, config.ConnMaxIdleTime, "Default ConnMaxIdleTime should be 5 minutes")
}

// 测试extractDatabaseName
func TestExtractDatabaseName(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		expected string
	}{
		{
			name:     "standard postgres url",
			dsn:      "postgres://user:pass@localhost:5432/mydb",
			expected: "mydb",
		},
		{
			name:     "with query parameters",
			dsn:      "postgres://user:pass@localhost:5432/mydb?sslmode=disable",
			expected: "mydb",
		},
		{
			name:     "invalid format",
			dsn:      "invalid-dsn",
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractDatabaseName(tt.dsn)
			assert.Equal(t, tt.expected, result, "Database name extraction should match expected result")
		})
	}
}

// 测试New函数
func TestNewAndConnect(t *testing.T) {
	t.Run("missing DSN", func(t *testing.T) {
		config := DefaultDBConfig()
		// DSN not set

		db, err := New(config)
		assert.Error(t, err, "Should return error when DSN is missing")
		assert.Nil(t, db, "Should return nil DB when config is invalid")
		assert.Contains(t, err.Error(), "database DSN is required", "Error message should indicate DSN is required")
	})
}

// 测试一些特殊的错误类型
func TestErrorTypes(t *testing.T) {
	assert.Equal(t, "duplicated", ErrDuplicated.Error())
	assert.Equal(t, "record not found", ErrRecordNotFound.Error())
	assert.Equal(t, "invalid table structure", ErrInvalidStructure.Error())
	assert.Equal(t, "permission denied", ErrPermissionDenied.Error())
	assert.Equal(t, "database connection failed", ErrConnectionFailed.Error())
	assert.Equal(t, "query timeout", ErrQueryTimeout.Error())
	assert.Equal(t, "constraint violation", ErrConstraintViolation.Error())
	assert.Equal(t, "foreign key violation", ErrForeignKeyViolation.Error())
	assert.Equal(t, "unique violation", ErrUniqueViolation.Error())
	assert.Equal(t, "check constraint violation", ErrCheckViolation.Error())
}

// 测试操作类型常量
func TestOperTypes(t *testing.T) {
	assert.Equal(t, oper("query"), queryOper)
	assert.Equal(t, oper("insert"), insertOper)
	assert.Equal(t, oper("update"), updateOper)
	assert.Equal(t, oper("upsert"), upsertOper)
	assert.Equal(t, oper("delete"), deleteOper)
	assert.Equal(t, oper("column"), columnOper)
	assert.Equal(t, oper("index"), indexOper)
	assert.Equal(t, oper("create"), createOper)
	assert.Equal(t, oper("alert"), alertOper)
}

// 测试指标收集函数 - 这些只是功能性测试，不检查实际的指标收集
func TestMetricsFunctions(t *testing.T) {
	// 测试函数不应该panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Metrics functions should not panic: %v", r)
		}
	}()

	collectOperCount("test_collection", queryOper)
	collectErrorCount("test_collection", queryOper)
	collectOperDuration("test_collection", queryOper, 100*time.Millisecond)
}
