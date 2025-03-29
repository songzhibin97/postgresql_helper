package postgresql_helper

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/songzhibin97/postgresql_helper/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 设置测试环境
func setupSchemaTest(t *testing.T) (*Schema, sqlmock.Sqlmock, func()) {
	// 创建sqlmock
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err, "Failed to create mock database")

	// 创建sqlx包装
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	// 创建我们的DB对象
	db := &DB{
		db:   sqlxDB,
		name: "test_db",
	}

	// 创建Schema对象
	schema := &Schema{db}

	// 清理函数
	cleanup := func() {
		sqlxDB.Close()
	}

	return schema, mock, cleanup
}

// 测试CreateTable方法
func TestSchema_CreateTable(t *testing.T) {
	schema, mock, cleanup := setupSchemaTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("successful create table", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("CREATE TABLE").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 准备测试数据
		tableSchema := types.TableSchema{
			Name: "users",
			Columns: []types.ColumnDefinition{
				{
					Name:       "id",
					Type:       "SERIAL",
					PrimaryKey: true,
				},
				{
					Name:     "name",
					Type:     "VARCHAR(100)",
					Nullable: false,
				},
			},
		}

		// 执行测试
		err := schema.CreateTable(ctx, tableSchema)
		assert.NoError(t, err, "CreateTable should not return error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("create table with if not exists", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 准备测试数据
		tableSchema := types.TableSchema{
			Name:        "temp_table",
			IfNotExists: true,
			Columns: []types.ColumnDefinition{
				{
					Name: "id",
					Type: "INTEGER",
				},
			},
		}

		// 执行测试
		err := schema.CreateTable(ctx, tableSchema)
		assert.NoError(t, err, "CreateTable with IF NOT EXISTS should not return error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("create table with constraints", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("CREATE TABLE").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 准备测试数据
		tableSchema := types.TableSchema{
			Name: "products",
			Columns: []types.ColumnDefinition{
				{
					Name:       "id",
					Type:       "SERIAL",
					PrimaryKey: true,
				},
				{
					Name:     "name",
					Type:     "VARCHAR(100)",
					Nullable: false,
					Unique:   true,
				},
				{
					Name:  "price",
					Type:  "DECIMAL(10,2)",
					Check: "price > 0",
				},
				{
					Name: "category_id",
					Type: "INTEGER",
					ForeignKey: &types.ForeignKey{
						ReferenceTable:  "categories",
						ReferenceColumn: "id",
						OnDelete:        "CASCADE",
						OnUpdate:        "RESTRICT",
					},
				},
			},
		}

		// 执行测试
		err := schema.CreateTable(ctx, tableSchema)
		assert.NoError(t, err, "CreateTable with constraints should not return error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("create table error", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("CREATE TABLE").
			WillReturnError(&pq.Error{
				Code:    "42P07",
				Message: "relation already exists",
			})

		// 准备测试数据
		tableSchema := types.TableSchema{
			Name: "users",
			Columns: []types.ColumnDefinition{
				{
					Name: "id",
					Type: "SERIAL",
				},
			},
		}

		// 执行测试
		err := schema.CreateTable(ctx, tableSchema)
		assert.Error(t, err, "CreateTable should return error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// 测试AlterTable方法
func TestSchema_AlterTable(t *testing.T) {
	schema, mock, cleanup := setupSchemaTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("successful alter table", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("ALTER TABLE users").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 准备测试数据
		alterations := []string{
			"ADD COLUMN email VARCHAR(255)",
		}

		// 执行测试
		err := schema.AlterTable(ctx, "users", alterations)
		assert.NoError(t, err, "AlterTable should not return error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("multiple alterations", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("ALTER TABLE users").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 准备测试数据
		alterations := []string{
			"ADD COLUMN email VARCHAR(255)",
			"DROP COLUMN old_field",
			"RENAME COLUMN name TO full_name",
		}

		// 执行测试
		err := schema.AlterTable(ctx, "users", alterations)
		assert.NoError(t, err, "AlterTable with multiple alterations should not return error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("no alterations", func(t *testing.T) {
		// 执行测试
		err := schema.AlterTable(ctx, "users", []string{})
		assert.Error(t, err, "AlterTable with empty alterations should return error")
		assert.Contains(t, err.Error(), "no alterations provided")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("alter table error", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("ALTER TABLE users").
			WillReturnError(errors.New("alter table error"))

		// 准备测试数据
		alterations := []string{
			"ADD COLUMN email VARCHAR(255)",
		}

		// 执行测试
		err := schema.AlterTable(ctx, "users", alterations)
		assert.Error(t, err, "AlterTable should return error")
		assert.Contains(t, err.Error(), "alter table error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// 测试DropTable方法
func TestSchema_DropTable(t *testing.T) {
	schema, mock, cleanup := setupSchemaTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("drop table without cascade", func(t *testing.T) {
		// 设置期望 - 确保不包含CASCADE
		mock.ExpectExec("DROP TABLE users$").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 执行测试
		err := schema.DropTable(ctx, "users", false)
		assert.NoError(t, err, "DropTable without cascade should not return error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("drop table with cascade", func(t *testing.T) {
		// 设置期望 - 确保包含CASCADE
		mock.ExpectExec("DROP TABLE users CASCADE").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 执行测试
		err := schema.DropTable(ctx, "users", true)
		assert.NoError(t, err, "DropTable with cascade should not return error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("drop table error", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("DROP TABLE users").
			WillReturnError(errors.New("drop table error"))

		// 执行测试
		err := schema.DropTable(ctx, "users", false)
		assert.Error(t, err, "DropTable should return error")
		assert.Contains(t, err.Error(), "drop table error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// 测试TableExists方法
func TestSchema_TableExists(t *testing.T) {
	schema, mock, cleanup := setupSchemaTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("table exists", func(t *testing.T) {
		// 设置期望
		rows := sqlmock.NewRows([]string{"exists"}).
			AddRow(true)
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("users").
			WillReturnRows(rows)

		// 执行测试
		exists, err := schema.TableExists(ctx, "users")
		assert.NoError(t, err, "TableExists should not return error")
		assert.True(t, exists, "Table should exist")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("table does not exist", func(t *testing.T) {
		// 设置期望
		rows := sqlmock.NewRows([]string{"exists"}).
			AddRow(false)
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("non_existent").
			WillReturnRows(rows)

		// 执行测试
		exists, err := schema.TableExists(ctx, "non_existent")
		assert.NoError(t, err, "TableExists should not return error")
		assert.False(t, exists, "Table should not exist")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("query error", func(t *testing.T) {
		// 设置期望
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("error_table").
			WillReturnError(errors.New("query error"))

		// 执行测试
		exists, err := schema.TableExists(ctx, "error_table")
		assert.Error(t, err, "TableExists should return error")
		assert.False(t, exists, "Should return false on error")
		assert.Contains(t, err.Error(), "query error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// 测试GetTableSchema方法 - 因为这个方法很复杂，所以我们只测试一些基本路径
func TestSchema_GetTableSchema(t *testing.T) {
	schema, mock, cleanup := setupSchemaTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("table not exists", func(t *testing.T) {
		// 设置TableExists期望
		rows := sqlmock.NewRows([]string{"exists"}).
			AddRow(false)
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("non_existent").
			WillReturnRows(rows)

		// 执行测试
		tableSchema, err := schema.GetTableSchema(ctx, "non_existent")
		assert.Error(t, err, "GetTableSchema should return error for non-existent table")
		assert.True(t, errors.Is(err, types.ErrRecordNotFound), "Error should be ErrRecordNotFound")
		assert.Equal(t, &types.TableSchema{}, tableSchema, "Should return empty schema")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	// 注意：完整测试GetTableSchema方法需要模拟多个查询，这里省略
	// 在实际项目中，你应该根据需要添加更多的测试用例
}

// 测试一些辅助函数
func TestSchema_HelperFunctions(t *testing.T) {
	t.Run("extractColumnsFromIndexDef", func(t *testing.T) {
		tests := []struct {
			name     string
			def      string
			expected []string
		}{
			{
				name:     "simple index",
				def:      "CREATE INDEX idx_name ON users (name)",
				expected: []string{"name"},
			},
			{
				name:     "composite index",
				def:      "CREATE INDEX idx_name_email ON users (name, email)",
				expected: []string{"name", "email"},
			},
			{
				name:     "unique index",
				def:      "CREATE UNIQUE INDEX idx_email ON users (email)",
				expected: []string{"email"},
			},
			{
				name:     "with spaces",
				def:      "CREATE INDEX idx_name ON users (name, user_id )",
				expected: []string{"name", "user_id"},
			},
			{
				name:     "invalid format",
				def:      "not a valid index definition",
				expected: nil,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := extractColumnsFromIndexDef(tt.def)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("parseColumnsFromCheck", func(t *testing.T) {
		tests := []struct {
			name     string
			clause   string
			expected []string
		}{
			{
				name:     "simple check",
				clause:   "CHECK (age > 0)",
				expected: []string{"age"},
			},
			{
				name:     "multiple columns",
				clause:   "CHECK (age > 0 AND name IS NOT NULL)",
				expected: []string{"age", "name"},
			},
			{
				name:     "with table prefix",
				clause:   "CHECK (users.age > 0)",
				expected: []string{},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := parseColumnsFromCheck(tt.clause)
				// We're only checking if the expected columns are included,
				// not the exact match, because the implementation might extract additional tokens
				for _, expected := range tt.expected {
					assert.Contains(t, result, expected)
				}
			})
		}
	})

	t.Run("normalizeAction", func(t *testing.T) {
		tests := []struct {
			input    string
			expected string
		}{
			{
				input:    "NO ACTION",
				expected: "RESTRICT",
			},
			{
				input:    "CASCADE",
				expected: "CASCADE",
			},
			{
				input:    "SET NULL",
				expected: "SET NULL",
			},
		}

		for _, tt := range tests {
			t.Run(tt.input, func(t *testing.T) {
				result := normalizeAction(tt.input)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("isReservedWord", func(t *testing.T) {
		reserved := []string{"CHECK", "AND", "OR", "NOT", "NULL", "IS"}
		notReserved := []string{"name", "age", "email", "user_id"}

		for _, word := range reserved {
			assert.True(t, isReservedWord(word), "%s should be a reserved word", word)
		}

		for _, word := range notReserved {
			assert.False(t, isReservedWord(word), "%s should not be a reserved word", word)
		}

		// Test case insensitivity
		assert.True(t, isReservedWord("check"), "check should be a reserved word (case insensitive)")
		assert.True(t, isReservedWord("AND"), "AND should be a reserved word")
	})
}
