package postgresql_helper

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/songzhibin97/postgresql_helper/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// User 测试用结构体
type TestUser struct {
	Name  string `db:"name"`
	Email string `db:"email"`
	Age   int    `db:"age"`
}

type User struct {
	ID    int    `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
	Age   int    `db:"age"`
}

// setupTableTest 设置表测试环境
func setupTableTest(t *testing.T) (*Table, sqlmock.Sqlmock, func()) {
	// 创建sqlmock，使用PostgreSQL风格参数占位符($1,$2...)并启用正则表达式匹配
	mockDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
	)
	require.NoError(t, err, "Failed to create mock database")

	// 创建sqlx包装
	sqlxDB := sqlx.NewDb(mockDB, "postgres")

	// 创建我们的DB对象
	db := &DB{
		db:   sqlxDB,
		name: "test_db",
	}

	// 创建Table对象
	table := &Table{
		DB:   db,
		name: "users",
	}

	// 清理函数
	cleanup := func() {
		sqlxDB.Close()
	}

	return table, mock, cleanup
}

// TestTable_Insert 测试Insert方法
func TestTable_Insert(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("insert struct success", func(t *testing.T) {
		// 使用没有ID的结构体避免参数不匹配
		user := TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Age:   30,
		}

		// 设置期望 - 使用AnyArg()进行模糊匹配
		mock.ExpectExec(`INSERT INTO users \(.*\) VALUES \(.*\)`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))

		// 执行测试
		err := table.Insert(ctx, user)
		assert.NoError(t, err, "Insert should succeed")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("insert map success", func(t *testing.T) {
		// 准备测试数据
		userData := map[string]interface{}{
			"name":  "Jane Doe",
			"email": "jane@example.com",
			"age":   25,
		}

		// 设置期望 - 使用AnyArg()进行模糊匹配
		mock.ExpectExec(`INSERT INTO users \(.*\) VALUES \(.*\)`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))

		// 执行测试
		err := table.Insert(ctx, userData)
		assert.NoError(t, err, "Insert with map should succeed")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("insert error", func(t *testing.T) {
		// 准备测试数据
		user := TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Age:   30,
		}

		// 设置期望 - 返回错误
		mock.ExpectExec(`INSERT INTO users \(.*\) VALUES \(.*\)`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnError(&pq.Error{
				Code:    "23505",
				Message: "duplicate key value violates unique constraint",
			})

		// 执行测试
		err := table.Insert(ctx, user)
		assert.Error(t, err, "Insert should return error for duplicate")
		assert.Contains(t, err.Error(), "unique violation")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("invalid data type", func(t *testing.T) {
		// 执行测试 - 传入非结构体非map类型
		err := table.Insert(ctx, "invalid")
		assert.Error(t, err, "Insert should return error for invalid data type")
		assert.Contains(t, err.Error(), "invalid table structure")
	})
}

// TestTable_InsertAndGetID 测试InsertAndGetID方法
func TestTable_InsertAndGetID(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("insert and get id success", func(t *testing.T) {
		// 使用没有ID的结构体避免参数不匹配
		user := TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Age:   30,
		}

		// 设置期望 - 使用模糊匹配
		rows := sqlmock.NewRows([]string{"id"}).AddRow(123)
		mock.ExpectQuery(`INSERT INTO users \(.*\) VALUES \(.*\) RETURNING id`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(rows)

		// 执行测试
		id, err := table.InsertAndGetID(ctx, user)
		assert.NoError(t, err, "InsertAndGetID should succeed")
		assert.Equal(t, int64(123), id, "Should return correct ID")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("custom id column", func(t *testing.T) {
		// 使用没有ID的结构体
		user := TestUser{
			Name:  "Jane Doe",
			Email: "jane@example.com",
			Age:   25,
		}

		// 设置期望 - 注意这里是RETURNING user_id并使用模糊匹配
		rows := sqlmock.NewRows([]string{"user_id"}).AddRow(456)
		mock.ExpectQuery(`INSERT INTO users \(.*\) VALUES \(.*\) RETURNING user_id`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(rows)

		// 执行测试
		id, err := table.InsertAndGetID(ctx, user, "user_id")
		assert.NoError(t, err, "InsertAndGetID with custom column should succeed")
		assert.Equal(t, int64(456), id, "Should return correct ID")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("scan error", func(t *testing.T) {
		// 使用没有ID的结构体
		user := TestUser{
			Name:  "Invalid",
			Email: "invalid@example.com",
			Age:   0,
		}

		// 设置期望 - 返回错误
		mock.ExpectQuery(`INSERT INTO users \(.*\) VALUES \(.*\) RETURNING id`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnError(errors.New("database error"))

		// 执行测试
		id, err := table.InsertAndGetID(ctx, user)
		assert.Error(t, err, "InsertAndGetID should return error")
		assert.Equal(t, int64(0), id, "ID should be 0 on error")
		assert.Contains(t, err.Error(), "database error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// TestTable_InsertAndGetMultipleColumns 测试InsertAndGetMultipleColumns方法
func TestTable_InsertAndGetMultipleColumns(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("insert and get multiple columns success", func(t *testing.T) {
		// 使用没有ID的结构体
		user := TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Age:   30,
		}

		// 设置期望 - 使用模糊匹配
		rows := sqlmock.NewRows([]string{"id", "created_at"}).
			AddRow(123, "2023-01-01 12:00:00")

		mock.ExpectQuery(`INSERT INTO users \(.*\) VALUES \(.*\) RETURNING id, created_at`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(rows)

		// 执行测试
		result, err := table.InsertAndGetMultipleColumns(ctx, user, []string{"id", "created_at"})
		assert.NoError(t, err, "InsertAndGetMultipleColumns should succeed")
		assert.Equal(t, 2, len(result), "Should return 2 columns")
		assert.Equal(t, int64(123), result["id"], "ID should match")
		assert.Equal(t, "2023-01-01 12:00:00", result["created_at"], "created_at should match")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("empty return columns", func(t *testing.T) {
		// 使用没有ID的结构体
		user := TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Age:   30,
		}

		// 执行测试
		result, err := table.InsertAndGetMultipleColumns(ctx, user, []string{})
		assert.Error(t, err, "InsertAndGetMultipleColumns should return error for empty columns")
		assert.Nil(t, result, "Result should be nil for error")
		assert.Contains(t, err.Error(), "no return columns specified")
	})

	t.Run("scan error", func(t *testing.T) {
		// 使用没有ID的结构体
		user := TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Age:   30,
		}

		// 设置期望 - 返回错误
		mock.ExpectQuery(`INSERT INTO users \(.*\) VALUES \(.*\) RETURNING id, email`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnError(errors.New("scan error"))

		// 执行测试
		result, err := table.InsertAndGetMultipleColumns(ctx, user, []string{"id", "email"})

		// 检查错误和结果
		if assert.Error(t, err, "InsertAndGetMultipleColumns should return error") {
			assert.Contains(t, err.Error(), "scan error", "Error message should mention scan error")
		}

		// 检查结果映射
		// 注意：根据实现，结果可能是nil或空map，这里我们放宽检查条件
		// 如果是空map也认为符合预期
		if result != nil {
			assert.Empty(t, result, "Result should be empty for error")
		}

		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// TestTable_InsertAndGetObject 测试InsertAndGetObject方法
func TestTable_InsertAndGetObject(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("insert and get object success", func(t *testing.T) {
		// 使用完整的User结构体，因为返回的结果也是User
		inputUser := TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Age:   30,
		}

		// 设置期望 - 匹配任何RETURNING子句
		rows := sqlmock.NewRows([]string{"id", "name", "email", "age"}).
			AddRow(123, "John Doe", "john@example.com", 30)

		mock.ExpectQuery(`INSERT INTO users \(.*\) VALUES \(.*\) RETURNING .*`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(rows)

		// 执行测试 - 使用User结构体接收结果
		var resultUser User // 这里使用User而不是TestUser，因为需要接收ID
		err := table.InsertAndGetObject(ctx, inputUser, &resultUser)
		assert.NoError(t, err, "InsertAndGetObject should succeed")
		assert.Equal(t, 123, resultUser.ID, "ID should be populated")
		assert.Equal(t, "John Doe", resultUser.Name, "Name should match")
		assert.Equal(t, "john@example.com", resultUser.Email, "Email should match")
		assert.Equal(t, 30, resultUser.Age, "Age should match")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("nil destination", func(t *testing.T) {
		// 使用TestUser结构体
		inputUser := TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Age:   30,
		}

		// 执行测试
		var resultUser *User = nil
		err := table.InsertAndGetObject(ctx, inputUser, resultUser)
		assert.Error(t, err, "InsertAndGetObject should return error for nil destination")
		assert.Contains(t, err.Error(), "destination must be a non-nil pointer")
	})

	t.Run("non-pointer destination", func(t *testing.T) {
		// 使用TestUser结构体
		inputUser := TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Age:   30,
		}

		// 执行测试
		var resultUser User
		err := table.InsertAndGetObject(ctx, inputUser, resultUser) // 传递值而非指针
		assert.Error(t, err, "InsertAndGetObject should return error for non-pointer destination")
		assert.Contains(t, err.Error(), "destination must be a non-nil pointer")
	})

	t.Run("scan error", func(t *testing.T) {
		// 使用TestUser结构体
		inputUser := TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Age:   30,
		}

		// 设置期望 - 返回错误
		mock.ExpectQuery(`INSERT INTO users \(.*\) VALUES \(.*\) RETURNING .*`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnError(errors.New("scan error"))

		// 执行测试
		var resultUser User
		err := table.InsertAndGetObject(ctx, inputUser, &resultUser)
		assert.Error(t, err, "InsertAndGetObject should return error")
		assert.Contains(t, err.Error(), "scan error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// TestTable_Update 测试Update方法
func TestTable_Update(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("update success", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("UPDATE users SET .*").
			WillReturnResult(sqlmock.NewResult(0, 2))

		// 准备测试数据
		whereClause := "age > $1"
		whereArgs := map[string]interface{}{
			"1": 20,
		}
		updateData := map[string]interface{}{
			"active": true,
			"status": "verified",
		}

		// 执行测试
		affected, err := table.Update(ctx, whereClause, whereArgs, updateData)
		assert.NoError(t, err, "Update should succeed")
		assert.Equal(t, int64(2), affected, "Should affect 2 rows")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("update no rows", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("UPDATE users SET .*").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 准备测试数据
		whereClause := "id = $1"
		whereArgs := map[string]interface{}{
			"1": 999, // 不存在的ID
		}
		updateData := map[string]interface{}{
			"name": "New Name",
		}

		// 执行测试
		affected, err := table.Update(ctx, whereClause, whereArgs, updateData)
		assert.NoError(t, err, "Update should succeed even with no rows affected")
		assert.Equal(t, int64(0), affected, "Should affect 0 rows")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("update error", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("UPDATE users SET .*").
			WillReturnError(errors.New("update error"))

		// 准备测试数据
		whereClause := "id = $1"
		whereArgs := map[string]interface{}{
			"1": 1,
		}
		updateData := map[string]interface{}{
			"email": "invalid@email",
		}

		// 执行测试
		affected, err := table.Update(ctx, whereClause, whereArgs, updateData)
		assert.Error(t, err, "Update should return error")
		assert.Equal(t, int64(0), affected, "Affected rows should be 0 on error")
		assert.Contains(t, err.Error(), "update error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// TestTable_Delete 测试Delete方法
func TestTable_Delete(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("delete success", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("DELETE FROM users WHERE .*").
			WillReturnResult(sqlmock.NewResult(0, 1))

		// 准备测试数据
		whereClause := "id = $1"
		whereArgs := map[string]interface{}{
			"1": 1,
		}

		// 执行测试
		affected, err := table.Delete(ctx, whereClause, whereArgs)
		assert.NoError(t, err, "Delete should succeed")
		assert.Equal(t, int64(1), affected, "Should affect 1 row")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("delete multiple rows", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("DELETE FROM users WHERE .*").
			WillReturnResult(sqlmock.NewResult(0, 3))

		// 准备测试数据
		whereClause := "age < $1"
		whereArgs := map[string]interface{}{
			"1": 18,
		}

		// 执行测试
		affected, err := table.Delete(ctx, whereClause, whereArgs)
		assert.NoError(t, err, "Delete should succeed")
		assert.Equal(t, int64(3), affected, "Should affect 3 rows")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("delete no rows", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("DELETE FROM users WHERE .*").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 准备测试数据
		whereClause := "id = $1"
		whereArgs := map[string]interface{}{
			"1": 999, // 不存在的ID
		}

		// 执行测试
		affected, err := table.Delete(ctx, whereClause, whereArgs)
		assert.NoError(t, err, "Delete should succeed even with no rows affected")
		assert.Equal(t, int64(0), affected, "Should affect 0 rows")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("delete error", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("DELETE FROM users WHERE .*").
			WillReturnError(errors.New("delete error"))

		// 准备测试数据
		whereClause := "id = $1"
		whereArgs := map[string]interface{}{
			"1": 1,
		}

		// 执行测试
		affected, err := table.Delete(ctx, whereClause, whereArgs)
		assert.Error(t, err, "Delete should return error")
		assert.Equal(t, int64(0), affected, "Affected rows should be 0 on error")
		assert.Contains(t, err.Error(), "delete error")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// TestTable_Query 测试Query方法
func TestTable_Query(t *testing.T) {
	table, _, cleanup := setupTableTest(t)
	defer cleanup()

	query := table.Query()
	assert.NotNil(t, query, "Query should return a non-nil Query object")
	assert.IsType(t, &Query{}, query, "Query should return correct type")

	queryObj, ok := query.(*Query)
	assert.True(t, ok, "Query should be of correct underlying type")
	assert.Equal(t, "users", queryObj.table, "Query should reference correct table")
}

// TestTable_AddColumn 测试AddColumn方法
func TestTable_AddColumn(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("add column success", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("ALTER TABLE users ADD COLUMN status VARCHAR").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 准备测试数据
		col := types.ColumnDefinition{
			Name:     "status",
			Type:     "VARCHAR",
			Nullable: true,
		}

		// 执行测试
		err := table.AddColumn(ctx, col)
		assert.NoError(t, err, "AddColumn should succeed")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("add not null column", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("ALTER TABLE users ADD COLUMN address VARCHAR NOT NULL").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 准备测试数据
		col := types.ColumnDefinition{
			Name:     "address",
			Type:     "VARCHAR",
			Nullable: false,
		}

		// 执行测试
		err := table.AddColumn(ctx, col)
		assert.NoError(t, err, "AddColumn with NOT NULL should succeed")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("add column error", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("ALTER TABLE users ADD COLUMN").
			WillReturnError(errors.New("column already exists"))

		// 准备测试数据
		col := types.ColumnDefinition{
			Name: "id", // 尝试添加已存在的列
			Type: "SERIAL",
		}

		// 执行测试
		err := table.AddColumn(ctx, col)
		assert.Error(t, err, "AddColumn should return error for duplicate column")
		assert.Contains(t, err.Error(), "column already exists")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// TestTable_DropColumn 测试DropColumn方法
func TestTable_DropColumn(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("drop column success", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("ALTER TABLE users DROP COLUMN status").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 执行测试
		err := table.DropColumn(ctx, "status")
		assert.NoError(t, err, "DropColumn should succeed")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("drop column error", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("ALTER TABLE users DROP COLUMN").
			WillReturnError(errors.New("column does not exist"))

		// 执行测试
		err := table.DropColumn(ctx, "nonexistent")
		assert.Error(t, err, "DropColumn should return error for nonexistent column")
		assert.Contains(t, err.Error(), "column does not exist")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// TestTable_RenameColumn 测试RenameColumn方法
func TestTable_RenameColumn(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("rename column success", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("ALTER TABLE users RENAME COLUMN old_name TO new_name").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 执行测试
		err := table.RenameColumn(ctx, "old_name", "new_name")
		assert.NoError(t, err, "RenameColumn should succeed")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("rename column error", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("ALTER TABLE users RENAME COLUMN").
			WillReturnError(errors.New("column does not exist"))

		// 执行测试
		err := table.RenameColumn(ctx, "nonexistent", "new_name")
		assert.Error(t, err, "RenameColumn should return error for nonexistent column")
		assert.Contains(t, err.Error(), "column does not exist")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// TestTable_CreateIndex 测试CreateIndex方法
func TestTable_CreateIndex(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("create index success", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("CREATE INDEX idx_name ON users \\(name\\)").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 执行测试
		err := table.CreateIndex(ctx, "idx_name", []string{"name"}, false)
		assert.NoError(t, err, "CreateIndex should succeed")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("create unique index", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("CREATE UNIQUE INDEX idx_email ON users \\(email\\)").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 执行测试
		err := table.CreateIndex(ctx, "idx_email", []string{"email"}, true)
		assert.NoError(t, err, "CreateIndex with UNIQUE should succeed")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("create composite index", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("CREATE INDEX idx_name_email ON users \\(name, email\\)").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 执行测试
		err := table.CreateIndex(ctx, "idx_name_email", []string{"name", "email"}, false)
		assert.NoError(t, err, "CreateIndex with multiple columns should succeed")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("no columns specified", func(t *testing.T) {
		// 执行测试
		err := table.CreateIndex(ctx, "idx_empty", []string{}, false)
		assert.Error(t, err, "CreateIndex should return error with empty columns")
		assert.Contains(t, err.Error(), "no columns specified")
	})

	t.Run("create index error", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("CREATE INDEX").
			WillReturnError(errors.New("index already exists"))

		// 执行测试
		err := table.CreateIndex(ctx, "idx_existing", []string{"name"}, false)
		assert.Error(t, err, "CreateIndex should return error for duplicate index")
		assert.Contains(t, err.Error(), "index already exists")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// TestTable_DropIndex 测试DropIndex方法
func TestTable_DropIndex(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("drop index success", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("DROP INDEX idx_name").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// 执行测试
		err := table.DropIndex(ctx, "idx_name")
		assert.NoError(t, err, "DropIndex should succeed")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("drop index error", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("DROP INDEX").
			WillReturnError(errors.New("index does not exist"))

		// 执行测试
		err := table.DropIndex(ctx, "idx_nonexistent")
		assert.Error(t, err, "DropIndex should return error for nonexistent index")
		assert.Contains(t, err.Error(), "index does not exist")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})
}

// TestTable_BulkUpsert 测试BulkUpsert方法
func TestTable_BulkUpsert(t *testing.T) {
	table, mock, cleanup := setupTableTest(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("empty data", func(t *testing.T) {
		// 执行测试
		affected, err := table.BulkUpsert(ctx, []string{"id"}, []interface{}{})
		assert.NoError(t, err, "BulkUpsert with empty data should succeed")
		assert.Equal(t, int64(0), affected, "Affected rows should be 0 for empty data")
	})

	t.Run("bulk insert success", func(t *testing.T) {
		// 设置期望 - 注意这里的SQL匹配可能很复杂
		mock.ExpectExec("INSERT INTO users").
			WillReturnResult(sqlmock.NewResult(0, 2))

		// 准备测试数据
		type User struct {
			ID    int    `db:"id"`
			Name  string `db:"name"`
			Email string `db:"email"`
			Age   int    `db:"age"`
		}

		users := []interface{}{
			User{Name: "User1", Email: "user1@example.com", Age: 25},
			User{Name: "User2", Email: "user2@example.com", Age: 30},
		}

		// 执行测试
		affected, err := table.BulkUpsert(ctx, []string{}, users)
		assert.NoError(t, err, "BulkUpsert should succeed")
		assert.Equal(t, int64(2), affected, "Should affect 2 rows")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("upsert with conflict key", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("INSERT INTO users .* ON CONFLICT \\(id\\) DO UPDATE SET").
			WillReturnResult(sqlmock.NewResult(0, 3))

		// 准备测试数据
		type User struct {
			ID    int    `db:"id"`
			Name  string `db:"name"`
			Email string `db:"email"`
			Age   int    `db:"age"`
		}

		users := []interface{}{
			User{ID: 1, Name: "User1", Email: "user1@example.com", Age: 25},
			User{ID: 2, Name: "User2", Email: "user2@example.com", Age: 30},
			User{ID: 3, Name: "User3", Email: "user3@example.com", Age: 35},
		}

		// 执行测试
		affected, err := table.BulkUpsert(ctx, []string{"id"}, users)
		assert.NoError(t, err, "BulkUpsert with conflict key should succeed")
		assert.Equal(t, int64(3), affected, "Should affect 3 rows")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("upsert do nothing", func(t *testing.T) {
		// 设置期望 - ON CONFLICT DO NOTHING
		mock.ExpectExec("INSERT INTO users .* ON CONFLICT \\(id, email\\) DO").
			WillReturnResult(sqlmock.NewResult(0, 1))

		// 准备测试数据
		type User struct {
			ID    int    `db:"id"`
			Name  string `db:"name"`
			Email string `db:"email"`
			Age   int    `db:"age"`
		}

		users := []interface{}{
			User{ID: 1, Name: "User1", Email: "user1@example.com", Age: 25},
		}

		// 执行测试
		affected, err := table.BulkUpsert(ctx, []string{"id", "email"}, users)
		assert.NoError(t, err, "BulkUpsert with DO NOTHING should succeed")
		assert.Equal(t, int64(1), affected, "Should affect 1 row")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("upsert error", func(t *testing.T) {
		// 设置期望
		mock.ExpectExec("INSERT INTO users").
			WillReturnError(errors.New("batch insert failed"))

		// 准备测试数据
		type User struct {
			ID    int    `db:"id"`
			Name  string `db:"name"`
			Email string `db:"email"`
			Age   int    `db:"age"`
		}

		users := []interface{}{
			User{Name: "User1", Email: "user1@example.com", Age: 25},
			User{Name: "User2", Email: "invalid-email", Age: 30}, // 假设这会导致错误
		}

		// 执行测试
		affected, err := table.BulkUpsert(ctx, []string{}, users)
		assert.Error(t, err, "BulkUpsert should return error")
		assert.Equal(t, int64(0), affected, "Affected rows should be 0 on error")
		assert.Contains(t, err.Error(), "batch insert failed")
		assert.NoError(t, mock.ExpectationsWereMet(), "All expectations should be met")
	})

	t.Run("invalid data type", func(t *testing.T) {
		// 准备测试数据 - 非结构体类型
		invalidData := []interface{}{
			"string1",
			"string2",
		}

		// 执行测试
		affected, err := table.BulkUpsert(ctx, []string{}, invalidData)
		assert.Error(t, err, "BulkUpsert should return error for invalid data type")
		assert.Equal(t, int64(0), affected, "Affected rows should be 0 on error")
		assert.Contains(t, err.Error(), "invalid table structure")
	})
}

// 测试缓存机制的辅助函数
func TestGetStructFieldsWithCache(t *testing.T) {
	// 准备测试数据
	type User struct {
		ID       int    `db:"id"`
		Name     string `db:"name"`
		Email    string `db:"email"`
		Age      int    `db:"age"`
		Ignored  string // 无标签字段
		Excluded string `db:"-"` // 排除字段
	}

	user := User{}

	t.Run("first call without cache", func(t *testing.T) {
		// 清除缓存
		structFieldsCache = sync.Map{}

		// 执行测试
		fields, err := getStructFieldsWithCache(user)
		assert.NoError(t, err, "getStructFieldsWithCache should succeed")
		assert.Equal(t, 4, len(fields), "Should extract 4 fields")
		assert.Contains(t, fields, "id", "Fields should contain id")
		assert.Contains(t, fields, "name", "Fields should contain name")
		assert.Contains(t, fields, "email", "Fields should contain email")
		assert.Contains(t, fields, "age", "Fields should contain age")
		assert.NotContains(t, fields, "ignored", "Fields should not contain field without db tag")
		assert.NotContains(t, fields, "excluded", "Fields should not contain excluded field")
	})

	t.Run("second call with cache", func(t *testing.T) {
		// 确保缓存已经被填充
		// 执行测试
		fields, err := getStructFieldsWithCache(user)
		assert.NoError(t, err, "getStructFieldsWithCache should succeed")
		assert.Equal(t, 4, len(fields), "Should extract 4 fields with cache")
	})

	t.Run("invalid type", func(t *testing.T) {
		// 使用非结构体类型
		invalidData := "not a struct"

		// 执行测试
		fields, err := getStructFieldsWithCache(invalidData)
		assert.Error(t, err, "getStructFieldsWithCache should fail for non-struct type")
		assert.Contains(t, err.Error(), "invalid table structure")
		assert.Nil(t, fields, "Fields should be nil for invalid type")
	})
}

// 测试缓存值的提取
func TestExtractValuesWithCache(t *testing.T) {
	// 准备测试数据
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
		Age   int    `db:"age"`
	}

	user := User{
		ID:    1,
		Name:  "John Doe",
		Email: "john@example.com",
		Age:   30,
	}

	fields := []string{"id", "name", "email", "age"}

	t.Run("first call without cache", func(t *testing.T) {
		// 清除缓存
		fieldValuesCache = sync.Map{}

		// 执行测试
		values, err := extractValuesWithCache(user, fields)
		assert.NoError(t, err, "extractValuesWithCache should succeed")
		assert.Equal(t, 4, len(values), "Should extract 4 values")
		assert.Equal(t, 1, values[0], "First value should be ID")
		assert.Equal(t, "John Doe", values[1], "Second value should be Name")
		assert.Equal(t, "john@example.com", values[2], "Third value should be Email")
		assert.Equal(t, 30, values[3], "Fourth value should be Age")
	})

	t.Run("second call with cache", func(t *testing.T) {
		// 确保缓存已经被填充
		// 执行测试
		values, err := extractValuesWithCache(user, fields)
		assert.NoError(t, err, "extractValuesWithCache should succeed with cache")
		assert.Equal(t, 4, len(values), "Should extract 4 values with cache")
	})

	t.Run("extract with missing field", func(t *testing.T) {
		// 准备测试数据
		fieldsWithMissing := []string{"id", "name", "email", "age", "nonexistent"}

		// 执行测试
		values, err := extractValuesWithCache(user, fieldsWithMissing)
		assert.NoError(t, err, "extractValuesWithCache should handle missing fields")
		assert.Equal(t, 5, len(values), "Should extract 5 values")
		assert.Nil(t, values[4], "Value for nonexistent field should be nil")
	})

	t.Run("invalid type", func(t *testing.T) {
		// 使用非结构体类型
		invalidData := "not a struct"

		// 执行测试
		values, err := extractValuesWithCache(invalidData, fields)
		assert.Error(t, err, "extractValuesWithCache should fail for non-struct type")
		assert.Contains(t, err.Error(), "invalid table structure")
		assert.Nil(t, values, "Values should be nil for invalid type")
	})
}

// 测试辅助函数
func TestHelperFunctions(t *testing.T) {
	t.Run("buildPlaceholderTemplate", func(t *testing.T) {
		tests := []struct {
			fieldCount int
			expected   string
		}{
			{1, "($1)"},
			{2, "($1, $2)"},
			{3, "($1, $2, $3)"},
			{5, "($1, $2, $3, $4, $5)"},
		}

		for _, tt := range tests {
			t.Run(fmt.Sprintf("field count %d", tt.fieldCount), func(t *testing.T) {
				result := buildPlaceholderTemplate(tt.fieldCount)
				assert.Equal(t, tt.expected, result, "Should build correct placeholder template")
			})
		}
	})

	t.Run("buildUpdateClauses", func(t *testing.T) {
		tests := []struct {
			name        string
			fields      []string
			conflictKey []string
			expected    []string
		}{
			{
				name:        "no conflict keys",
				fields:      []string{"id", "name", "email"},
				conflictKey: []string{},
				expected:    []string{"id = EXCLUDED.id", "name = EXCLUDED.name", "email = EXCLUDED.email"},
			},
			{
				name:        "with single conflict key",
				fields:      []string{"id", "name", "email"},
				conflictKey: []string{"id"},
				expected:    []string{"name = EXCLUDED.name", "email = EXCLUDED.email"},
			},
			{
				name:        "with multiple conflict keys",
				fields:      []string{"id", "name", "email", "age"},
				conflictKey: []string{"id", "email"},
				expected:    []string{"name = EXCLUDED.name", "age = EXCLUDED.age"},
			},
			{
				name:        "all fields are conflict keys",
				fields:      []string{"id", "name"},
				conflictKey: []string{"id", "name"},
				expected:    []string{},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := buildUpdateClauses(tt.fields, tt.conflictKey)
				assert.Equal(t, tt.expected, result, "Should build correct update clauses")
			})
		}
	})

	t.Run("contains", func(t *testing.T) {
		tests := []struct {
			name     string
			slice    []string
			item     string
			expected bool
		}{
			{
				name:     "item exists",
				slice:    []string{"apple", "banana", "cherry"},
				item:     "banana",
				expected: true,
			},
			{
				name:     "item doesn't exist",
				slice:    []string{"apple", "banana", "cherry"},
				item:     "grape",
				expected: false,
			},
			{
				name:     "empty slice",
				slice:    []string{},
				item:     "apple",
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := contains(tt.slice, tt.item)
				assert.Equal(t, tt.expected, result, "Contains should correctly check if item exists in slice")
			})
		}
	})
}

// 测试表字段的获取和值的提取
func TestGetStructFields(t *testing.T) {
	// 准备测试数据
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
		Age   int    `db:"age"`
	}

	user := User{
		ID:    1,
		Name:  "John Doe",
		Email: "john@example.com",
		Age:   30,
	}

	t.Run("getStructFields", func(t *testing.T) {
		fields := getStructFields(user)
		assert.NotEmpty(t, fields, "Should get fields from struct")
		assert.Contains(t, fields, "id", "Fields should contain id")
		assert.Contains(t, fields, "name", "Fields should contain name")
		assert.Contains(t, fields, "email", "Fields should contain email")
		assert.Contains(t, fields, "age", "Fields should contain age")
	})

	t.Run("getStructValues", func(t *testing.T) {
		values := getStructValues(user)
		assert.Equal(t, 4, len(values), "Should get 4 values from struct")

		// 检查值
		assert.Contains(t, values, 1, "Values should contain ID")
		assert.Contains(t, values, "John Doe", "Values should contain Name")
		assert.Contains(t, values, "john@example.com", "Values should contain Email")
		assert.Contains(t, values, 30, "Values should contain Age")
	})
}
