package postgresql_helper

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/songzhibin97/postgresql_helper/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupMigratorTest 创建测试Migrator所需的环境
func setupMigratorTest(t *testing.T) (types.Migrator, sqlmock.Sqlmock, func()) {
	// 创建sqlmock，使用PostgreSQL风格参数占位符并启用正则表达式匹配
	mockDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
	)
	require.NoError(t, err, "Failed to create mock database")

	// 创建sqlx包装
	sqlxDB := sqlx.NewDb(mockDB, "postgres")

	// 创建DB对象
	db := &DB{
		db:   sqlxDB,
		name: "test_db",
	}

	// 创建Migrator对象
	migrator, err := NewMigrator(db)
	require.NoError(t, err, "Failed to create migrator")

	// 清理函数
	cleanup := func() {
		sqlxDB.Close()
	}

	return migrator, mock, cleanup
}

func TestMigrator_MigrateUp(t *testing.T) {
	// 设置测试环境
	m, mock, cleanup := setupMigratorTest(t)
	defer cleanup()

	ctx := context.Background()

	// 设置测试迁移
	migration1 := types.Migration{
		Version:     20230101000001,
		Name:        "First migration",
		Description: "First test migration",
		UpFn: func(ctx context.Context, db types.DB) error {
			return nil
		},
		DownFn: func(ctx context.Context, db types.DB) error {
			return nil
		},
	}

	migration2 := types.Migration{
		Version:     20230101000002,
		Name:        "Second migration",
		Description: "Second test migration",
		UpFn: func(ctx context.Context, db types.DB) error {
			return nil
		},
		DownFn: func(ctx context.Context, db types.DB) error {
			return nil
		},
	}

	// 注册迁移
	err := m.Register(migration1)
	require.NoError(t, err)
	err = m.Register(migration2)
	require.NoError(t, err)

	// 设置mock期望

	// 1. 表存在检查
	mock.ExpectQuery(`SELECT EXISTS \( SELECT FROM information_schema\.tables WHERE table_schema = 'public' AND table_name = \$1 \)`).
		WithArgs("schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	// 2. 创建表 - 使用完整的SQL匹配
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS schema_migrations \(version BIGINT PRIMARY KEY NOT NULL,name VARCHAR\(255\) NOT NULL,description TEXT,applied_at TIMESTAMP WITH TIME ZONE NOT NULL\)`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// 3. 获取当前版本 - 检查表是否存在
	mock.ExpectQuery(`SELECT EXISTS \( SELECT FROM information_schema\.tables WHERE table_schema = 'public' AND table_name = \$1 \)`).
		WithArgs("schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	// 4. 获取当前版本
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(version\), 0\) FROM schema_migrations`).
		WillReturnRows(sqlmock.NewRows([]string{"coalesce"}).AddRow(0))

	// 5. 获取已应用的版本
	mock.ExpectQuery(`SELECT version FROM schema_migrations`).
		WillReturnRows(sqlmock.NewRows([]string{"version"}))

	// 6. 第一个迁移的事务
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO schema_migrations \(version, name, description\) VALUES \(\$1, \$2, \$3\)`).
		WithArgs(20230101000001, "First migration", "First test migration").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// 7. 第二个迁移的事务
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO schema_migrations \(version, name, description\) VALUES \(\$1, \$2, \$3\)`).
		WithArgs(20230101000002, "Second migration", "Second test migration").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// 测试迁移向上
	result, err := m.MigrateUp(ctx)
	if err != nil {
		t.Logf("Migration error: %v", err)
	}

	assert.NoError(t, err)
	assert.NotNil(t, result)
	if result != nil {
		assert.Equal(t, int64(0), result.StartVersion)
		assert.Equal(t, int64(20230101000002), result.CurrentVersion)
		assert.Equal(t, int64(20230101000002), result.EndVersion)
		assert.Len(t, result.AppliedMigrations, 2)
	}

	// 验证所有期望都已满足
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled mock expectations: %s", err)
	}
}

// 测试创建迁移表功能
func TestMigrator_CreateMigrationsTable(t *testing.T) {
	// 设置测试环境
	m, mock, cleanup := setupMigratorTest(t)
	defer cleanup()

	ctx := context.Background()

	// 表不存在情况
	mock.ExpectQuery(`SELECT EXISTS \( SELECT FROM information_schema\.tables WHERE table_schema = 'public' AND table_name = \$1 \)`).
		WithArgs("schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	// 创建表 - 需要匹配确切的SQL，使用ExpectExec而不是ExpectQuery
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS schema_migrations \(version BIGINT PRIMARY KEY NOT NULL,name VARCHAR\(255\) NOT NULL,description TEXT,applied_at TIMESTAMP WITH TIME ZONE NOT NULL\)`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := m.CreateMigrationsTable(ctx)
	assert.NoError(t, err)

	// 表已存在情况
	mock.ExpectQuery(`SELECT EXISTS \( SELECT FROM information_schema\.tables WHERE table_schema = 'public' AND table_name = \$1 \)`).
		WithArgs("schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	err = m.CreateMigrationsTable(ctx)
	assert.NoError(t, err)

	// 验证所有期望都已满足
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled mock expectations: %s", err)
	}
}

// 测试获取当前版本功能
func TestMigrator_GetCurrentVersion(t *testing.T) {
	// 设置测试环境
	m, mock, cleanup := setupMigratorTest(t)
	defer cleanup()

	ctx := context.Background()

	// 表存在检查
	mock.ExpectQuery(`SELECT EXISTS \( SELECT FROM information_schema\.tables WHERE table_schema = 'public' AND table_name = \$1 \)`).
		WithArgs("schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	// 获取当前版本
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(version\), 0\) FROM schema_migrations`).
		WillReturnRows(sqlmock.NewRows([]string{"coalesce"}).AddRow(20230101000001))

	version, err := m.GetCurrentVersion(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(20230101000001), version)

	// 验证所有期望都已满足
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled mock expectations: %s", err)
	}
}

// 测试辅助函数
func TestMigrationHelperFunctions(t *testing.T) {
	// 测试NewMigration
	version := int64(20230101000001)
	name := "Test Migration"
	description := "Test migration description"
	upFn := func(ctx context.Context, db types.DB) error { return nil }
	downFn := func(ctx context.Context, db types.DB) error { return nil }

	migration := NewMigration(version, name, description, upFn, downFn)
	assert.Equal(t, version, migration.Version)
	assert.Equal(t, name, migration.Name)
	assert.Equal(t, description, migration.Description)
	assert.NotNil(t, migration.UpFn)
	assert.NotNil(t, migration.DownFn)

	// 测试TimestampMigration
	timestampMigration := TimestampMigration(name, description, upFn, downFn)
	assert.NotZero(t, timestampMigration.Version)
	assert.Equal(t, name, timestampMigration.Name)
	assert.Equal(t, description, timestampMigration.Description)

	// 测试DateTimeMigration
	dateTimeMigration := DateTimeMigration(name, description, upFn, downFn)
	assert.NotZero(t, dateTimeMigration.Version)
	assert.Equal(t, name, dateTimeMigration.Name)
	assert.Equal(t, description, dateTimeMigration.Description)

	// 测试SQLMigration
	upSQL := "CREATE TABLE test (id SERIAL PRIMARY KEY);"
	downSQL := "DROP TABLE test;"

	sqlMigration := SQLMigration(version, name, description, upSQL, downSQL)
	assert.Equal(t, version, sqlMigration.Version)
	assert.Equal(t, name, sqlMigration.Name)
	assert.Equal(t, description, sqlMigration.Description)
	assert.NotNil(t, sqlMigration.UpFn)
	assert.NotNil(t, sqlMigration.DownFn)
}
