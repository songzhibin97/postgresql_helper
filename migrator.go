package postgresql_helper

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/songzhibin97/postgresql_helper/types"
)

// migrator 迁移管理器实现
type migrator struct {
	db         *DB
	migrations []types.Migration
	tableName  string
}

// NewMigrator 创建新的迁移管理器
func NewMigrator(db *DB, opts ...MigratorOption) (types.Migrator, error) {
	m := &migrator{
		db:         db,
		migrations: []types.Migration{},
		tableName:  "schema_migrations", // 默认表名
	}

	// 应用选项
	for _, opt := range opts {
		opt(m)
	}

	return m, nil
}

// MigratorOption 迁移管理器配置选项
type MigratorOption func(*migrator)

// WithMigrationsTable 自定义迁移表名
func WithMigrationsTable(tableName string) MigratorOption {
	return func(m *migrator) {
		m.tableName = tableName
	}
}

// Register 注册新迁移
func (m *migrator) Register(migration types.Migration) error {
	// 检查版本号重复
	for _, existing := range m.migrations {
		if existing.Version == migration.Version {
			return fmt.Errorf("migration with version %d already exists: %s",
				migration.Version, existing.Name)
		}
	}

	m.migrations = append(m.migrations, migration)

	// 按版本排序
	sort.Slice(m.migrations, func(i, j int) bool {
		return m.migrations[i].Version < m.migrations[j].Version
	})

	return nil
}

// CreateMigrationsTable 创建迁移表
func (m *migrator) CreateMigrationsTable(ctx context.Context) error {
	schema := m.db.Schema()

	// 检查表是否存在
	exists, err := schema.TableExists(ctx, m.tableName)
	if err != nil {
		return fmt.Errorf("failed to check migrations table: %w", err)
	}

	if exists {
		return nil // 表已存在，无需创建
	}

	// 创建迁移表
	tableSchema := types.TableSchema{
		Name: m.tableName,
		Columns: []types.ColumnDefinition{
			{
				Name:       "version",
				Type:       "BIGINT",
				PrimaryKey: true,
				Nullable:   false,
			},
			{
				Name:     "name",
				Type:     "VARCHAR(255)",
				Nullable: false,
			},
			{
				Name:     "description",
				Type:     "TEXT",
				Nullable: true,
			},
			{
				Name:     "applied_at",
				Type:     "TIMESTAMP WITH TIME ZONE",
				Nullable: false,
				Default:  "NOW()",
			},
		},
		IfNotExists: true,
	}

	if err := schema.CreateTable(ctx, tableSchema); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	return nil
}

// GetCurrentVersion 获取当前数据库版本
func (m *migrator) GetCurrentVersion(ctx context.Context) (int64, error) {
	// 确保迁移表存在
	if err := m.CreateMigrationsTable(ctx); err != nil {
		return 0, err
	}

	// 查询最高版本
	query := fmt.Sprintf("SELECT COALESCE(MAX(version), 0) FROM %s", m.tableName)
	var version int64

	if err := m.db.db.GetContext(ctx, &version, query); err != nil {
		return 0, fmt.Errorf("failed to get current version: %w", err)
	}

	return version, nil
}

// GetAppliedMigrations 获取已应用的迁移
func (m *migrator) GetAppliedMigrations(ctx context.Context) ([]types.Migration, error) {
	// 确保迁移表存在
	if err := m.CreateMigrationsTable(ctx); err != nil {
		return nil, err
	}

	// 查询所有已应用的迁移
	query := fmt.Sprintf(
		"SELECT version, name, description, applied_at FROM %s ORDER BY version",
		m.tableName)

	rows, err := m.db.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query migrations: %w", err)
	}
	defer rows.Close()

	var migrations []types.Migration
	for rows.Next() {
		var migration types.Migration
		var appliedAt time.Time

		if err := rows.Scan(
			&migration.Version,
			&migration.Name,
			&migration.Description,
			&appliedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan migration: %w", err)
		}

		migration.AppliedAt = &appliedAt
		migrations = append(migrations, migration)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migrations: %w", err)
	}

	return migrations, nil
}

// MigrateUp 应用所有未应用的迁移
func (m *migrator) MigrateUp(ctx context.Context) (*types.MigrationResult, error) {
	return m.MigrateUpTo(ctx, math.MaxInt64)
}

// MigrateUpTo 迁移到指定版本
func (m *migrator) MigrateUpTo(ctx context.Context, targetVersion int64) (*types.MigrationResult, error) {
	startTime := time.Now()

	// 确保迁移表存在
	if err := m.CreateMigrationsTable(ctx); err != nil {
		return nil, err
	}

	// 获取当前版本
	currentVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return nil, err
	}

	// 获取已应用的迁移版本集合
	appliedVersions, err := m.getAppliedVersions(ctx)
	if err != nil {
		return nil, err
	}

	result := &types.MigrationResult{
		StartVersion:      currentVersion,
		AppliedMigrations: []types.Migration{},
	}

	// 筛选需要应用的迁移
	var migrationsToApply []types.Migration
	for _, migration := range m.migrations {
		// 只应用比当前版本新且未超过目标版本的迁移
		if migration.Version > currentVersion && migration.Version <= targetVersion {
			// 确保迁移未应用过（防止重复应用）
			if _, applied := appliedVersions[migration.Version]; !applied {
				migrationsToApply = append(migrationsToApply, migration)
			}
		}
	}

	// 按版本排序
	sort.Slice(migrationsToApply, func(i, j int) bool {
		return migrationsToApply[i].Version < migrationsToApply[j].Version
	})

	// 没有迁移需要应用
	if len(migrationsToApply) == 0 {
		result.CurrentVersion = currentVersion
		result.EndVersion = currentVersion
		result.ExecutionTime = time.Since(startTime)
		return result, nil
	}

	// 应用迁移
	for _, migration := range migrationsToApply {
		// 跳过没有Up函数的迁移
		if migration.UpFn == nil {
			continue
		}

		// 在事务中执行迁移
		err := m.db.InTx(ctx, func(ctx context.Context) error {
			// 执行迁移
			if err := migration.UpFn(ctx, m.db); err != nil {
				return fmt.Errorf("migration %d (%s) failed: %w",
					migration.Version, migration.Name, err)
			}

			// 记录迁移
			query := fmt.Sprintf(
				"INSERT INTO %s (version, name, description) VALUES ($1, $2, $3)",
				m.tableName)

			_, err = m.db.db.ExecContext(ctx, query,
				migration.Version, migration.Name, migration.Description)

			if err != nil {
				return fmt.Errorf("failed to record migration %d: %w",
					migration.Version, err)
			}

			return nil
		})

		if err != nil {
			// 迁移失败
			result.Error = err
			result.CurrentVersion = currentVersion
			result.EndVersion = currentVersion
			result.ExecutionTime = time.Since(startTime)
			return result, err
		}

		// 记录已应用的迁移
		now := time.Now()
		migration.AppliedAt = &now
		result.AppliedMigrations = append(result.AppliedMigrations, migration)
		currentVersion = migration.Version
	}

	result.CurrentVersion = currentVersion
	result.EndVersion = currentVersion
	result.ExecutionTime = time.Since(startTime)

	return result, nil
}

// MigrateDown 回滚指定数量的迁移
func (m *migrator) MigrateDown(ctx context.Context, steps int) (*types.MigrationResult, error) {
	if steps <= 0 {
		return nil, fmt.Errorf("invalid steps: %d, must be positive", steps)
	}

	// 获取已应用的迁移
	appliedMigrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	if len(appliedMigrations) == 0 {
		return &types.MigrationResult{
			StartVersion:      0,
			CurrentVersion:    0,
			EndVersion:        0,
			AppliedMigrations: []types.Migration{},
			ExecutionTime:     0,
		}, nil
	}

	// 计算目标版本
	targetIndex := len(appliedMigrations) - steps
	if targetIndex < 0 {
		targetIndex = 0
	}

	var targetVersion int64 = 0
	if targetIndex > 0 {
		targetVersion = appliedMigrations[targetIndex-1].Version
	}

	return m.MigrateDownTo(ctx, targetVersion)
}

// MigrateDownTo 回滚到指定版本
func (m *migrator) MigrateDownTo(ctx context.Context, targetVersion int64) (*types.MigrationResult, error) {
	startTime := time.Now()

	// 确保迁移表存在
	if err := m.CreateMigrationsTable(ctx); err != nil {
		return nil, err
	}

	// 获取当前版本
	currentVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return nil, err
	}

	// 获取已应用的迁移
	appliedMigrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	result := &types.MigrationResult{
		StartVersion:      currentVersion,
		AppliedMigrations: []types.Migration{},
	}

	// 没有可回滚的迁移
	if len(appliedMigrations) == 0 || currentVersion <= targetVersion {
		result.CurrentVersion = currentVersion
		result.EndVersion = currentVersion
		result.ExecutionTime = time.Since(startTime)
		return result, nil
	}

	// 按版本倒序排列
	sort.Slice(appliedMigrations, func(i, j int) bool {
		return appliedMigrations[i].Version > appliedMigrations[j].Version
	})

	// 找到需要回滚的迁移
	var migrationsToRollback []types.Migration
	for _, appliedMigration := range appliedMigrations {
		if appliedMigration.Version > targetVersion {
			// 在已注册的迁移中查找对应的迁移定义
			for _, migration := range m.migrations {
				if migration.Version == appliedMigration.Version {
					migrationsToRollback = append(migrationsToRollback, migration)
					break
				}
			}
		}
	}

	// 按版本倒序排列（先回滚最新的）
	sort.Slice(migrationsToRollback, func(i, j int) bool {
		return migrationsToRollback[i].Version > migrationsToRollback[j].Version
	})

	// 回滚迁移
	for _, migration := range migrationsToRollback {
		// 跳过没有Down函数的迁移
		if migration.DownFn == nil {
			return nil, fmt.Errorf("migration %d (%s) has no down function",
				migration.Version, migration.Name)
		}

		// 在事务中执行回滚
		err := m.db.InTx(ctx, func(ctx context.Context) error {
			// 执行回滚
			if err := migration.DownFn(ctx, m.db); err != nil {
				return fmt.Errorf("rollback migration %d (%s) failed: %w",
					migration.Version, migration.Name, err)
			}

			// 删除迁移记录
			query := fmt.Sprintf("DELETE FROM %s WHERE version = $1", m.tableName)
			_, err = m.db.db.ExecContext(ctx, query, migration.Version)

			if err != nil {
				return fmt.Errorf("failed to delete migration record %d: %w",
					migration.Version, err)
			}

			return nil
		})

		if err != nil {
			// 回滚失败
			result.Error = err
			result.CurrentVersion = currentVersion
			result.EndVersion = currentVersion
			result.ExecutionTime = time.Since(startTime)
			return result, err
		}

		// 记录已回滚的迁移
		result.AppliedMigrations = append(result.AppliedMigrations, migration)
		currentVersion = migration.Version - 1
	}

	// 获取最新版本
	newVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return nil, err
	}

	result.CurrentVersion = newVersion
	result.EndVersion = newVersion
	result.ExecutionTime = time.Since(startTime)

	return result, nil
}

// 获取已应用的迁移版本集合
func (m *migrator) getAppliedVersions(ctx context.Context) (map[int64]struct{}, error) {
	query := fmt.Sprintf("SELECT version FROM %s", m.tableName)
	rows, err := m.db.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query applied versions: %w", err)
	}
	defer rows.Close()

	result := make(map[int64]struct{})
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("failed to scan version: %w", err)
		}
		result[version] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating versions: %w", err)
	}

	return result, nil
}

// NewMigration 创建新的迁移
func NewMigration(version int64, name string, description string, up types.MigrateFn, down types.MigrateFn) types.Migration {
	return types.Migration{
		Version:     version,
		Name:        name,
		Description: description,
		UpFn:        up,
		DownFn:      down,
	}
}

// TimestampMigration 使用时间戳创建迁移
func TimestampMigration(name string, description string, up types.MigrateFn, down types.MigrateFn) types.Migration {
	version := time.Now().Unix()
	return NewMigration(version, name, description, up, down)
}

// DateTimeMigration 使用yyyyMMddHHmmss格式创建迁移
func DateTimeMigration(name string, description string, up types.MigrateFn, down types.MigrateFn) types.Migration {
	now := time.Now()
	version, _ := strconv.ParseInt(now.Format("20060102150405"), 10, 64)
	return NewMigration(version, name, description, up, down)
}

// SQLMigration 从SQL字符串创建迁移
func SQLMigration(version int64, name string, description string, upSQL string, downSQL string) types.Migration {
	var upFn types.MigrateFn = func(ctx context.Context, db types.DB) error {
		if upSQL == "" {
			return nil
		}
		_, err := db.Query(ctx, upSQL)
		return err
	}

	var downFn types.MigrateFn = func(ctx context.Context, db types.DB) error {
		if downSQL == "" {
			return nil
		}
		_, err := db.Query(ctx, downSQL)
		return err
	}

	return NewMigration(version, name, description, upFn, downFn)
}

// FileMigration 从SQL文件创建迁移
func FileMigration(version int64, name string, description string, upFile string, downFile string) (types.Migration, error) {
	upSQL, err := os.ReadFile(upFile)
	if err != nil && !os.IsNotExist(err) {
		return types.Migration{}, fmt.Errorf("failed to read up migration file: %w", err)
	}

	downSQL, err := os.ReadFile(downFile)
	if err != nil && !os.IsNotExist(err) {
		return types.Migration{}, fmt.Errorf("failed to read down migration file: %w", err)
	}

	return SQLMigration(
		version,
		name,
		description,
		string(upSQL),
		string(downSQL),
	), nil
}
