package postgresql_helper

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/jmoiron/sqlx"
	"github.com/songzhibin97/postgresql_helper/types"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	_totalOperCount  *prometheus.CounterVec
	_totalErrorCount *prometheus.CounterVec
	_operDuration    *prometheus.HistogramVec
)

func init() {
	_totalOperCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pgsql_helper",
		Subsystem: "pgsql",
		Name:      "total_operate_count",
		Help:      "Total DB operation count",
	}, []string{"collection", "operation"})

	_totalErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pgsql_helper",
		Subsystem: "pgsql",
		Name:      "total_error_count",
		Help:      "Total DB operation errors",
	}, []string{"collection", "operation"})

	_operDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "pgsql_helper",
		Subsystem: "pgsql",
		Name:      "operate_duration_seconds",
		Help:      "DB operation duration in seconds",
		Buckets:   []float64{0.02, 0.04, 0.06, 0.08, 0.1, 0.3, 0.5, 0.7, 1, 5, 10, 20, 30, 60},
	}, []string{"collection", "operation"})

	prometheus.DefaultRegisterer.MustRegister(_totalOperCount, _totalErrorCount, _operDuration)
}

type oper string

var (
	ErrDuplicated          = errors.New("duplicated")
	ErrRecordNotFound      = errors.New("record not found")
	ErrInvalidStructure    = errors.New("invalid table structure")
	ErrPermissionDenied    = errors.New("permission denied")
	ErrConnectionFailed    = errors.New("database connection failed")
	ErrQueryTimeout        = errors.New("query timeout")
	ErrConstraintViolation = errors.New("constraint violation")
	ErrForeignKeyViolation = errors.New("foreign key violation")
	ErrUniqueViolation     = errors.New("unique violation")
	ErrCheckViolation      = errors.New("check constraint violation")
)

const (
	queryOper  oper = "query"
	insertOper oper = "insert"
	updateOper oper = "update"
	upsertOper oper = "upsert"
	deleteOper oper = "delete"
	columnOper oper = "column"
	indexOper  oper = "index"
	createOper oper = "create"
	alertOper  oper = "alert"
)

func collectOperCount(collection string, op oper) {
	_totalOperCount.WithLabelValues(collection, string(op)).Inc()
}

func collectErrorCount(collection string, op oper) {
	_totalErrorCount.WithLabelValues(collection, string(op)).Inc()
}

func collectOperDuration(collection string, op oper, duration time.Duration) {
	_operDuration.WithLabelValues(collection, string(op)).Observe(duration.Seconds())
}

var _ types.DB = (*DB)(nil)

type DB struct {
	db   *sqlx.DB
	name string
}

// 添加错误包装函数到 DB 结构体
func (p DB) wrapError(err error, operation string) error {
	if err == nil {
		return nil
	}

	// 解析PostgreSQL特定错误
	var pgErr *pq.Error
	ok := errors.As(err, &pgErr)
	if ok {
		switch pgErr.Code {
		case "23505": // 唯一约束冲突
			return fmt.Errorf("%w: %s - %s", ErrUniqueViolation, operation, pgErr.Detail)
		case "23503": // 外键冲突
			return fmt.Errorf("%w: %s - %s", ErrForeignKeyViolation, operation, pgErr.Detail)
		case "23514": // CHECK约束冲突
			return fmt.Errorf("%w: %s - %s", ErrCheckViolation, operation, pgErr.Detail)
		case "23000": // 完整性约束冲突
			return fmt.Errorf("%w: %s - %s", ErrConstraintViolation, operation, pgErr.Detail)
		case "42501": // 权限不足
			return fmt.Errorf("%w: %s", ErrPermissionDenied, operation)
		case "57014": // 查询取消
			return fmt.Errorf("%w: %s", ErrQueryTimeout, operation)
		}
	}

	// 处理通用SQL错误
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrRecordNotFound, operation)
	}

	return fmt.Errorf("%s: %w", operation, err)
}

func (p DB) Table(ctx context.Context, tableName string) types.Table {
	return &Table{DB: &p, name: tableName}
}

func (p DB) Schema() types.Schema {
	return &Schema{&p}
}

func (p DB) Ping(ctx context.Context) error {
	return p.withMetrics(ctx, "", queryOper, func(ctx context.Context) error {
		err := p.db.PingContext(ctx)
		return p.wrapError(err, "ping database")
	})
}

type contextTxKey struct{}

func getTxFromContext(ctx context.Context) *sqlx.Tx {
	if tx, ok := ctx.Value(contextTxKey{}).(*sqlx.Tx); ok {
		return tx
	}
	return nil
}

func (p DB) InTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if tx := getTxFromContext(ctx); tx != nil {
		return fn(ctx) // 已存在事务，直接执行（禁止嵌套）
	}

	tx, err := p.db.BeginTxx(ctx, nil)
	if err != nil {
		return p.wrapError(err, "begin transaction")
	}

	ctx = context.WithValue(ctx, contextTxKey{}, tx)

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(ctx); err != nil {
		_ = tx.Rollback()
		return err // 这里不需要包装，因为错误来自用户函数
	}

	err = tx.Commit()
	return p.wrapError(err, "commit transaction")
}

func (p DB) Close() error {
	return p.db.Close()
}

func (p DB) Query(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	var result *sqlx.Rows
	err := p.withMetrics(ctx, "", queryOper, func(ctx context.Context) error {
		var err error
		result, err = p.db.QueryxContext(ctx, query, args...)
		return p.wrapError(err, "execute query")
	})
	return result, err
}

func (p DB) withMetrics(ctx context.Context, collection string, op oper, fn func(context.Context) error) error {
	collectOperCount(collection, op)
	start := time.Now()
	defer func() { collectOperDuration(collection, op, time.Since(start)) }()

	if err := fn(ctx); err != nil {
		collectErrorCount(collection, op)
		return err
	}
	return nil
}

// DBConfig 数据库连接配置
type DBConfig struct {
	DSN             string        // 数据源名称 (PostgreSQL连接字符串)
	MaxOpenConns    int           // 最大打开连接数
	MaxIdleConns    int           // 最大空闲连接数
	ConnMaxLifetime time.Duration // 连接最大生命周期
	ConnMaxIdleTime time.Duration // 连接最大空闲时间
}

// DefaultDBConfig 返回带有合理默认值的配置
func DefaultDBConfig() DBConfig {
	return DBConfig{
		MaxOpenConns:    25,
		MaxIdleConns:    10,
		ConnMaxLifetime: 15 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
	}
}

// New 创建新的数据库连接，使用完整配置
func New(config DBConfig) (*DB, error) {
	if config.DSN == "" {
		return nil, fmt.Errorf("database DSN is required")
	}

	// 创建底层sqlx连接
	db, err := sqlx.Connect("postgres", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("connect to database failed: %w", err)
	}

	// 应用连接池配置
	if config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(config.MaxOpenConns)
	}
	if config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(config.MaxIdleConns)
	}
	if config.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(config.ConnMaxLifetime)
	}
	if config.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(config.ConnMaxIdleTime)
	}

	// 验证连接
	if err := db.Ping(); err != nil {
		db.Close() // 确保关闭失败的连接
		return nil, fmt.Errorf("ping database failed: %w", err)
	}

	return &DB{
		db:   db,
		name: extractDatabaseName(config.DSN),
	}, nil
}

// Connect 使用DSN和默认配置创建数据库连接 (简便方法)
func Connect(dsn string) (*DB, error) {
	config := DefaultDBConfig()
	config.DSN = dsn
	return New(config)
}

// 辅助函数：从DSN提取数据库名称
func extractDatabaseName(dsn string) string {
	// 尝试从DSN中提取数据库名称
	// 假设DSN格式为: postgres://user:pass@host:port/dbname?params
	parts := strings.Split(dsn, "/")
	if len(parts) < 2 {
		return "unknown"
	}

	dbPart := parts[len(parts)-1]
	return strings.Split(dbPart, "?")[0]
}

// AddConnectionStats 向指定的度量注册表添加连接统计信息
func (p *DB) AddConnectionStats(register *prometheus.Registry) {
	// 连接池统计信息
	statsCollector := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "pgsql_helper",
			Subsystem: "pgsql",
			Name:      "connections_open",
			Help:      "Number of open connections in the pool",
		},
		func() float64 {
			return float64(p.db.Stats().OpenConnections)
		},
	)

	idleCollector := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "pgsql_helper",
			Subsystem: "pgsql",
			Name:      "connections_idle",
			Help:      "Number of idle connections in the pool",
		},
		func() float64 {
			return float64(p.db.Stats().Idle)
		},
	)

	inUseCollector := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "pgsql_helper",
			Subsystem: "pgsql",
			Name:      "connections_in_use",
			Help:      "Number of connections currently in use",
		},
		func() float64 {
			stats := p.db.Stats()
			return float64(stats.OpenConnections - stats.Idle)
		},
	)

	// 注册到指定的注册表
	register.MustRegister(statsCollector, idleCollector, inUseCollector)
}

// GetStats 返回连接池的当前状态
func (p *DB) GetStats() sql.DBStats {
	return p.db.Stats()
}
