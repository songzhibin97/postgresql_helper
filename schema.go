package postgresql_helper

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/songzhibin97/postgresql_helper/types"
)

var _ types.Schema = (*Schema)(nil)

type Schema struct {
	*DB
}

func (s Schema) CreateTable(ctx context.Context, schema types.TableSchema) error {
	return s.withMetrics(ctx, schema.Name, createOper, func(ctx context.Context) error {
		var columns []string
		for _, col := range schema.Columns {
			columnDef := fmt.Sprintf("%s %s", col.Name, col.Type)

			if col.PrimaryKey {
				columnDef += " PRIMARY KEY"
			}
			if !col.Nullable {
				columnDef += " NOT NULL"
			}
			if col.Unique {
				columnDef += " UNIQUE"
			}
			if col.Check != "" {
				columnDef += " CHECK (" + col.Check + ")"
			}
			if col.ForeignKey != nil {
				fk := col.ForeignKey
				columnDef += fmt.Sprintf(" REFERENCES %s(%s)", fk.ReferenceTable, fk.ReferenceColumn)
				if fk.OnDelete != "" {
					columnDef += " ON DELETE " + fk.OnDelete
				}
				if fk.OnUpdate != "" {
					columnDef += " ON UPDATE " + fk.OnUpdate
				}
			}
			columns = append(columns, columnDef)
		}

		createSQL := "CREATE TABLE"
		if schema.IfNotExists {
			createSQL += " IF NOT EXISTS"
		}
		createSQL += fmt.Sprintf(" %s (%s)", schema.Name, strings.Join(columns, ","))

		_, err := s.db.ExecContext(ctx, createSQL)
		return s.wrapError(err, "create table "+schema.Name)
	})
}

func (s Schema) AlterTable(ctx context.Context, tableName string, alterations []string) error {
	return s.withMetrics(ctx, tableName, alertOper, func(ctx context.Context) error {
		if len(alterations) == 0 {
			return s.wrapError(
				fmt.Errorf("%w: no alterations provided", types.ErrInvalidStructure),
				"alter table",
			)
		}

		alterSQL := fmt.Sprintf("ALTER TABLE %s %s", tableName, strings.Join(alterations, ","))
		_, err := s.db.ExecContext(ctx, alterSQL)
		return s.wrapError(err, "alter table "+tableName)
	})
}

func (s Schema) DropTable(ctx context.Context, tableName string, cascade bool) error {
	return s.withMetrics(ctx, tableName, deleteOper, func(ctx context.Context) error {
		query := "DROP TABLE " + tableName
		if cascade {
			query += " CASCADE"
		}
		_, err := s.db.ExecContext(ctx, query)
		return s.wrapError(err, "drop table "+tableName)
	})
}

func (s Schema) TableExists(ctx context.Context, tableName string) (bool, error) {
	var exists bool
	err := s.withMetrics(ctx, tableName, queryOper, func(ctx context.Context) error {
		query := `SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = $1
    )`
		err := s.db.GetContext(ctx, &exists, query, tableName)
		return s.wrapError(err, "check table exists")
	})
	return exists, err
}

func (s Schema) GetTableSchema(ctx context.Context, tableName string) (*types.TableSchema, error) {
	var schema types.TableSchema
	err := s.withMetrics(ctx, tableName, queryOper, func(ctx context.Context) error {
		// 1. 验证表是否存在
		exists, err := s.TableExists(ctx, tableName)
		if err != nil {
			return s.wrapError(err, "check table exists")
		}
		if !exists {
			return types.ErrRecordNotFound
		}

		// 2. 获取基础列信息
		columns, err := s.getColumns(ctx, tableName)
		if err != nil {
			return s.wrapError(err, "get columns")
		}

		// 3. 获取主键信息
		pkColumns, err := s.getPrimaryKeys(ctx, tableName)
		if err != nil {
			return s.wrapError(err, "get primary keys")
		}

		// 4. 获取索引信息
		indexes, err := s.getIndexes(ctx, tableName)
		if err != nil {
			return s.wrapError(err, "get indexes")
		}

		// 5. 获取外键约束
		fkConstraints, err := s.getForeignKeys(ctx, tableName)
		if err != nil {
			return s.wrapError(err, "get foreign keys")
		}

		// 6. 获取检查约束
		checkConstraints, err := s.getCheckConstraints(ctx, tableName)
		if err != nil {
			return s.wrapError(err, "get check constraints")
		}

		// 7. 合并信息到列定义
		for i := range columns {
			col := &columns[i]

			// 标记主键
			if _, ok := pkColumns[col.Name]; ok {
				col.PrimaryKey = true
			}

			// 处理索引
			if indexes, ok := indexes[col.Name]; ok {
				col.Index = len(indexes) > 0
				for _, idx := range indexes {
					if idx.Unique {
						col.Unique = true
					}
				}
			}

			// 处理外键
			if fk, ok := fkConstraints[col.Name]; ok {
				col.ForeignKey = fk
			}

			// 处理检查约束
			if check, ok := checkConstraints[col.Name]; ok {
				col.Check = check
			}
		}
		schema.Name = tableName
		schema.Columns = columns
		return nil
	})

	return &schema, err
}

// 列基础信息查询
func (s Schema) getColumns(ctx context.Context, tableName string) ([]types.ColumnDefinition, error) {
	query := `
		SELECT 
			column_name,
			udt_name as data_type,
			is_nullable,
			column_default
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position`

	var columns []struct {
		Name     string         `db:"column_name"`
		Type     string         `db:"data_type"`
		Nullable string         `db:"is_nullable"`
		Default  sql.NullString `db:"column_default"`
	}

	if err := s.db.SelectContext(ctx, &columns, query, tableName); err != nil {
		return nil, fmt.Errorf("get columns failed: %w", err)
	}

	result := make([]types.ColumnDefinition, 0, len(columns))
	for _, c := range columns {
		col := types.ColumnDefinition{
			Name:     c.Name,
			Type:     strings.ToUpper(c.Type),
			Nullable: c.Nullable == "YES",
		}

		if c.Default.Valid {
			col.Default = c.Default.String
		}

		// 处理特殊类型映射
		switch col.Type {
		case "text":
			col.Type = "TEXT"
		case "numeric":
			col.Type = "DECIMAL"
		case "jsonb":
			col.Type = "JSONB"
		case "timestamptz":
			col.Type = "TIMESTAMP WITH TIME ZONE"
		case "varchar", "bpchar":
			col.Type = "VARCHAR"
		case "int4":
			col.Type = "INTEGER"
		case "int8":
			col.Type = "BIGINT"
		case "bool":
			col.Type = "BOOLEAN"
		case "timestamp":
			col.Type = "TIMESTAMP"
		}

		result = append(result, col)
	}
	return result, nil
}

// 主键查询
func (s Schema) getPrimaryKeys(ctx context.Context, tableName string) (map[string]struct{}, error) {
	query := `
		SELECT 
			pg_attribute.attname AS column_name
		FROM pg_index
		JOIN pg_attribute 
			ON pg_attribute.attrelid = pg_index.indrelid
			AND pg_attribute.attnum = ANY(pg_index.indkey)
		WHERE 
			pg_index.indrelid = $1::regclass
			AND pg_index.indisprimary`

	var columns []struct {
		Name string `db:"column_name"`
	}
	if err := s.db.SelectContext(ctx, &columns, query, tableName); err != nil {
		return nil, fmt.Errorf("get primary keys failed: %w", err)
	}

	pkMap := make(map[string]struct{})
	for _, c := range columns {
		pkMap[c.Name] = struct{}{}
	}
	return pkMap, nil
}

// 索引查询
func (s Schema) getIndexes(ctx context.Context, tableName string) (map[string][]indexInfo, error) {
	query := `
		SELECT
			indexname,
			indexdef,
			indisunique
		FROM pg_indexes
		WHERE tablename = $1`

	var indexes []struct {
		Name     string `db:"indexname"`
		Def      string `db:"indexdef"`
		IsUnique bool   `db:"indisunique"`
	}

	if err := s.db.SelectContext(ctx, &indexes, query, tableName); err != nil {
		return nil, fmt.Errorf("get indexes failed: %w", err)
	}

	result := make(map[string][]indexInfo)
	for _, idx := range indexes {
		// 解析索引涉及的列
		cols := extractColumnsFromIndexDef(idx.Def)
		for _, col := range cols {
			result[col] = append(result[col], indexInfo{
				Name:   idx.Name,
				Unique: idx.IsUnique,
			})
		}
	}
	return result, nil
}

// 外键查询
func (s Schema) getForeignKeys(ctx context.Context, tableName string) (map[string]*types.ForeignKey, error) {
	query := `
		SELECT
			kc.column_name,
			cc.table_name AS ref_table,
			cc.column_name AS ref_column,
			rc.delete_rule,
			rc.update_rule
		FROM information_schema.key_column_usage kc
		JOIN information_schema.referential_constraints rc
			ON kc.constraint_name = rc.constraint_name
		JOIN information_schema.constraint_column_usage cc
			ON rc.unique_constraint_name = cc.constraint_name
		WHERE kc.table_name = $1`

	var fks []struct {
		Column    string `db:"column_name"`
		RefTable  string `db:"ref_table"`
		RefColumn string `db:"ref_column"`
		OnDelete  string `db:"delete_rule"`
		OnUpdate  string `db:"update_rule"`
	}

	if err := s.db.SelectContext(ctx, &fks, query, tableName); err != nil {
		return nil, fmt.Errorf("get foreign keys failed: %w", err)
	}

	result := make(map[string]*types.ForeignKey)
	for _, fk := range fks {
		result[fk.Column] = &types.ForeignKey{
			ReferenceTable:  fk.RefTable,
			ReferenceColumn: fk.RefColumn,
			OnDelete:        normalizeAction(fk.OnDelete),
			OnUpdate:        normalizeAction(fk.OnUpdate),
		}
	}
	return result, nil
}

// 辅助函数：规范化外键动作
func normalizeAction(action string) string {
	switch action {
	case "NO ACTION":
		return "RESTRICT"
	default:
		return action
	}
}

// 检查约束查询
func (s Schema) getCheckConstraints(ctx context.Context, tableName string) (map[string]string, error) {
	query := `
		SELECT
			pgc.conname AS constraint_name,
			pg_get_constraintdef(pgc.oid) AS check_clause
		FROM pg_constraint pgc
		JOIN pg_class cls
			ON pgc.conrelid = cls.oid
		WHERE 
			cls.relname = $1
			AND pgc.contype = 'c'`

	var checks []struct {
		Name        string `db:"constraint_name"`
		CheckClause string `db:"check_clause"`
	}

	if err := s.db.SelectContext(ctx, &checks, query, tableName); err != nil {
		return nil, fmt.Errorf("get check constraints failed: %w", err)
	}

	result := make(map[string]string)
	for _, c := range checks {
		// 解析涉及列（简化处理）
		if cols := parseColumnsFromCheck(c.CheckClause); len(cols) > 0 {
			for _, col := range cols {
				result[col] = c.CheckClause
			}
		}
	}
	return result, nil
}

// 辅助结构体和函数
type indexInfo struct {
	Name   string
	Unique bool
}

func extractColumnsFromIndexDef(def string) []string {
	// 示例索引定义: CREATE INDEX idx_name ON table (col1, col2)
	start := strings.Index(def, "(")
	end := strings.Index(def, ")")
	if start == -1 || end == -1 {
		return nil
	}

	colsPart := def[start+1 : end]
	return strings.Split(strings.ReplaceAll(colsPart, " ", ""), ",")
}

func parseColumnsFromCheck(clause string) []string {
	// 示例检查约束: CHECK (age > 0 AND name IS NOT NULL)
	var cols []string
	tokens := strings.FieldsFunc(clause, func(r rune) bool {
		return r == '(' || r == ')' || r == ' ' || r == '='
	})

	for _, token := range tokens {
		if strings.Contains(token, ".") {
			continue // 排除表名前缀
		}
		if isReservedWord(token) {
			continue
		}
		cols = append(cols, token)
	}
	return cols
}

func isReservedWord(word string) bool {
	reserved := map[string]struct{}{
		"CHECK": {}, "AND": {}, "OR": {}, "NOT": {},
		"NULL": {}, "IS": {}, ">": {}, "<": {}, "=": {},
	}
	_, ok := reserved[strings.ToUpper(word)]
	return ok
}
