package postgresql_helper

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/songzhibin97/postgresql_helper/types"
)

var _ types.Query = (*Query)(nil)

type Query struct {
	*DB
	table  string
	config types.QueryConfig
	args   []interface{}
}

func (q Query) Select(fields ...string) types.Query {
	newQuery := q.clone()
	newQuery.config.SelectFields = fields
	return newQuery
}

func (q Query) Where(conditions string, args ...interface{}) types.Query {
	newQuery := q.clone()
	newQuery.config.WhereClause = conditions
	newQuery.args = args
	return newQuery
}

func (q Query) OrderBy(fields string) types.Query {
	newQuery := q.clone()
	newQuery.config.OrderBy = fields
	return newQuery
}

func (q Query) Limit(n int) types.Query {
	newQuery := q.clone()
	newQuery.config.Limit = n
	return newQuery
}

func (q Query) Offset(n int) types.Query {
	newQuery := q.clone()
	newQuery.config.Offset = n
	return newQuery
}

func (q Query) Join(joinClause string) types.Query {
	newQuery := q.clone()
	newQuery.config.JoinClauses = append(newQuery.config.JoinClauses, joinClause)
	return newQuery
}

func (q Query) GroupBy(fields string) types.Query {
	newQuery := q.clone()
	newQuery.config.GroupBy = fields
	return newQuery
}

func (q Query) Having(conditions string) types.Query {
	newQuery := q.clone()
	newQuery.config.Having = conditions
	return newQuery
}

func (q Query) ForUpdate() types.Query {
	newQuery := q.clone()
	newQuery.config.ForUpdate = true
	return newQuery
}

func (q Query) clone() *Query {
	return &Query{
		DB:     q.DB,
		table:  q.table,
		config: q.config,
		args:   append([]interface{}{}, q.args...),
	}
}

func (q Query) Get(ctx context.Context, dest interface{}) error {
	query := q.buildSelectQuery()
	err := q.db.GetContext(ctx, dest, query, q.args...)
	return q.wrapError(err, "execute get query")
}

func (q Query) GetAll(ctx context.Context, dest interface{}) error {
	query := q.buildSelectQuery()
	err := q.db.SelectContext(ctx, dest, query, q.args...)
	return q.wrapError(err, "execute get all query")
}

func (q Query) buildSelectQuery() string {
	var sb strings.Builder

	// SELECT
	sb.WriteString("SELECT ")
	if len(q.config.SelectFields) > 0 {
		sb.WriteString(strings.Join(q.config.SelectFields, ", "))
	} else {
		sb.WriteString("*")
	}

	// FROM
	sb.WriteString(" FROM " + q.table)

	// JOINS
	for _, join := range q.config.JoinClauses {
		sb.WriteString(" " + join)
	}

	// WHERE
	if q.config.WhereClause != "" {
		sb.WriteString(" WHERE " + q.config.WhereClause)
	}

	// GROUP BY
	if q.config.GroupBy != "" {
		sb.WriteString(" GROUP BY " + q.config.GroupBy)
	}

	// HAVING
	if q.config.Having != "" {
		sb.WriteString(" HAVING " + q.config.Having)
	}

	// ORDER BY
	if q.config.OrderBy != "" {
		sb.WriteString(" ORDER BY " + q.config.OrderBy)
	}

	// LIMIT
	if q.config.Limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", q.config.Limit))
	}

	// OFFSET
	if q.config.Offset > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", q.config.Offset))
	}

	// FOR UPDATE
	if q.config.ForUpdate {
		sb.WriteString(" FOR UPDATE")
	}

	return sb.String()
}

func (q Query) Count(ctx context.Context) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", q.table)
	if q.config.WhereClause != "" {
		query += " WHERE " + q.config.WhereClause
	}
	err := q.db.GetContext(ctx, &count, query, q.args...)
	return count, q.wrapError(err, "execute count query")
}

func (q Query) Exists(ctx context.Context) (bool, error) {
	// 构建优化查询
	tmpQuery := q.clone()
	tmpQuery.config.SelectFields = []string{"1"}
	tmpQuery.config.Limit = 1

	queryStr := tmpQuery.buildSelectQuery()

	// 执行查询
	row := tmpQuery.db.QueryRowContext(ctx, queryStr, tmpQuery.args...)

	var result int
	err := row.Scan(&result)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, q.wrapError(err, "exists check failed")
	default:
		return true, nil
	}
}

// WithCursor 实现基于游标的分页
func (q Query) WithCursor(keyField string, cursor *types.Cursor) types.Query {
	newQuery := q.clone()

	// 如果没有提供游标，返回未修改的查询
	if cursor == nil || keyField == "" {
		return newQuery
	}

	// 设置分页大小
	if cursor.Limit > 0 {
		newQuery = newQuery.Limit(cursor.Limit + 1).(*Query) // 获取比需要多一条记录以检查是否有更多页
	}

	// 如果没有键值，只应用限制
	if cursor.KeyValue == nil {
		return newQuery
	}

	// 解析查询的排序规则，以确定游标条件
	orderBy := newQuery.config.OrderBy
	if orderBy == "" {
		// 默认按键字段升序排序
		orderBy = keyField + " ASC"
		newQuery = newQuery.OrderBy(orderBy).(*Query)
	}

	// 分析排序规则
	orderParts := strings.Fields(orderBy)
	if len(orderParts) < 2 {
		// 如果排序语法不完整，假设是升序
		orderParts = append(orderParts, "ASC")
	}

	// 确定排序方向
	orderDirection := strings.ToUpper(orderParts[1])

	// 构建游标条件
	var compareOp string

	// 根据游标方向和排序方向确定比较运算符
	if cursor.Forward {
		if orderDirection == "ASC" {
			compareOp = ">"
		} else {
			compareOp = "<"
		}
	} else {
		if orderDirection == "ASC" {
			compareOp = "<"
		} else {
			compareOp = ">"
		}
	}

	// 构建WHERE子句
	whereClause := fmt.Sprintf("%s %s ?", keyField, compareOp)

	// 添加到现有条件
	if newQuery.config.WhereClause != "" {
		newQuery.config.WhereClause = fmt.Sprintf("(%s) AND (%s)",
			newQuery.config.WhereClause, whereClause)
		newQuery.args = append(newQuery.args, cursor.KeyValue)
	} else {
		newQuery.config.WhereClause = whereClause
		newQuery.args = []interface{}{cursor.KeyValue}
	}

	return newQuery
}

// GetPage 执行分页查询并返回结果
// GetPage 执行分页查询并返回结果
func (q Query) GetPage(ctx context.Context, dest interface{}, withCount bool) (*types.PageResult, error) {
	// 验证目标是否为切片指针
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr || destValue.Elem().Kind() != reflect.Slice {
		return nil, fmt.Errorf("%w: destination must be a pointer to slice", types.ErrInvalidStructure)
	}

	// 保存原始限制，以便后面使用
	originalLimit := q.config.Limit

	// 执行查询获取当前页数据
	err := q.GetAll(ctx, dest)
	if err != nil {
		return nil, q.wrapError(err, "execute page query")
	}

	// 获取结果数量
	resultSlice := destValue.Elem()
	resultCount := resultSlice.Len()

	// 准备结果
	result := &types.PageResult{
		Data:    dest,
		HasNext: false,
		HasPrev: false,
	}

	// 处理有下一页的情况
	hasMore := originalLimit > 0 && resultCount > originalLimit

	// 如果我们获取了额外的记录，需要从结果中移除它
	if hasMore && resultCount > 0 {
		// 设置截断后的切片 (移除额外记录)
		resultSlice.Set(resultSlice.Slice(0, resultCount-1))
		result.HasNext = true

		// 创建下一页游标
		lastItem := resultSlice.Index(resultSlice.Len() - 1)

		// 获取键字段值
		// 注意：这里假设我们知道键字段的位置，实际实现中需要通过反射提取对应字段
		// 此处简化实现，在实际代码中需要按字段名或标签查找
		var keyValue interface{}

		// 这里简化为取第一个字段作为键值
		// 实际代码需要根据字段名查找
		if lastItem.Kind() == reflect.Struct && lastItem.NumField() > 0 {
			keyValue = lastItem.Field(0).Interface()
		}

		if keyValue != nil {
			result.NextCursor = &types.Cursor{
				KeyValue: keyValue,
				Forward:  true,
				Limit:    originalLimit,
			}
		}
	}

	// 判断是否有上一页
	// 这取决于游标的存在和方向
	// 简化实现，假设如果使用了游标并且不是第一页则有上一页
	if q.config.WhereClause != "" && resultCount > 0 {
		result.HasPrev = true

		// 创建上一页游标
		firstItem := resultSlice.Index(0)

		// 获取键字段值（同样简化实现）
		var keyValue interface{}
		if firstItem.Kind() == reflect.Struct && firstItem.NumField() > 0 {
			keyValue = firstItem.Field(0).Interface()
		}

		if keyValue != nil {
			result.PrevCursor = &types.Cursor{
				KeyValue: keyValue,
				Forward:  false,
				Limit:    originalLimit,
			}
		}
	}

	// 如果需要，计算总记录数
	if withCount {
		// 创建一个新的查询对象，避免修改原始查询
		tempQuery := Query{
			DB:     q.DB,
			table:  q.table,
			config: q.config,                           // 拷贝原始配置
			args:   append([]interface{}{}, q.args...), // 拷贝参数
		}

		// 重置LIMIT设置
		tempQuery.config.Limit = 0

		// 使用临时查询执行计数
		totalCount, err := tempQuery.Count(ctx)
		if err != nil {
			return nil, q.wrapError(err, "count total records")
		}
		result.TotalCount = totalCount
	}

	return result, nil
}

// PageByKeySince 基于指定键值进行分页，并返回从该键值开始的记录
func (q Query) PageByKeySince(ctx context.Context, dest interface{}, keyField string, keyValue interface{}, limit int, withCount bool) (*types.PageResult, error) {
	cursor := &types.Cursor{
		KeyValue: keyValue,
		Forward:  true,
		Limit:    limit,
	}

	return q.WithCursor(keyField, cursor).GetPage(ctx, dest, withCount)
}

// PageByKeyBefore 基于指定键值进行分页，并返回该键值之前的记录
func (q Query) PageByKeyBefore(ctx context.Context, dest interface{}, keyField string, keyValue interface{}, limit int, withCount bool) (*types.PageResult, error) {
	cursor := &types.Cursor{
		KeyValue: keyValue,
		Forward:  false,
		Limit:    limit,
	}

	return q.WithCursor(keyField, cursor).GetPage(ctx, dest, withCount)
}

// WithCompositeCursor 实现基于复合游标的分页
// 这对于按多个字段排序的场景很有用
func (q Query) WithCompositeCursor(cursor *types.CompositeCursor) types.Query {
	if cursor == nil || len(cursor.KeyValues) == 0 || len(cursor.OrderFields) == 0 {
		return q
	}

	// 创建新的Query实例作为拷贝，而不是使用类型断言
	newQuery := Query{
		DB:     q.DB,
		table:  q.table,
		config: q.config,                           // 拷贝配置
		args:   append([]interface{}{}, q.args...), // 拷贝参数
	}

	// 设置分页大小
	if cursor.Limit > 0 {
		newQuery.config.Limit = cursor.Limit + 1
	}

	// 构建排序字段
	var orderParts []string
	for _, field := range cursor.OrderFields {
		orderParts = append(orderParts, fmt.Sprintf("%s %s", field.Name, field.Direction))
	}
	orderBy := strings.Join(orderParts, ", ")
	newQuery.config.OrderBy = orderBy

	// 构建复合WHERE条件
	// 这使用了"行比较"语法，允许多列同时比较
	// 例如: (col1, col2) > (val1, val2)
	var fieldNames []string
	var fieldPlaceholders []string
	var fieldValues []interface{}

	for _, field := range cursor.OrderFields {
		fieldName := field.Name
		fieldNames = append(fieldNames, fieldName)

		value, exists := cursor.KeyValues[fieldName]
		if !exists {
			// 如果没有该字段的值，使用占位符
			value = nil
		}

		fieldPlaceholders = append(fieldPlaceholders, "?")
		fieldValues = append(fieldValues, value)
	}

	// 确定比较运算符
	var compareOp string
	if cursor.Forward {
		compareOp = ">"
	} else {
		compareOp = "<"
	}

	// 构建WHERE子句
	whereClause := fmt.Sprintf("(%s) %s (%s)",
		strings.Join(fieldNames, ", "),
		compareOp,
		strings.Join(fieldPlaceholders, ", "))

	// 添加到现有条件
	if newQuery.config.WhereClause != "" {
		newQuery.config.WhereClause = fmt.Sprintf("(%s) AND (%s)",
			newQuery.config.WhereClause, whereClause)
		newQuery.args = append(newQuery.args, fieldValues...)
	} else {
		newQuery.config.WhereClause = whereClause
		newQuery.args = fieldValues
	}

	return &newQuery
}
