package postgresql_helper

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"

	"github.com/songzhibin97/postgresql_helper/types"
)

var _ types.Table = (*Table)(nil)

type Table struct {
	*DB
	name string
}

func (t Table) Insert(ctx context.Context, data interface{}) error {
	return t.withMetrics(ctx, t.name, insertOper, func(ctx context.Context) error {
		// 解析数据结构获取字段和值
		fields, values, err := extractFieldsAndValues(data)
		if err != nil {
			return t.wrapError(err, "extract fields for insert")
		}

		if len(fields) == 0 {
			return t.wrapError(types.ErrInvalidStructure, "no fields to insert")
		}

		// 构建 INSERT 语句
		columns := strings.Join(fields, ", ")
		placeholders := make([]string, len(fields))
		for i := range placeholders {
			placeholders[i] = ":" + fields[i]
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			t.name, columns, strings.Join(placeholders, ", "))

		// 构建字段-值映射供 sqlx.Named 使用
		namedArgs := make(map[string]interface{})
		for i, field := range fields {
			namedArgs[field] = values[i]
		}

		// 使用 Named 参数执行
		query, args, err := sqlx.Named(query, namedArgs)
		if err != nil {
			return t.wrapError(err, "prepare insert statement")
		}

		// 转换成数据库驱动支持的格式
		query = t.db.Rebind(query)

		_, err = t.db.ExecContext(ctx, query, args...)
		return t.wrapError(err, "insert into "+t.name)
	})
}

// InsertAndGetID 在表中插入数据并返回生成的ID（通常是自增主键）
// 此方法适用于包含自增主键或序列的表
// 参数:
//
//	ctx: 上下文，可用于取消操作或传递事务
//	data: 要插入的数据，可以是带有db标签的结构体或字段名到值的映射
//	idColumnName: 要返回的ID列名，默认为"id"
//
// 返回:
//
//	int64: 新插入行的ID
//	error: 如有错误发生
func (t Table) InsertAndGetID(ctx context.Context, data interface{}, idColumnName ...string) (int64, error) {
	var id int64

	// 确定ID列名
	idColumn := "id"
	if len(idColumnName) > 0 && idColumnName[0] != "" {
		idColumn = idColumnName[0]
	}

	err := t.withMetrics(ctx, t.name, insertOper, func(ctx context.Context) error {
		// 解析数据结构获取字段和值
		fields, values, err := extractFieldsAndValues(data)
		if err != nil {
			return t.wrapError(err, "extract fields for insert")
		}

		if len(fields) == 0 {
			return t.wrapError(types.ErrInvalidStructure, "no fields to insert")
		}

		// 构建INSERT语句
		columns := strings.Join(fields, ", ")
		placeholders := make([]string, len(fields))
		for i := range placeholders {
			placeholders[i] = ":" + fields[i]
		}

		// 添加RETURNING子句以获取生成的ID
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
			t.name, columns, strings.Join(placeholders, ", "), idColumn)

		// 构建字段-值映射供sqlx.Named使用
		namedArgs := make(map[string]interface{})
		for i, field := range fields {
			namedArgs[field] = values[i]
		}

		// 使用Named参数准备语句
		query, args, err := sqlx.Named(query, namedArgs)
		if err != nil {
			return t.wrapError(err, "prepare insert statement")
		}

		// 转换成数据库驱动支持的格式
		query = t.db.Rebind(query)

		// 执行查询并获取返回的ID
		row := t.db.QueryRowxContext(ctx, query, args...)
		if err := row.Scan(&id); err != nil {
			return t.wrapError(err, "retrieve generated id")
		}

		return nil
	})

	return id, err
}

// InsertAndGetMultipleColumns 在表中插入数据并返回多个生成或指定的列值
// 当你需要返回多个列值（例如复合主键或额外计算列）时非常有用
// 参数:
//
//	ctx: 上下文，可用于取消操作或传递事务
//	data: 要插入的数据，可以是带有db标签的结构体或字段名到值的映射
//	returnColumns: 要返回的列名数组
//
// 返回:
//
//	map[string]interface{}: 包含请求的列名及其值的映射
//	error: 如有错误发生
func (t Table) InsertAndGetMultipleColumns(ctx context.Context, data interface{}, returnColumns []string) (map[string]interface{}, error) {
	if len(returnColumns) == 0 {
		return nil, t.wrapError(fmt.Errorf("%w: no return columns specified", types.ErrInvalidStructure), "insert and get multiple columns")
	}

	result := make(map[string]interface{})

	err := t.withMetrics(ctx, t.name, insertOper, func(ctx context.Context) error {
		// 解析数据结构获取字段和值
		fields, values, err := extractFieldsAndValues(data)
		if err != nil {
			return t.wrapError(err, "extract fields for insert")
		}

		if len(fields) == 0 {
			return t.wrapError(types.ErrInvalidStructure, "no fields to insert")
		}

		// 构建INSERT语句
		columns := strings.Join(fields, ", ")
		placeholders := make([]string, len(fields))
		for i := range placeholders {
			placeholders[i] = ":" + fields[i]
		}

		// 添加RETURNING子句以获取多个列
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
			t.name, columns, strings.Join(placeholders, ", "), strings.Join(returnColumns, ", "))

		// 构建字段-值映射供sqlx.Named使用
		namedArgs := make(map[string]interface{})
		for i, field := range fields {
			namedArgs[field] = values[i]
		}

		// 使用Named参数准备语句
		query, args, err := sqlx.Named(query, namedArgs)
		if err != nil {
			return t.wrapError(err, "prepare insert statement")
		}

		// 转换成数据库驱动支持的格式
		query = t.db.Rebind(query)

		// 执行查询并扫描返回值
		row := t.db.QueryRowxContext(ctx, query, args...)

		// 准备接收返回值的容器
		dest := make([]interface{}, len(returnColumns))
		destPtrs := make([]interface{}, len(returnColumns))
		for i := range dest {
			destPtrs[i] = &dest[i]
		}

		// 扫描结果到目标容器
		if err := row.Scan(destPtrs...); err != nil {
			return t.wrapError(err, "retrieve generated values")
		}

		// 将扫描结果填充到返回映射
		for i, col := range returnColumns {
			// 处理扫描的值，可能是不同类型
			val := dest[i]
			if bytesVal, ok := val.([]byte); ok {
				// 将[]byte转换为字符串，常见于文本字段
				result[col] = string(bytesVal)
			} else {
				result[col] = val
			}
		}

		return nil
	})

	return result, err
}

// InsertAndGetObject 插入数据并将返回的值填充到提供的对象中
// 对于希望直接将返回数据填充到结构体的场景很有用
// 参数:
//
//	ctx: 上下文，可用于取消操作或传递事务
//	data: 要插入的数据
//	dest: 接收返回数据的目标对象（必须是指针）
//
// 返回:
//
//	error: 如有错误发生
func (t Table) InsertAndGetObject(ctx context.Context, data interface{}, dest interface{}) error {
	return t.withMetrics(ctx, t.name, insertOper, func(ctx context.Context) error {
		// 验证dest是指针
		destValue := reflect.ValueOf(dest)
		if destValue.Kind() != reflect.Ptr || destValue.IsNil() {
			return t.wrapError(fmt.Errorf("%w: destination must be a non-nil pointer", types.ErrInvalidStructure), "insert and get object")
		}

		// 解析数据结构获取字段和值
		fields, values, err := extractFieldsAndValues(data)
		if err != nil {
			return t.wrapError(err, "extract fields for insert")
		}

		if len(fields) == 0 {
			return t.wrapError(types.ErrInvalidStructure, "no fields to insert")
		}

		// 构建INSERT语句
		columns := strings.Join(fields, ", ")
		placeholders := make([]string, len(fields))
		for i := range placeholders {
			placeholders[i] = ":" + fields[i]
		}

		// 确定要返回的列
		destElem := destValue.Elem()
		var returnColumns []string

		if destElem.Kind() == reflect.Struct {
			// 如果目标是结构体，从结构体获取db标签作为返回列
			destType := destElem.Type()
			for i := 0; i < destType.NumField(); i++ {
				field := destType.Field(i)
				tag := field.Tag.Get("db")
				if tag != "" && tag != "-" {
					returnColumns = append(returnColumns, tag)
				}
			}
		} else {
			// 其他情况，默认返回所有列（*）
			returnColumns = []string{"*"}
		}

		// 添加RETURNING子句
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
			t.name, columns, strings.Join(placeholders, ", "), strings.Join(returnColumns, ", "))

		// 构建字段-值映射供sqlx.Named使用
		namedArgs := make(map[string]interface{})
		for i, field := range fields {
			namedArgs[field] = values[i]
		}

		// 使用Named参数准备语句
		query, args, err := sqlx.Named(query, namedArgs)
		if err != nil {
			return t.wrapError(err, "prepare insert statement")
		}

		// 转换成数据库驱动支持的格式
		query = t.db.Rebind(query)

		// 使用sqlx将结果直接扫描到目标对象
		row := t.db.QueryRowxContext(ctx, query, args...)
		if err := row.StructScan(dest); err != nil {
			return t.wrapError(err, "scan result into destination object")
		}

		return nil
	})
}

// extractFieldsAndValues 从任意结构体或映射中提取字段名和值
func extractFieldsAndValues(data interface{}) ([]string, []interface{}, error) {
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.Struct:
		return extractFromStruct(val)
	case reflect.Map:
		return extractFromMap(val)
	default:
		return nil, nil, fmt.Errorf("%w: expected struct or map, got %s",
			types.ErrInvalidStructure, val.Kind())
	}
}

// 从结构体提取字段和值
func extractFromStruct(val reflect.Value) ([]string, []interface{}, error) {
	t := val.Type()
	var fields []string
	var values []interface{}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// 获取 db 标签
		dbTag := field.Tag.Get("db")
		if dbTag == "" || dbTag == "-" {
			continue // 跳过未标记或明确排除的字段
		}

		// 处理嵌入式结构体
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			embeddedFields, embeddedValues, err := extractFromStruct(val.Field(i))
			if err != nil {
				return nil, nil, err
			}
			fields = append(fields, embeddedFields...)
			values = append(values, embeddedValues...)
			continue
		}

		// 常规字段
		fieldValue := val.Field(i).Interface()

		// 特殊处理零值（可选）
		if isZeroValue(val.Field(i)) {
			// 这里可以选择跳过零值，或者保留它们
			// 如果想跳过零值，取消下面的注释：
			// continue
		}

		fields = append(fields, dbTag)
		values = append(values, fieldValue)
	}

	return fields, values, nil
}

// 从 map 提取字段和值
func extractFromMap(val reflect.Value) ([]string, []interface{}, error) {
	keys := val.MapKeys()
	if len(keys) == 0 {
		return nil, nil, fmt.Errorf("%w: empty map", types.ErrInvalidStructure)
	}

	// 检查键类型是否为字符串
	if keys[0].Kind() != reflect.String {
		return nil, nil, fmt.Errorf("%w: map keys must be strings", types.ErrInvalidStructure)
	}

	var fields []string
	var values []interface{}

	for _, key := range keys {
		fieldName := key.String()
		fieldValue := val.MapIndex(key).Interface()

		fields = append(fields, fieldName)
		values = append(values, fieldValue)
	}

	return fields, values, nil
}

// 判断值是否为零值
func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func (t Table) Update(ctx context.Context, whereClause string, args map[string]interface{}, data interface{}) (int64, error) {
	var total int64
	err := t.withMetrics(ctx, t.name, updateOper, func(ctx context.Context) error {
		// 构建SET子句
		setValues := make([]string, 0)
		for key, value := range data.(map[string]interface{}) {
			setValues = append(setValues, fmt.Sprintf("%s = :%s", key, key))
			args[key] = value
		}
		setClause := strings.Join(setValues, ", ")

		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", t.name, setClause, whereClause)

		// 使用 NamedExec 来处理命名参数
		query, args, err := sqlx.Named(query, args)
		if err != nil {
			return t.wrapError(err, "prepare update statement")
		}

		// 将命名参数转换为位置参数
		query, args, err = sqlx.In(query, args...)
		if err != nil {
			return t.wrapError(err, "convert named parameters")
		}
		query = t.db.Rebind(query)

		result, err := t.db.ExecContext(ctx, query, args...)
		if err != nil {
			return t.wrapError(err, "update "+t.name)
		}
		total, err = result.RowsAffected()
		return t.wrapError(err, "get rows affected")
	})
	return total, err
}

func (t Table) Delete(ctx context.Context, whereClause string, args map[string]interface{}) (int64, error) {
	var total int64
	err := t.withMetrics(ctx, t.name, deleteOper, func(ctx context.Context) error {
		query := fmt.Sprintf("DELETE FROM %s WHERE %s", t.name, whereClause)

		// 使用 NamedExec 来处理命名参数
		query, args, err := sqlx.Named(query, args)
		if err != nil {
			return t.wrapError(err, "prepare delete statement")
		}

		// 将命名参数转换为位置参数
		query = t.db.Rebind(query)

		result, err := t.db.ExecContext(ctx, query, args...)
		if err != nil {
			return t.wrapError(err, "delete from "+t.name)
		}
		total, err = result.RowsAffected()
		return t.wrapError(err, "get rows affected")
	})
	return total, err
}

func (t Table) Query() types.Query {
	return &Query{
		DB:    t.DB,
		table: t.name,
	}
}

func (t Table) AddColumn(ctx context.Context, col types.ColumnDefinition) error {
	return t.withMetrics(ctx, t.name, columnOper, func(ctx context.Context) error {
		query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", t.name, col.Name, col.Type)
		if !col.Nullable {
			query += " NOT NULL"
		}
		_, err := t.db.ExecContext(ctx, query)
		return t.wrapError(err, "add column "+col.Name)
	})
}

func (t Table) DropColumn(ctx context.Context, columnName string) error {
	return t.withMetrics(ctx, t.name, columnOper, func(ctx context.Context) error {
		query := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", t.name, columnName)
		_, err := t.db.ExecContext(ctx, query)
		return t.wrapError(err, "drop column "+columnName)
	})
}

func (t Table) RenameColumn(ctx context.Context, oldName, newName string) error {
	return t.withMetrics(ctx, t.name, columnOper, func(ctx context.Context) error {
		query := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
			t.name, oldName, newName)
		_, err := t.db.ExecContext(ctx, query)
		return t.wrapError(err, "rename column "+oldName+" to "+newName)
	})
}

func (t Table) CreateIndex(ctx context.Context, indexName string, columns []string, unique bool) error {
	return t.withMetrics(ctx, t.name, indexOper, func(ctx context.Context) error {
		if len(columns) == 0 {
			return t.wrapError(
				fmt.Errorf("%w: no columns specified", types.ErrInvalidStructure),
				"create index",
			)
		}

		uniqueClause := ""
		if unique {
			uniqueClause = "UNIQUE "
		}

		query := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
			uniqueClause,
			indexName,
			t.name,
			strings.Join(columns, ", "))

		_, err := t.db.ExecContext(ctx, query)
		return t.wrapError(err, "create index "+indexName)
	})
}

func (t Table) DropIndex(ctx context.Context, indexName string) error {
	return t.withMetrics(ctx, t.name, indexOper, func(ctx context.Context) error {
		query := fmt.Sprintf("DROP INDEX %s", indexName)
		_, err := t.db.ExecContext(ctx, query)
		return t.wrapError(err, "drop index "+indexName)
	})
}

// 优化后的 BulkUpsert 方法
func (t Table) BulkUpsert(ctx context.Context, conflictKey []string, data []interface{}) (int64, error) {
	var affected int64
	err := t.withMetrics(ctx, t.name, upsertOper, func(ctx context.Context) error {
		if len(data) == 0 {
			return nil // 没有数据要插入，直接返回
		}

		// 使用缓存获取结构体字段定义，减少反射操作
		fields, err := getStructFieldsWithCache(data[0])
		if err != nil {
			return t.wrapError(err, "extract fields for bulk upsert")
		}

		if len(fields) == 0 {
			return t.wrapError(types.ErrInvalidStructure, "no fields found")
		}

		// 构建 INSERT 语句前缀
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
			t.name, strings.Join(fields, ", "))

		// 预分配足够容量以减少内存分配
		placeholders := make([]string, len(data))
		args := make([]interface{}, 0, len(data)*len(fields))

		// 为每行数据构建占位符和提取值
		placeholderTemplate := buildPlaceholderTemplate(len(fields))

		for i, item := range data {
			values, err := extractValuesWithCache(item, fields)
			if err != nil {
				return t.wrapError(err, "extract values")
			}

			// 构建带有参数索引的占位符
			rowPlaceholders := fmt.Sprintf(placeholderTemplate, i*len(fields)+1)
			placeholders[i] = rowPlaceholders
			args = append(args, values...)
		}

		// 完成 VALUES 子句
		query += strings.Join(placeholders, ", ")

		// 添加 ON CONFLICT 子句 (如果提供了冲突键)
		if len(conflictKey) > 0 {
			updateClauses := buildUpdateClauses(fields, conflictKey)
			if len(updateClauses) > 0 {
				query += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s",
					strings.Join(conflictKey, ", "),
					strings.Join(updateClauses, ", "))
			} else {
				query += fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING",
					strings.Join(conflictKey, ", "))
			}
		}

		// 执行批量操作
		result, err := t.db.ExecContext(ctx, query, args...)
		if err != nil {
			return t.wrapError(err, "execute bulk upsert")
		}

		affected, err = result.RowsAffected()
		if err != nil {
			return t.wrapError(err, "get rows affected")
		}

		return nil
	})

	return affected, err
}

// 使用同步映射缓存结构体字段定义
var (
	structFieldsCache = sync.Map{}
	fieldValuesCache  = sync.Map{}
)

// 构建占位符模板 (例如: ($%d, $%d, $%d))
func buildPlaceholderTemplate(fieldCount int) string {
	placeholders := make([]string, fieldCount)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return "(" + strings.Join(placeholders, ", ") + ")"
}

// 构建 UPDATE 子句，排除冲突键
func buildUpdateClauses(fields []string, conflictKey []string) []string {
	// 创建冲突键集合，用于快速查找
	conflictKeySet := make(map[string]struct{}, len(conflictKey))
	for _, key := range conflictKey {
		conflictKeySet[key] = struct{}{}
	}

	// 创建 UPDATE 子句，排除冲突键
	updateClauses := make([]string, 0, len(fields)-len(conflictKey))
	for _, field := range fields {
		if _, isConflictKey := conflictKeySet[field]; !isConflictKey {
			updateClauses = append(updateClauses,
				fmt.Sprintf("%s = EXCLUDED.%s", field, field))
		}
	}

	return updateClauses
}

// 使用缓存获取结构体字段
func getStructFieldsWithCache(data interface{}) ([]string, error) {
	t := reflect.TypeOf(data)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// 检查结构体类型
	if t.Kind() != reflect.Struct {
		return nil, types.ErrInvalidStructure
	}

	// 尝试从缓存获取
	cacheKey := t.PkgPath() + "." + t.Name()
	if cachedFields, found := structFieldsCache.Load(cacheKey); found {
		return cachedFields.([]string), nil
	}

	// 缓存未命中，解析字段
	fields := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("db")
		if tag != "" && tag != "-" {
			fields = append(fields, tag)
		}
	}

	// 存入缓存
	structFieldsCache.Store(cacheKey, fields)

	return fields, nil
}

// 使用缓存提取结构体值
func extractValuesWithCache(data interface{}, fields []string) ([]interface{}, error) {
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, types.ErrInvalidStructure
	}

	t := v.Type()
	cacheKey := t.PkgPath() + "." + t.Name()

	// 尝试从缓存获取字段索引映射
	var fieldIndexMap map[string]int
	if cachedIndices, found := fieldValuesCache.Load(cacheKey); found {
		fieldIndexMap = cachedIndices.(map[string]int)
	} else {
		// 创建字段名到索引的映射
		fieldIndexMap = make(map[string]int, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			tag := t.Field(i).Tag.Get("db")
			if tag != "" && tag != "-" {
				fieldIndexMap[tag] = i
			}
		}
		// 存入缓存
		fieldValuesCache.Store(cacheKey, fieldIndexMap)
	}

	// 提取字段值
	values := make([]interface{}, len(fields))
	for i, fieldName := range fields {
		if idx, ok := fieldIndexMap[fieldName]; ok {
			values[i] = v.Field(idx).Interface()
		} else {
			// 如果字段不存在，使用零值
			values[i] = nil
		}
	}

	return values, nil
}

func getStructFields(data interface{}) []string {
	// 使用反射或结构体标签获取字段列表
	// 这里简化实现，实际应使用sqlx的字段解析
	t := reflect.TypeOf(data)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	var fields []string
	for i := 0; i < t.NumField(); i++ {
		fields = append(fields, t.Field(i).Tag.Get("db"))
	}
	return fields
}

func getStructValues(data interface{}) []interface{} {
	// 使用反射获取字段值
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	var values []interface{}
	for i := 0; i < v.NumField(); i++ {
		values = append(values, v.Field(i).Interface())
	}
	return values
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
