package mysql

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/didi/gendry/builder"
	"github.com/didi/gendry/scanner"
	"go.uber.org/zap"
)

func init() {
	scanner.SetTagName("json")
}

// SqlBuilder
type SqlBuilder struct {
	DB     *MysqlDB
	Query  *query
	logger *zap.Logger
}

// NewSqlBuilder
func NewSqlBuilder(ctx context.Context, config *Config, logger *zap.Logger) (*SqlBuilder, error) {
	mgr, err := NewMysqlDB(ctx, config, logger)
	if err != nil {
		return nil, err
	}
	return &SqlBuilder{
		DB: mgr,
		Query: &query{
			MysqlDB: mgr,
		},
	}, nil
}

// QuerySql by native sql
func (s *SqlBuilder) QuerySql(ctx context.Context, sqlStr string,
	bindMap map[string]interface{}, entity interface{}) error {
	var err error
	cond, val, err := builder.NamedQuery(sqlStr, bindMap)
	if err != nil {
		return err
	}
	s.logger.With(zap.String("action", "SqlBuilder")).Info("sql:",
		zap.String("cond", cond), zap.Any("val", val))
	return s.querySql(ctx, cond, val, entity)
}

// ExecuteSql by native sql
func (s *SqlBuilder) ExecuteSql(ctx context.Context, sqlStr string,
	bindMap map[string]interface{}) (int64, error) {
	var err error
	cond, val, err := builder.NamedQuery(sqlStr, bindMap)
	if err != nil {
		return 0, err
	}
	s.logger.With(zap.String("action", "SqlBuilder")).Info("sql:",
		zap.String("cond", cond), zap.Any("val", val))
	return s.execSql(ctx, cond, val)
}

//Find gets one record from table by condition "where"
func (s *SqlBuilder) Find(ctx context.Context, entity interface{},
	table string, where map[string]interface{}, selectFields ...[]string) error {
	if table == "" {
		return errors.New("table name couldn't be empty")
	}
	var field []string
	if len(selectFields) > 0 {
		field = selectFields[0]
	} else {
		field = nil
	}
	// limit
	if where == nil {
		where = map[string]interface{}{}
	}
	where["_limit"] = []uint{0, 1}
	cond, val, err := builder.BuildSelect(table, where, field)
	if nil != err {
		return err
	}
	s.logger.With(zap.String("action", "SqlBuilder")).Info("sql:",
		zap.String("cond", cond), zap.Any("val", val))
	return s.querySql(ctx, cond, val, entity)
}

//FindAll gets multiple records from table by condition "where"
func (s *SqlBuilder) FindAll(ctx context.Context, entity interface{},
	table string, where map[string]interface{}, selectFields ...[]string) error {
	if table == "" {
		return errors.New("table name couldn't be empty")
	}
	var field []string
	if len(selectFields) > 0 {
		field = selectFields[0]
	} else {
		field = nil
	}
	cond, val, err := builder.BuildSelect(table, where, field)
	if nil != err {
		return err
	}
	s.logger.With(zap.String("action", "SqlBuilder")).Info("sql:",
		zap.String("cond", cond), zap.Any("val", val))
	return s.querySql(ctx, cond, val, entity)
}

//Insert inserts data into table
func (s *SqlBuilder) Insert(ctx context.Context, table string, entity interface{}) (int64, error) {
	if table == "" {
		return 0, errors.New("table name couldn't be empty")
	}
	dataMap, err := ConvertEntityToMap(entity)
	if err != nil {
		return 0, err
	}
	data := []map[string]interface{}{dataMap}
	cond, val, err := builder.BuildInsert(table, data)
	if nil != err {
		return 0, err
	}
	s.logger.With(zap.String("action", "SqlBuilder")).Info("sql:",
		zap.String("cond", cond), zap.Any("val", val))
	return s.execSql(ctx, cond, val)
}

//Update updates the table COLUMNS
func (s *SqlBuilder) Update(ctx context.Context, table string,
	where, update map[string]interface{}) (int64, error) {
	if table == "" {
		return 0, errors.New("table name couldn't be empty")
	}
	cond, val, err := builder.BuildUpdate(table, where, update)
	if err != nil {
		return 0, err
	}
	s.logger.With(zap.String("action", "SqlBuilder")).Info("sql:",
		zap.String("cond", cond), zap.Any("val", val))
	return s.execSql(ctx, cond, val)
}

// Delete deletes matched records in COLUMNS
func (s *SqlBuilder) Delete(ctx context.Context, table string,
	where map[string]interface{}) (int64, error) {
	if table == "" {
		return 0, errors.New("table name couldn't be empty")
	}
	cond, val, err := builder.BuildDelete(table, where)
	if err != nil {
		return 0, err
	}
	s.logger.With(zap.String("action", "SqlBuilder")).Info("sql:",
		zap.String("cond", cond), zap.Any("val", val))
	return s.execSql(ctx, cond, val)
}

// querySql
func (s *SqlBuilder) querySql(ctx context.Context, cond string,
	val []interface{}, entity interface{}) error {
	stmt, err := s.Query.MysqlDB.RDB().PrepareContext(ctx, cond)
	if err != nil {
		return err
	}
	rows, err := stmt.QueryContext(ctx, val...)
	if nil != err || nil == rows {
		return err
	}
	defer func() {
		if stmt != nil {
			stmt.Close()
		}
	}()
	err = scanner.ScanClose(rows, entity)
	if err != nil && err.Error() != "[scanner]: empty result" {
		return err
	}
	return nil
}

// execSql
func (s *SqlBuilder) execSql(ctx context.Context, cond string, val []interface{}) (int64, error) {
	stmt, err := s.Query.MysqlDB.WDB().PrepareContext(ctx, cond)
	if err != nil {
		return 0, err
	}
	result, err := stmt.ExecContext(ctx, val...)
	if nil != err || nil == result {
		return 0, err
	}
	defer func() {
		if stmt != nil {
			stmt.Close()
		}
	}()
	return result.RowsAffected()
}

// ConvertEntityToMap
func ConvertEntityToMap(entity interface{}) (map[string]interface{}, error) {
	entityMap := make(map[string]interface{})
	bt, err := json.Marshal(entity)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bt, &entityMap)
	if err != nil {
		return nil, err
	}
	return entityMap, nil
}
