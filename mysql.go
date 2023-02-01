package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql" // init mysql driver
	"github.com/goriller/ginny-util/graceful"
	"go.uber.org/zap"
)

// MysqlDB 数据库管理器 读写分离 仅对同一业务库
type MysqlDB struct {
	writeDB *sql.DB
	readDBs []*sql.DB
	logger  *zap.Logger
}

// NewMysqlDB 根据基础配置 初始化数据库
func NewMysqlDB(ctx context.Context, config *Config, logger *zap.Logger) (*MysqlDB, error) {
	writeDB, err := newDB(ctx, &config.WDB, config)
	if err != nil {
		return nil, err
	}
	// RDB多个
	rDBLen := len(config.RDBs)
	// 未配置rdb
	if rDBLen == 0 {
		config.RDBs = append(config.RDBs, config.WDB)
	}
	readDBs := make([]*sql.DB, 0, rDBLen)
	for i := 0; i < rDBLen; i++ {
		source := &Source{
			Host:     config.RDBs[i].Host,
			UserName: config.RDBs[i].UserName,
			PassWord: config.RDBs[i].PassWord,
		}
		readDB, err := newDB(ctx, source, config)
		if err != nil {
			return nil, err
		}
		readDBs = append(readDBs, readDB)
	}

	db := &MysqlDB{
		writeDB: writeDB,
		readDBs: readDBs,
		logger:  logger,
	}

	// graceful
	graceful.AddCloser(func(ctx context.Context) error {
		return db.Close()
	})

	return db, nil
}

// RDB 随机返回一个读库
func (m *MysqlDB) RDB() *sql.DB {
	return m.readDBs[rand.Intn(len(m.readDBs))]
}

// WDB 返回唯一写库
func (m *MysqlDB) WDB() *sql.DB {
	return m.writeDB
}

// Close 关闭所有读写连接池，停止keepalive保活协程。该函数应当很少使用到
func (m *MysqlDB) Close() error {
	if err := m.writeDB.Close(); err != nil {
		m.logger.With(zap.String("action", "mysql")).Error("close write db error", zap.Error(err))
		return err
	}
	for i := 0; i < len(m.readDBs); i++ {
		if err := m.readDBs[i].Close(); err != nil {
			m.logger.With(zap.String("action", "mysql")).Error("close db read pool error", zap.Error(err))
			return err
		}
	}
	return nil
}

// newDB
func newDB(ctx context.Context, source *Source, config *Config) (*sql.DB, error) {
	// user:pass@tcp(ip:port)/dbname
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true&loc=Local&multiStatements=true",
		source.UserName, source.PassWord, source.Host, config.DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(config.MaxOpenConn)
	db.SetMaxIdleConns(config.MaxIdleConn)
	db.SetConnMaxLifetime(time.Duration(config.MaxLifetime) * time.Second)
	return db, db.PingContext(ctx)
}
