package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql" // init mysql driver
	"go.uber.org/zap"
)

// 默认的keepalive间隔 3h
const defaultKeepalive = 3 * 60 * 60 * time.Second

// MysqlDB 数据库管理器 读写分离 仅对同一业务库
type MysqlDB struct {
	writeDB *sql.DB
	readDBs []*sql.DB

	cancel context.CancelFunc
	logger *zap.Logger
}

// NewMysqlDB 根据基础配置 初始化数据库
func NewMysqlDB(ctx context.Context, config *Config, logger *zap.Logger) (*MysqlDB, error) {
	writeDB, err := newDB(ctx, &config.WDB, config)
	if err != nil {
		return nil, err
	}
	// RDB多个
	readDBs := make([]*sql.DB, 0, len(config.RDBs))
	for i := 0; i < len(config.RDBs); i++ {
		source := &Source{
			Host: config.RDBs[i].Host,
			User: config.RDBs[i].User,
			Pass: config.RDBs[i].Pass,
		}
		readDB, err := newDB(ctx, source, config)
		if err != nil {
			return nil, err
		}
		readDBs = append(readDBs, readDB)
	}

	return NewMysqlDBFromSqlDB(ctx, writeDB, readDBs,
		time.Duration(config.Keepalive)*time.Second, logger), nil
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
func (m *MysqlDB) Close() {
	m.cancel()
	if err := m.writeDB.Close(); err != nil {
		m.logger.With(zap.String("action", "mysql")).Fatal("close write db error", zap.Error(err))
	}
	for i := 0; i < len(m.readDBs); i++ {
		if err := m.readDBs[i].Close(); err != nil {
			m.logger.With(zap.String("action", "mysql")).Fatal("close db read pool error", zap.Error(err))
		}
	}
}

//
func newDB(ctx context.Context, source *Source, config *Config) (*sql.DB, error) {
	// user:pass@tcp(ip:port)/dbname
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true&loc=Local&multiStatements=true",
		source.User, source.Pass, source.Host, config.DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(config.MaxOpenConn)
	db.SetMaxIdleConns(config.MaxIdleConn)
	db.SetConnMaxLifetime(time.Duration(config.MaxLifetime) * time.Second)
	return db, db.Ping()
}

// NewMysqlDBFromSqlDB 根据SqlDB对象 初始化数据库
func NewMysqlDBFromSqlDB(ctx context.Context, writeDB *sql.DB,
	readDBs []*sql.DB, keepaliveInterval time.Duration, logger *zap.Logger) *MysqlDB {
	rand.Seed(time.Now().Unix())
	//控制keepalive goroutine结束
	c, cancel := context.WithCancel(context.Background())
	go keepalive(c, writeDB, keepaliveInterval, logger)
	for i := 0; i < len(readDBs); i++ {
		go keepalive(c, readDBs[i], keepaliveInterval, logger)
	}

	return &MysqlDB{
		writeDB: writeDB,
		readDBs: readDBs,
		cancel:  cancel,
	}
}

// 定时ping db 保持连接激活
func keepalive(ctx context.Context, db *sql.DB, interval time.Duration, logger *zap.Logger) {
	if interval.Nanoseconds() == 0 {
		interval = defaultKeepalive
	}

	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ctx.Done():
			logger.With(zap.String("action", "mysql")).Info("keepalive db end")
			return
		case <-ticker.C:
			if err := db.Ping(); err != nil {
				logger.With(zap.String("action", "mysql")).Error("keepalive db ping error", zap.Error(err))
			}
		}
	}
}
