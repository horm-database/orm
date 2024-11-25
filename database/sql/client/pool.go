package client

import (
	"database/sql"
	"sync"
	"time"

	"github.com/horm-database/common/consts"
)

var (
	dbLock = new(sync.RWMutex)
	dbPool = map[string]*sql.DB{}
)

type Options struct {
	MaxIdle     int           // 最大空闲连接数
	MaxOpen     int           // 最大活跃连接数
	MaxLifetime time.Duration // 最大连接生存时间
	MaxIdleTime time.Duration // 最大空闲时间
}

// defaultOpt 默认配置
var defaultOpt = &Options{
	MaxIdle:     10,
	MaxOpen:     10000,
	MaxLifetime: 3 * time.Minute,
	MaxIdleTime: 0,
}

// getDB 获取连接
// clickhouse dsn: v1: https://github.com/ClickHouse/clickhouse-go/tree/v1#dsn
// clickhouse dsn: v2: https://github.com/ClickHouse/clickhouse-go#dsn
func getDB(dbType int, dsn string) (*sql.DB, error) {
	dbLock.RLock()
	db, ok := dbPool[dsn]
	dbLock.RUnlock()

	if ok {
		return db, nil
	}

	dbLock.Lock()
	defer dbLock.Unlock()

	db, ok = dbPool[dsn]
	if ok {
		return db, nil
	}

	var driverName string

	switch dbType {
	case consts.DBTypePostgreSQL:
		driverName = "postgresql"
	case consts.DBTypeClickHouse:
		driverName = "clickhouse"
	case consts.DBTypeOracle:
		driverName = "oracle"
	case consts.DBTypeDB2:
		driverName = "db2"
	case consts.DBTypeSQLite:
		driverName = "sqlite3"
	default:
		driverName = "mysql"
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}

	if defaultOpt.MaxIdle > 0 {
		db.SetMaxIdleConns(defaultOpt.MaxIdle)
	}
	if defaultOpt.MaxOpen > 0 {
		db.SetMaxOpenConns(defaultOpt.MaxOpen)
	}
	if defaultOpt.MaxLifetime > 0 {
		db.SetConnMaxLifetime(defaultOpt.MaxLifetime)
	}

	if defaultOpt.MaxIdleTime > 0 {
		db.SetConnMaxIdleTime(defaultOpt.MaxIdleTime)
	}

	dbPool[dsn] = db
	return db, nil
}
