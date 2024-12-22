// Copyright (c) 2024 The horm-database Authors (such as CaoHao <18500482693@163.com>). All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-sql-driver/mysql"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/util"
)

// Client 底层查询代理
type Client interface {
	Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error)   // 执行语句
	Query(ctx context.Context, next NextFunc, query string, args ...interface{}) error // 查询语句
	Prepare(ctx context.Context, query string) (*sql.Stmt, error)                      // 预绑定
	BeginTx(ctx context.Context) error                                                 // 开启事务
	FinishTx(err error) error                                                          // 结束事务
}

// NextFunc query 请求时，每一行数据记录执行的逻辑，
// 包装在框架底层，可以防止用户漏写 rows.Close, rows.Err 等, 也可以防止 scan 过程中 context 被取消
// 返回值 error = nil 继续下一个 row， ErrBreak 则提前结束循环 !=nil 返回失败
type NextFunc func(*sql.Rows) error

// ErrBreak 如果 NextFunc 函数返回 ErrBreak 则提前结束 scan 循环
var ErrBreak = errors.New("sql scan rows break")

const (
	OpExec  = 1
	OpQuery = 2
)

// client 后端请求结构体
type client struct {
	dbType       int
	dsn          string
	readTimeout  time.Duration
	writeTimeout time.Duration

	db *sql.DB
	tx *sql.Tx
}

// NewClient 新建一个 db client
func NewClient(addr *util.DBAddress) (Client, error) {
	cli := &client{
		dbType:       addr.Type,
		dsn:          addr.Conn.DSN,
		writeTimeout: time.Duration(addr.WriteTimeout) * time.Millisecond,
		readTimeout:  time.Duration(addr.ReadTimeout) * time.Millisecond,
	}

	db, err := getDB(addr.Type, addr.Conn.DSN)
	if err != nil {
		return nil, err
	}

	cli.db = db

	return cli, nil
}

// Query 查询 query 语句
func (c *client) Query(ctx context.Context, next NextFunc, query string, args ...interface{}) error {
	_, err := c.invoke(ctx, OpQuery, query, args, next)
	return err
}

// Exec 执行 query 语句
func (c *client) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.invoke(ctx, OpExec, query, args, nil)
}

// Prepare creates a prepared statement.
func (c *client) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	if c.tx != nil {
		return c.tx.PrepareContext(ctx, query)
	} else {
		return c.db.PrepareContext(ctx, query)
	}
}

// BeginTx 开启事务
func (c *client) BeginTx(ctx context.Context) (err error) {
	c.tx, err = c.db.BeginTx(ctx, new(sql.TxOptions))
	return err
}

// FinishTx 结束事务
func (c *client) FinishTx(err error) error {
	defer func() { //事务结束，tx 关闭
		c.tx = nil
	}()

	if err != nil {
		if e := c.tx.Rollback(); e != nil {
			return errs.NewDBErrorf(errs.ErrTransaction,
				"rollback error: [%s], source query error=[%s]", e.Error(), err.Error())
		}
		return err
	}

	err = c.tx.Commit()
	if err != nil {
		if e := c.tx.Rollback(); e != nil {
			return errs.NewDBErrorf(errs.ErrTransaction,
				"rollback error: [%s], source commit error=[%s]", e.Error(), err.Error())
		}
		return err
	}

	return nil
}

// invoke 收发mysql包
func (c *client) invoke(ctx context.Context,
	op int8, query string, args []interface{}, next NextFunc) (rsp sql.Result, err error) {
	defer func() {
		if err == sql.ErrNoRows {
			err = errs.NewDBError(errs.ErrSQLQuery, "sql: no rows in result set")
			return
		}

		switch e := err.(type) {
		case *clickhouse.Exception:
			err = errs.NewDBError(int(e.Code), e.Message)
		case *mysql.MySQLError:
			err = errs.NewDBError(int(e.Number), e.Message)
		case *errs.Error:
			err = e
		case nil:
			err = nil
		default:
			err = errs.NewDBError(errs.ErrClientNet, err.Error())
		}
	}()

	timeout := c.writeTimeout

	if op == OpQuery {
		timeout = c.readTimeout
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	begin := time.Now()

	switch op {
	case OpQuery:
		err = c.query(ctx, query, args, next)
	case OpExec:
		if c.tx != nil {
			rsp, err = c.tx.ExecContext(ctx, query, args...)
		} else {
			rsp, err = c.db.ExecContext(ctx, query, args...)
		}
	default:
		return nil, errs.NewDBError(errs.ErrUnknown, "mysql: undefined op type")
	}

	if e, ok := err.(net.Error); ok {
		if e.Timeout() {
			err = errs.Newf(errs.ErrClientTimeout, fmt.Sprintf("%s, cost:%s", e.Error(), time.Since(begin)))
		}
	}

	return
}

func (c *client) query(ctx context.Context, query string, args []interface{}, next NextFunc) (err error) {
	var rows *sql.Rows

	if c.tx != nil {
		rows, err = c.tx.QueryContext(ctx, query, args...)
	} else {
		rows, err = c.db.QueryContext(ctx, query, args...)
	}

	if err != nil {
		return
	}

	defer rows.Close()

	for rows.Next() {
		err = next(rows)

		if err == ErrBreak {
			break
		}

		if err != nil {
			return
		}
	}

	err = rows.Err()
	return
}
