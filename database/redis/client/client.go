// Package redis 封装第三方库redigo
package client

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/util"
)

// Client redis请求接口
type Client interface {
	Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error)
}

// redisCli 后端请求结构体
type redisCli struct {
	target       string
	password     string
	readTimeout  time.Duration
	writeTimeout time.Duration

	pool *redigo.Pool
}

// NewClient 新建一个redis后端请求代理
var NewClient = func(addr *util.DBAddress) Client {
	c := &redisCli{
		target:       addr.Conn.DSN,
		password:     addr.Conn.Password,
		readTimeout:  time.Duration(addr.ReadTimeout) * time.Millisecond,
		writeTimeout: time.Duration(addr.WriteTimeout) * time.Millisecond,
	}

	c.pool = getPool(addr.Conn.DSN, addr.Conn.Password)
	return c
}

// Do 执行redis命令
func (c *redisCli) Do(ctx context.Context, cmd string, args ...interface{}) (rsp interface{}, err error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, errs.NewDBError(errs.ErrClientTimeout, err.Error())
	}

	defer conn.Close()

	timeout := c.writeTimeout
	if consts.OpType(cmd) == consts.OpTypeRead {
		timeout = c.readTimeout
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	begin := time.Now()
	reply, err := redigo.DoWithTimeout(conn, timeout, cmd, args...)
	cost := time.Since(begin)

	if err != nil {
		if e, ok := err.(net.Error); ok {
			if e.Timeout() {
				msg := fmt.Sprintf("%s, cost:%s", e.Error(), cost)
				return nil, errs.NewDBError(errs.ErrClientTimeout, msg)
			}

			if strings.Contains(err.Error(), "connection refused") {
				return nil, errs.NewDBError(errs.ErrClientConnect, err.Error())
			}
		}
		return nil, errs.NewDBError(errs.ErrRedisDo, err.Error())
	}

	return reply, nil
}
