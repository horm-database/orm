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
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/horm-database/common/types"
)

var (
	locker    = new(sync.RWMutex)
	redisPool = map[string]*redigo.Pool{}
)

type Options struct {
	MaxIdle         int           // 最大空闲连接数
	MaxActive       int           // 最大活跃连接数
	IdleTimeout     time.Duration // 最大空闲时间
	MaxConnLifetime time.Duration // 最大连接生存时间
	DefaultTimeout  time.Duration // 默认连接超时和读写超时
	IsWait          bool          // 设置连接池用尽时是否等待空闲连接
}

// defaultOpt 默认配置
var defaultOpt = &Options{
	MaxIdle:         2048,
	MaxActive:       0,
	IdleTimeout:     3 * time.Minute,
	MaxConnLifetime: 0,
	DefaultTimeout:  time.Second,
	IsWait:          false,
}

func getPool(address, password string) *redigo.Pool {
	key := fmt.Sprintf("redis_%s:%s", address, password)
	locker.RLock()
	pool, ok := redisPool[key]
	locker.RUnlock()
	if ok {
		return pool
	}

	locker.Lock()
	defer locker.Unlock()
	pool, ok = redisPool[key]
	if ok {
		return pool
	}

	pool = &redigo.Pool{
		MaxIdle:         defaultOpt.MaxIdle,
		MaxActive:       defaultOpt.MaxActive,
		IdleTimeout:     defaultOpt.IdleTimeout,
		MaxConnLifetime: defaultOpt.MaxConnLifetime,
		Dial: func() (redigo.Conn, error) {
			dialOpts := []redigo.DialOption{
				redigo.DialWriteTimeout(defaultOpt.DefaultTimeout),
				redigo.DialReadTimeout(defaultOpt.DefaultTimeout),
				redigo.DialConnectTimeout(defaultOpt.DefaultTimeout),
				redigo.DialPassword(password),
				redigo.DialContextFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
					dialer := &net.Dialer{
						Timeout:   defaultOpt.DefaultTimeout,
						LocalAddr: getLocalAddr(""),
					}
					return dialer.DialContext(ctx, network, addr)
				}),
			}
			// tcp url 连接
			rawURL := "redis://" + address
			c, err := redigo.DialURL(rawURL, dialOpts...)
			if err != nil {
				return handleNoAuthErr(err, rawURL)
			}
			return c, nil
		},
		TestOnBorrow: func(c redigo.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
		Wait: defaultOpt.IsWait,
	}
	redisPool[key] = pool
	return pool
}

func handleNoAuthErr(err error, rawURL string) (redigo.Conn, error) {
	/*
		当redis-server没有配置密码而client使用密码去连接时会失败，server返回错误：
		ERR Client sent AUTH, but no password is set
		当接收到这个错误，客户端不使用密码进行重连

		这个feature最有用的地方在于可以支持redis在线改密码：
		- 运维删除redis-server密码（重启codis twemproxy等）
		- 客户端连接报错，此时不使用密码进行重连
		- 客户端定期reload配置中心的密码，最终所有client都同步到新密码
		- 运维把redis-server密码改成新密码（客户端已经提前同步到了），并重启
		- 客户端使用新密码成功连接

		整个过程可以做到比较平滑，只有少量报错没有额外的不可用时长
	*/
	if strings.Contains(err.Error(), "but no password is set") {
		return redigo.DialURL(
			rawURL,
			redigo.DialWriteTimeout(defaultOpt.DefaultTimeout),
			redigo.DialReadTimeout(defaultOpt.DefaultTimeout),
			redigo.DialConnectTimeout(defaultOpt.DefaultTimeout),
		)
	}
	return nil, err
}

func getLocalAddr(addrStr string) net.Addr {
	if addrStr == "" {
		return nil
	}
	addressList := strings.Split(addrStr, ":")
	if len(addressList) != 2 {
		return nil
	}

	ip := types.StringToBytes(addressList[0])
	port := 0
	if addressList[1] != "" {
		var err error
		port, err = strconv.Atoi(addressList[1])
		if err != nil {
			port = 0
		}
	}
	return &net.TCPAddr{IP: ip, Port: port}
}
