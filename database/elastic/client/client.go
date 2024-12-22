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
	"net/http"
	"strings"
	"time"

	"github.com/horm-database/common/util"

	esv6 "github.com/olivere/elastic/v6"
	esv7 "github.com/olivere/elastic/v7"
)

// Client es database interface
type Client interface {
	Do(request *http.Request) (*http.Response, error)        // Do olivere elastic v7 Doer 要求实现的方法
	RoundTrip(request *http.Request) (*http.Response, error) // RoundTrip olivere elastic v6 http.Client要求实现的方法
}

// NewClientV6 快速实例化一个 elastic search 6.x API版本的 elastic.Client
func NewClientV6(isRead bool, addr *util.DBAddress, esClientOpts ...esv6.ClientOptionFunc) (*esv6.Client, error) {
	cli := &http.Client{
		Transport: newClient(isRead, addr),
	}

	esClientOpts = append(esClientOpts, esv6.SetHttpClient(cli))
	return esv6.NewSimpleClient(esClientOpts...)
}

// NewClientV7 快速实例化一个 elastic search 7.x API版本的 elastic.Client
func NewClientV7(isRead bool, addr *util.DBAddress, esClientOpts ...esv7.ClientOptionFunc) (*esv7.Client, error) {
	esClientOpts = append(esClientOpts, esv7.SetHttpClient(newClient(isRead, addr)))
	return esv7.NewSimpleClient(esClientOpts...)
}

// Client 接口的 elastic 实现
type client struct {
	address  string
	username string
	password string
	timeout  time.Duration
}

// 实例化 Client 版本
func newClient(isRead bool, addr *util.DBAddress) Client {
	username, password := getUserPassword(addr.Conn.Password)
	timeout := time.Duration(addr.WriteTimeout) * time.Millisecond
	if isRead {
		timeout = time.Duration(addr.ReadTimeout) * time.Millisecond
	}

	return &client{
		address:  addr.Conn.DSN,
		username: username,
		password: password,
		timeout:  timeout,
	}
}

// Do 实现 elastic.Doer 接口
func (c *client) Do(request *http.Request) (*http.Response, error) {
	if c.timeout > 0 {
		ctx, cancel := context.WithTimeout(request.Context(), c.timeout)
		defer cancel()
		request = request.WithContext(ctx)
	}

	request.Host = c.address
	request.URL.Host = c.address
	request.SetBasicAuth(c.username, c.password)

	return http.DefaultTransport.RoundTrip(request)
}

// RoundTrip 实现 http.RoundTripper 接口
func (c *client) RoundTrip(request *http.Request) (*http.Response, error) {
	return c.Do(request)
}

// 将鉴权字符串 s 拆分成 username 和 password。
// s 的格式是 username:password
func getUserPassword(s string) (username, password string) {
	index := strings.IndexByte(s, ':')
	if index == -1 {
		return
	}

	return s[:index], s[index+1:]
}
