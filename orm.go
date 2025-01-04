// Copyright (c) 2024 The horm-database Authors. All rights reserved.
// This file Author:  CaoHao <18500482693@163.com> .
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

// Package orm 本地 horm， 他不会访问 server 代理层，而是直接访问数据库
package orm

import (
	"context"
	"fmt"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/types"
	"github.com/horm-database/common/util"
	"github.com/horm-database/go-horm/horm"
	"github.com/horm-database/orm/obj"
)

// ORM 统一接入协议 本地 orm 的实现
type ORM struct {
	db      *obj.TblDB
	query   *horm.Query
	initErr error
}

// NewORM 创建 local orm 客户端
func NewORM(dbName string) *ORM {
	c := ORM{
		db:    &obj.TblDB{},
		query: horm.NewQuery(""),
	}

	dbConf, err := horm.GetDBConfig(dbName)
	if err != nil {
		c.initErr = err
		return &c
	}

	c.db.Name = dbConf.Name

	dbType, ok := consts.DBTypeMap[dbConf.Type]
	if !ok {
		c.initErr = errs.Newf(errs.ErrDBTypeInvalid, "db config type invalid: %s", dbConf.Type)
		return &c
	}

	if dbConf.Network == "" {
		dbConf.Network = "TCP"
	}

	if dbConf.WarnTimeout == 0 {
		dbConf.WarnTimeout = 200
	}

	c.db.Addr = &util.DBAddress{
		Type:    dbType,
		Version: dbConf.Version,
		Network: dbConf.Network,
		Address: dbConf.Address,

		WriteTimeout: dbConf.WriteTimeout,
		ReadTimeout:  dbConf.ReadTimeout,
		WarnTimeout:  dbConf.WarnTimeout,
		OmitError:    dbConf.OmitError,
		Debug:        dbConf.Debug,
	}

	err = util.ParseConnFromAddress(c.db.Addr)
	if err != nil {
		c.initErr = errs.Newf(errs.ErrDBAddressParse, "db address [%s] parse error: %v", dbConf.Address, err)
		return &c
	}

	return &c
}

// Exec 单执行单元 result 接收结果的指针
func (o *ORM) Exec(ctx context.Context, retReceiver ...interface{}) (isNil bool, err error) {
	if o.initErr != nil {
		return false, o.initErr
	}

	defer func() {
		if e := recover(); e != nil {
			err = errs.New(errs.ErrPanic, fmt.Sprintf("%v", e))
		}
		o.query.Reset()
	}()

	if o.query.Unit.Size < 0 {
		o.query.Unit.Size = 0
	}

	tree := &obj.Tree{}
	err = initTree(tree, o.query.Unit, o.db)
	if err != nil {
		return false, err
	}

	tree.Result, tree.Detail, tree.IsNil, tree.Error = query(ctx, tree)

	var ret interface{}
	isNil, ret, err = ParseResult(tree)
	if err != nil {
		return isNil, err
	}

	err = o.query.GetCoder().Decode(o.query.ResultType, ret, retReceiver)
	if err != nil {
		return false, errs.Newf(errs.ErrClientDecode,
			"[request_id=%d] %v, result=[%s]", o.query.RequestID, err, types.ToString(ret))
	}

	return isNil, nil
}
