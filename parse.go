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

package orm

import (
	"context"
	"strings"

	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/log"
	"github.com/horm-database/common/proto"
	"github.com/horm-database/common/proto/plugin"
	"github.com/horm-database/common/types"
	"github.com/horm-database/common/util"
	"github.com/horm-database/orm/database"
	"github.com/horm-database/orm/obj"
)

// 节点查询
func query(ctx context.Context, node *obj.Tree) (result interface{}, detail *proto.Detail, isNil bool, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errs.Newf(errs.ErrPanic, "query panic: %v", e)
			log.Errorf(ctx, errs.ErrPanic, "query panic: %v", e)
			return
		}
	}()

	realNode := node.GetReal()
	op := realNode.GetOp()
	unit := realNode.GetUnit()

	request := &plugin.Request{}

	request.Op = op
	request.Tables = realNode.Tables()
	request.Where = unit.Where
	request.Column = unit.Column
	request.Group = unit.Group
	request.Having = unit.Having
	request.Order = unit.Order
	request.Page = unit.Page
	request.Size = unit.Size
	request.From = unit.From

	request.Val = unit.Val
	request.Data = unit.Data
	request.Datas = unit.Datas

	request.Join = unit.Join

	request.Type = unit.Type
	request.Scroll = unit.Scroll

	request.Prefix = unit.Prefix
	request.Key = unit.Key
	request.Keys = unit.Keys
	request.Bytes = unit.Bytes

	request.Params = unit.Params

	request.Query = unit.Query
	request.Args = unit.Args

	// 走 db 查询
	return database.QueryResult(ctx, request, realNode, realNode.GetDB().Addr, node.TransInfo)
}

// 初始化分析树
func initTree(node *obj.Tree, unit *proto.Unit, db *obj.TblDB) error {
	if db == nil {
		return errs.Newf(errs.ErrNotFindName, "not find db, data name is %s", unit.Name)
	}

	node.Name = unit.Name
	node.Unit = unit

	property := obj.Property{}
	property.Op = strings.ToLower(unit.Op)
	property.Name, property.Alias = util.Alias(unit.Name)
	property.Path = unit.Name

	var tables []string

	if unit.Name != "" {
		tables = append(tables, unit.Name)
	} else if len(unit.Shard) > 0 {
		tables = unit.Shard
	}

	property.Tables = tables
	property.DB = db

	node.Property = &property
	return nil
}

// ParseResult 解析结果
func ParseResult(node *obj.Tree) (bool, interface{}, error) {
	if !node.IsSuccess() {
		msg := errs.Msg(node.Error)
		if len(msg) > 5000 { //返回太长，截断
			node.Error = errs.SetErrorMsg(node.Error, msg[0:5000])
		}

		return false, nil, node.Error
	}

	if hasDetail(node.Detail) {
		result := proto.PageResult{Detail: node.Detail}
		if node.Result == nil {
			result.Data = []interface{}{}
		} else {
			result.Data, _ = types.ToArray(node.Result)
		}
		return node.IsNil, result, nil
	}

	return node.IsNil, node.Result, nil
}

func hasDetail(detail *proto.Detail) bool {
	if detail == nil {
		return false
	}
	if detail.Size > 0 || detail.Scroll != nil || len(detail.Extras) > 0 {
		return true
	}
	return false
}
