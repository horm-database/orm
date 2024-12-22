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

package database

import (
	"context"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/proto"
	"github.com/horm-database/common/proto/plugin"
	"github.com/horm-database/common/util"
	"github.com/horm-database/orm/database/elastic"
	"github.com/horm-database/orm/database/redis"
	"github.com/horm-database/orm/database/sql"
	"github.com/horm-database/orm/obj"
)

// Query 数据库开放接口
type Query interface {
	Query(ctx context.Context) (interface{}, *proto.Detail, bool, error)                                     // 查询
	SetParams(req *plugin.Request, prop *obj.Property, addr *util.DBAddress, transInfo *obj.TransInfo) error // 设置查询参数
	GetQueryStatement() string                                                                               // 获取查询语句
}

func QueryResult(ctx context.Context, req *plugin.Request, node *obj.Tree,
	addr *util.DBAddress, transInfo *obj.TransInfo) (interface{}, *proto.Detail, bool, error) {
	var query Query

	switch node.GetDB().Addr.Type {
	case consts.DBTypeMySQL, consts.DBTypePostgreSQL, consts.DBTypeClickHouse,
		consts.DBTypeOracle, consts.DBTypeDB2, consts.DBTypeSQLite:
		query = &sql.Query{}
	case consts.DBTypeElastic:
		query = &elastic.Query{}
	case consts.DBTypeRedis:
		query = &redis.Redis{}
	}

	if query == nil {
		return nil, nil, false, errs.Newf(errs.ErrQueryNotImp,
			"not find database %s`s query implementation, type=[%d]", node.GetDB().Name, addr.Type)
	}

	err := query.SetParams(req, node.Property, addr, transInfo)
	if err != nil {
		return nil, nil, false, err
	}

	return query.Query(ctx)
}
