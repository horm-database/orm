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
package elastic

import (
	"context"
	"fmt"
	"strings"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/log"
	"github.com/horm-database/common/proto"
	"github.com/horm-database/common/proto/plugin"
	"github.com/horm-database/common/types"
	"github.com/horm-database/common/util"
	ol "github.com/horm-database/orm/log"
	"github.com/horm-database/orm/obj"
)

const (
	ElasticV6 = "v6" // elastic v6 版本
	ElasticV7 = "v7" // elastic v7 版本
)

type Query struct {
	OP                 string
	Path               string
	Index              []string
	Type               string
	Column             []string
	Where              map[string]interface{}
	Data               map[string]interface{}
	Datas              []map[string]interface{}
	Page               int
	Size               int
	From               uint64
	Order              []string
	Refresh            string
	Routing            string
	Script, ScriptType string
	Source             string
	Scroll             *proto.Scroll
	HighLight          *HighLight

	// Params
	Params types.Map

	// 日志
	logStep         int8
	reqLog, respLog string

	Addr    *util.DBAddress
	TimeLog *log.TimeLog

	QL string
}

func (q *Query) SetParams(req *plugin.Request,
	prop *obj.Property, addr *util.DBAddress, transInfo *obj.TransInfo) error {
	q.OP = req.Op
	q.Index = req.Tables
	q.Type = req.Type
	q.Column = req.Column
	q.Where = req.Where
	q.Data = req.Data
	q.Datas = req.Datas
	q.Order = req.Order
	q.Page = req.Page
	q.Size = req.Size
	q.From = req.From
	q.Source = req.Query
	q.Scroll = req.Scroll

	q.Path = prop.Path
	q.Addr = addr

	refresh, _ := req.Params.GetBool("refresh")
	q.Refresh = "false"
	if refresh {
		q.Refresh = "true"
	}

	q.Routing, _ = req.Params.GetString("routing")

	q.HighLight = getHighLightParam(req.Params)

	q.Params = req.Params
	return nil
}

// Query 从 es 获取数据
func (q *Query) Query(ctx context.Context) (interface{}, *proto.Detail, bool, error) {
	q.TimeLog = ol.NewTimeLog(ctx, q.Addr)

	var esIds []string

	if len(q.Where) > 0 {
		id, ok := q.Where["_id"]
		if ok && id != nil {
			switch val := id.(type) {
			case []interface{}:
				for _, v := range val {
					esIds = append(esIds, types.InterfaceToString(v))
				}
			default:
				esIds = append(esIds, types.InterfaceToString(id))
			}
		}
	}

	switch q.OP {
	case consts.OpInsert, consts.OpReplace:
		if len(q.Datas) == 0 {
			if len(esIds) == 0 {
				return q.insert(ctx, q.OP, "")
			} else {
				return q.insert(ctx, q.OP, esIds[0])
			}
		} else {
			return q.bulkInsert(ctx, q.OP)
		}
	case consts.OpUpdate:
		if len(esIds) > 0 {
			return q.updateByID(ctx, esIds[0])
		} else {
			return q.updateByQuery(ctx)
		}
	case consts.OpDelete:
		if len(esIds) > 0 {
			return q.deleteByID(ctx, esIds[0])
		} else {
			return q.deleteByQuery(ctx)
		}
	case consts.OpFind:
		q.Page = 0
		q.Size = 1
		ret, _, isNil, err := q.search(ctx)
		if err != nil || isNil {
			return nil, nil, isNil, err
		}
		return ret[0], nil, false, nil
	case consts.OpFindAll:
		if q.Scroll != nil {
			if q.Scroll.ID != "" {
				return q.scrollByScrollID(ctx)
			} else if q.Scroll.Info != "" {
				return q.scrollByQuery(ctx)
			}
		} else {
			return q.search(ctx)
		}
	}

	return nil, nil, false, nil
}

// GetQueryStatement 获取 es 的查询语句
func (q *Query) GetQueryStatement() string {
	return q.QL
}

// Printf 实现 elastic Logger 接口
func (q *Query) Printf(format string, v ...interface{}) {
	if q.logStep == 0 {
		q.logStep++
		q.QL = strings.Replace(fmt.Sprintf(format, v...), "\n", "", -1)
	} else {
		q.QL = strings.Replace(fmt.Sprintf(format, v...), "\n", "", -1)
	}
}
