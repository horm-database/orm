package orm

import (
	"context"
	"strings"

	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/log"
	"github.com/horm-database/common/proto"
	"github.com/horm-database/common/proto/filter"
	"github.com/horm-database/common/types"
	"github.com/horm-database/common/util"
	"github.com/horm-database/orm/database"
	"github.com/horm-database/orm/obj"
)

// 节点查询
func query(ctx context.Context, node *obj.Tree) (result interface{}, detail *proto.Detail, isNil bool, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errs.Newf(errs.RetPanic, "query panic: %v", e)
			log.Errorf(ctx, errs.RetPanic, "query panic: %v", e)
			return
		}
	}()

	realNode := node.GetReal()
	op := realNode.GetOp()
	unit := realNode.GetUnit()

	request := &filter.Request{}

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

	request.Data = unit.Data
	request.Datas = unit.Datas

	request.Type = unit.Type
	request.Scroll = unit.Scroll

	request.Prefix = unit.Prefix
	request.Key = unit.Key
	request.Args = unit.Args
	request.Bytes = unit.Bytes

	request.Params = unit.Params
	request.Query = unit.Query

	// 走 db 查询
	return database.QueryResult(ctx, request, realNode, realNode.GetDB().Addr, node.TransInfo)
}

// 初始化分析树
func initTree(node *obj.Tree, unit *proto.Unit, db *obj.TblDB) error {
	if db == nil {
		return errs.Newf(errs.RetNotFindName, "not find db, data name is %s", unit.Name)
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
		if len(msg) > 20000 { //返回太长，截断
			node.Error = errs.SetErrorMsg(node.Error, msg[0:20000])
		}

		return false, nil, node.Error
	}

	if hasDetail(node.Detail) {
		result := proto.PageResult{Detail: node.Detail}
		if node.Result == nil {
			result.Data = []interface{}{}
		} else {
			result.Data, _ = types.InterfaceToArray(node.Result)
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
