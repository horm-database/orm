package database

import (
	"context"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/proto"
	"github.com/horm-database/common/proto/filter"
	"github.com/horm-database/common/util"
	"github.com/horm-database/orm/database/elastic"
	"github.com/horm-database/orm/database/redis"
	"github.com/horm-database/orm/database/sql"
	"github.com/horm-database/orm/obj"
)

// Query 数据库开放接口
type Query interface {
	Query(ctx context.Context) (interface{}, *proto.Detail, bool, error)                                     // 查询
	SetParams(req *filter.Request, prop *obj.Property, addr *util.DBAddress, transInfo *obj.TransInfo) error // 设置查询参数
}

func QueryResult(ctx context.Context, req *filter.Request, node *obj.Tree,
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
		return nil, nil, false, errs.Newf(errs.RetNotFindQueryImp,
			"not find database %s`s query implementation, type=[%d]", node.GetDB().Name, addr.Type)
	}

	err := query.SetParams(req, node.Property, addr, transInfo)
	if err != nil {
		return nil, nil, false, err
	}

	return query.Query(ctx)
}
