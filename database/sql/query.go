package sql

import (
	"context"
	"fmt"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/log"
	"github.com/horm-database/common/proto"
	"github.com/horm-database/common/proto/plugin"
	"github.com/horm-database/common/proto/sql"
	"github.com/horm-database/common/util"
	"github.com/horm-database/orm/database/sql/client"
	ol "github.com/horm-database/orm/log"
	"github.com/horm-database/orm/obj"
)

type Query struct {
	OP            string
	Table, Alias  string
	Where, Having map[string]interface{}
	Column        []string
	Group         []string
	Order         []string
	Join          []*sql.Join
	Data          map[string]interface{}
	Datas         []map[string]interface{}
	Page          int
	Size          int
	From          uint64
	SQL           string
	CountSQL      string
	Params        []interface{}

	// 建表用
	Shard       []string
	IfNotExists bool

	DB        *obj.TblDB
	Addr      *util.DBAddress
	TblTable  *obj.TblTable
	TransInfo *obj.TransInfo
	TimeLog   *log.TimeLog

	client client.Client
}

func (q *Query) SetParams(req *plugin.Request,
	property *obj.Property, addr *util.DBAddress, transInfo *obj.TransInfo) error {
	q.OP = req.Op
	q.Table = req.Tables[0]
	q.Alias = property.Alias
	q.Where = req.Where
	q.Having = req.Having
	q.Column = req.Column
	q.Group = req.Group
	q.Order = req.Order
	q.Data = req.Data
	q.Datas = req.Datas
	q.Page = req.Page
	q.Size = req.Size
	q.From = req.From
	q.SQL = req.Query
	q.Params = req.Args
	q.TransInfo = transInfo

	q.DB = property.DB
	q.Addr = addr
	q.TblTable = property.Table

	q.Shard = req.Tables
	q.Join = req.Join

	q.IfNotExists, _ = req.Params.GetBool("if_not_exists")
	return nil
}

// Query sql 查询
func (q *Query) Query(ctx context.Context) (interface{}, *proto.Detail, bool, error) {
	q.TimeLog = ol.NewTimeLog(ctx, q.Addr)

	if q.OP == consts.OpCreate { //表创建
		result, err := q.createTable(ctx, q.Shard, q.TblTable.Definition, q.IfNotExists)
		return result, nil, false, err
	}

	statement := &Statement{dbType: q.Addr.Type, op: q.OP}
	statement.SetColumn(q.Column)
	statement.SetTable(q.Table, q.Alias)
	statement.Join(q.Join)

	if len(q.Data) > 0 {
		q.Datas = append(q.Datas, q.Data)
	}

	if q.OP == consts.OpInsert {
		if q.Addr.Type == consts.DBTypeClickHouse {
			_, _, err := q.InsertToCK(ctx, "", false, 0, q.Table, q.Datas)
			return proto.ModRet{}, nil, false, err
		} else {
			statement.SetMaps(q.Datas)
		}
	} else if q.OP == consts.OpReplace {
		statement.SetMaps(q.Datas)
	} else if q.OP == consts.OpUpdate {
		statement.UpdateMap(q.Data)
	}

	statement.Where(q.Addr.Type, q.Where)
	statement.Group(q.Group)
	statement.Having(q.Addr.Type, q.Having)
	statement.Order(q.Order)

	if q.OP == consts.OpInsert || q.OP == consts.OpReplace || q.OP == consts.OpUpdate || q.OP == consts.OpDelete {
		if q.SQL == "" {
			q.SQL = statement.GetSQL()
			q.Params = statement.params
		}

		rowsAffected, lastInsertID, err := q.execute(ctx)
		if err != nil {
			return nil, nil, false, err
		}

		result := proto.ModRet{
			RowAffected: rowsAffected,
			ID:          proto.ID(fmt.Sprint(lastInsertID)),
		}

		return &result, nil, false, nil
	} else if q.OP == consts.OpFind {
		if q.SQL == "" {
			q.Size = 1
			statement.limit = 1
			statement.offset = q.From
			q.SQL = statement.GetSQL()
			q.Params = statement.params
		}

		dest, err := q.Find(ctx)
		if err != nil {
			return nil, nil, false, err
		}

		if len(dest) == 0 {
			return nil, nil, true, nil
		}

		return dest, nil, false, nil
	} else if q.OP == consts.OpFindAll {
		var detail *proto.Detail

		if q.Page > 0 {
			q.CountSQL = statement.CountSQL()
			q.Params = statement.params

			detail = &proto.Detail{Page: q.Page, Size: q.Size}
			total, err := q.Count(ctx)
			if err != nil {
				return nil, nil, false, err
			}

			detail.Total = total
			detail.TotalPage = util.CalcTotalPage(total, q.Size)
			q.From = uint64((q.Page - 1) * q.Size)
		}

		statement.limit = q.Size
		statement.offset = q.From

		if q.SQL == "" {
			q.SQL = statement.GetSQL()
			q.Params = statement.params
		}

		dest, err := q.FindAll(ctx)
		if err != nil {
			return nil, nil, false, err
		}

		if len(dest) == 0 {
			return nil, detail, true, nil
		}

		return dest, detail, false, nil
	} else if q.OP == consts.OpCount {
		if q.SQL == "" {
			q.CountSQL = statement.GetSQL()
			q.Params = statement.params
			q.SQL = q.CountSQL
		} else {
			q.CountSQL = q.SQL
		}

		total, err := q.Count(ctx)
		if err != nil {
			return nil, nil, false, err
		}

		return total, nil, false, nil
	}

	return nil, nil, false, nil
}

// GetQueryStatement 获取 sql 语句
func (q *Query) GetQueryStatement() string {
	return q.SQL
}
