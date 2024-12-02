package sql

import (
	"context"
	"fmt"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/log"
	"github.com/horm-database/common/proto"
	"github.com/horm-database/common/proto/filter"
	"github.com/horm-database/common/types"
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
	Join          []*Join
	Data          map[string]interface{}
	Datas         []map[string]interface{}
	Page          int
	Size          int
	From          uint64
	SQL           string
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

// Join MySQL 表 JOIN
type Join struct {
	Type  string            `json:"type,omitempty"`
	Table string            `json:"table,omitempty"`
	Using []string          `json:"using,omitempty"`
	On    map[string]string `json:"on,omitempty"`
}

func (q *Query) SetParams(req *filter.Request,
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

	joins, _, err := req.Params.GetMapArray("join")
	if err != nil {
		return errs.Newf(errs.RetDBReqParams, "sql get params join error: %v", err)
	}

	if len(joins) > 0 {
		for _, v := range joins {
			join := Join{}
			join.Type, _ = types.GetString(v, "type")
			join.Table, _ = types.GetString(v, "table")
			join.Using, _, err = types.GetStringArray(v, "using")
			if err != nil {
				return errs.Newf(errs.RetDBReqParams, "sql get params join using error: %v", err)
			}

			on, _, err := types.GetMap(v, "on")
			if err != nil {
				return errs.Newf(errs.RetDBReqParams, "sql get params join on error: %v", err)
			}

			if len(on) > 0 {
				join.On = map[string]string{}
				for onKey, onVal := range on {
					join.On[onKey] = types.InterfaceToString(onVal)
				}
			}

			q.Join = append(q.Join, &join)
		}
	}

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

	statement := &Statement{dbType: q.Addr.Type}
	statement.SetColumn(q.Column)
	statement.SetTable(q.Table, q.Alias)
	statement.Join(q.Join)

	if len(q.Data) > 0 {
		q.Datas = append(q.Datas, q.Data)
	}

	if q.OP == consts.OpInsert {
		if q.Addr.Type == consts.DBTypeClickHouse {
			_, _, err := q.InsertToCK(ctx, "", false, 0, q.Table, q.Datas)
			return proto.ModResult{}, nil, false, err
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
		var querySql string
		var params []interface{}

		if q.SQL == "" {
			switch q.OP {
			case consts.OpInsert:
				querySql = InsertSQL(statement)
			case consts.OpReplace:
				querySql = ReplaceSQL(statement)
			case consts.OpUpdate:
				querySql = UpdateSQL(statement)
			case consts.OpDelete:
				querySql = DeleteSQL(statement)
			}

			params = statement.params
		} else {
			querySql = q.SQL
			params = q.Params
		}

		rowsAffected, lastInsertID, err := q.execute(ctx, querySql, params...)
		if err != nil {
			return nil, nil, false, err
		}

		result := proto.ModResult{
			RowAffected: rowsAffected,
			ID:          proto.ID(fmt.Sprint(lastInsertID)),
		}

		return &result, nil, false, nil
	} else if q.OP == consts.OpFind {
		q.Page = 0
		q.Size = 1

		dest, err := q.Find(ctx, statement, q.SQL, q.Params)
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
			detail = &proto.Detail{Page: q.Page, Size: q.Size}
			total, err := q.Count(ctx, statement)
			if err != nil {
				return nil, nil, false, err
			}

			detail.Total = total
			detail.TotalPage = util.CalcTotalPage(total, q.Size)
			q.From = uint64((q.Page - 1) * q.Size)
		}

		statement.limit = q.Size
		statement.offset = q.From

		dest, err := q.FindAll(ctx, statement, q.SQL, q.Params)
		if err != nil {
			return nil, nil, false, err
		}

		if len(dest) == 0 {
			return nil, detail, true, nil
		}

		return dest, detail, false, nil
	}

	return nil, nil, false, nil
}
