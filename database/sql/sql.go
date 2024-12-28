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

package sql

import (
	"context"
	"database/sql"
	"time"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/structs"
	"github.com/horm-database/orm/database/sql/client"
)

// Find 查询符合要求的一条数据，返回结果为 map[string]string
func (q *Query) Find(ctx context.Context) (result map[string]interface{}, err error) {
	next := func(rows *sql.Rows) error {
		if q.Addr.Type == consts.DBTypeClickHouse {
			result, err = q.nextRowCK(rows)
		} else {
			result, err = q.nextRow(rows)
		}

		return err
	}

	err = q.query(ctx, next, q.SQL, q.Params...)
	return
}

// FindAll 查询符合要求的所有数据，返回 []map[string]string 格式数据
func (q *Query) FindAll(ctx context.Context) (result []map[string]interface{}, err error) {
	next := func(rows *sql.Rows) (err error) {
		var item map[string]interface{}

		if q.Addr.Type == consts.DBTypeClickHouse {
			item, err = q.nextRowCK(rows)
		} else {
			item, err = q.nextRow(rows)
		}

		if err != nil {
			return err
		}

		result = append(result, item)

		return nil
	}

	err = q.query(ctx, next, q.SQL, q.Params...)
	return
}

// Count 统计总数
func (q *Query) Count(ctx context.Context) (uint64, error) {
	var count uint64

	next := func(rows *sql.Rows) error {
		return rows.Scan(&count)
	}

	err := q.query(ctx, next, q.CountSQL, q.Params...)
	return count, err
}

// Transaction 事务函数
func (q *Query) Transaction(ctx context.Context, fn func(c *Query) error) error {
	err := q.client.BeginTx(ctx)
	if err != nil {
		return err
	}

	err = fn(q)

	return q.client.FinishTx(err)
}

// execute 原生操作支持，支持自定义sql语句，比如delete，update,insert,replace
func (q *Query) execute(ctx context.Context) (int64, int64, error) {
	err := q.initClient(ctx)
	if err != nil {
		return 0, 0, err
	}

	ret, err := q.client.Exec(ctx, q.SQL, q.Params...)
	if err != nil {
		return 0, 0, q.logError(err, q.SQL, q.Params)
	}

	rowsAffected, err := ret.RowsAffected()
	if err != nil {
		db, _ := consts.DBTypeDesc[q.Addr.Type]
		q.TimeLog.Errorf(errs.ErrAffectResult,
			"%s query get RowsAffected Error: [%v], sql=[%s], params=[%v]", db, err, q.SQL, q.Params)
	}

	lastInsertID, err := ret.LastInsertId()
	if err != nil {
		db, _ := consts.DBTypeDesc[q.Addr.Type]
		q.TimeLog.Errorf(errs.ErrAffectResult,
			"%s query get LastInsertId Error: [%v], sql=[%s], params=[%v]", db, err, q.SQL, q.Params)
	}

	q.logInfo(q.SQL, q.Params)

	return rowsAffected, lastInsertID, nil
}

func (q *Query) query(ctx context.Context, next func(*sql.Rows) error, sql string, args ...interface{}) (err error) {
	err = q.initClient(ctx)
	if err != nil {
		return err
	}

	err = q.client.Query(ctx, next, sql, args...)
	if err != nil {
		return q.logError(err, sql, args)
	}

	q.logInfo(sql, args)
	return nil
}

// 初始化事务
func (q *Query) initClient(ctx context.Context) (err error) {
	if q.TransInfo != nil {
		txClient := q.TransInfo.GetTxClient(q.Addr.Conn.DSN)
		if txClient == nil {
			q.client, err = client.NewClient(q.Addr)
			if err != nil {
				return
			}

			err = q.client.BeginTx(ctx)
			if err != nil {
				return
			}

			q.TransInfo.SetTxClient(q.Addr.Conn.DSN, q.client)
		} else {
			q.client = txClient
		}
	} else {
		q.client, err = client.NewClient(q.Addr)
	}

	return nil
}

func (q *Query) nextRow(rows *sql.Rows) (map[string]interface{}, error) {
	result := map[string]interface{}{}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var row = make([]interface{}, len(columns))

	for i := range columns {
		nullable, _ := colTypes[i].Nullable()
		typeName := colTypes[i].DatabaseTypeName()
		typ := MySQLTypeMap[typeName]

		switch typ {
		case structs.TypeInt:
			if nullable {
				recv := NullInt{}
				row[i] = &recv
			} else {
				var recv int
				row[i] = &recv
			}
		case structs.TypeInt8:
			if nullable {
				recv := NullInt{}
				row[i] = &recv
			} else {
				var recv int8
				row[i] = &recv
			}
		case structs.TypeInt16:
			if nullable {
				recv := NullInt{}
				row[i] = &recv
			} else {
				var recv int16
				row[i] = &recv
			}
		case structs.TypeInt32:
			if nullable {
				recv := NullInt{}
				row[i] = &recv
			} else {
				var recv int32
				row[i] = &recv
			}
		case structs.TypeInt64:
			if nullable {
				recv := NullInt{}
				row[i] = &recv
			} else {
				var recv int64
				row[i] = &recv
			}
		case structs.TypeUint:
			if nullable {
				recv := NullUint{}
				row[i] = &recv
			} else {
				var recv uint
				row[i] = &recv
			}
		case structs.TypeUint8:
			if nullable {
				recv := NullUint{}
				row[i] = &recv
			} else {
				var recv uint8
				row[i] = &recv
			}
		case structs.TypeUint16:
			if nullable {
				recv := NullUint{}
				row[i] = &recv
			} else {
				var recv uint16
				row[i] = &recv
			}
		case structs.TypeUint32:
			if nullable {
				recv := NullUint{}
				row[i] = &recv
			} else {
				var recv uint32
				row[i] = &recv
			}
		case structs.TypeUint64:
			if nullable {
				recv := NullUint{}
				row[i] = &recv
			} else {
				var recv uint64
				row[i] = &recv
			}
		case structs.TypeFloat:
			if nullable {
				recv := NullFloat{}
				row[i] = &recv
			} else {
				var recv float32
				row[i] = &recv
			}
		case structs.TypeDouble:
			if nullable {
				recv := NullFloat{}
				row[i] = &recv
			} else {
				var recv float64
				row[i] = &recv
			}
		case structs.TypeString:
			if nullable {
				recv := NullString{}
				row[i] = &recv
			} else {
				var recv string
				row[i] = &recv
			}
		case structs.TypeTime:
			if nullable {
				recv := NullTime{}
				row[i] = &recv
			} else {
				var recv time.Time
				row[i] = &recv
			}

		case structs.TypeBytes:
			var recv []byte
			row[i] = &recv
		case structs.TypeJSON:
			var recv NullString
			row[i] = &recv
		default:
			var recv interface{}
			row[i] = &recv
		}
	}

	err = rows.Scan(row...)
	if row == nil || err != nil {
		return nil, err
	}

	for i, column := range columns {
		if row[i] == nil {
			result[column] = nil
		} else {
			switch v := row[i].(type) {
			case *NullInt:
				if v.IsNull {
					result[column] = nil
				} else {
					result[column] = v.Int
				}
			case *NullFloat:
				if v.IsNull {
					result[column] = nil
				} else {
					result[column] = v.Float
				}
			case *NullBool:
				if v.IsNull {
					result[column] = nil
				} else {
					result[column] = v.Bool
				}
			case *NullString:
				if v.IsNull {
					result[column] = nil
				} else {
					result[column] = v.String
				}
			case *NullTime:
				if v.IsNull {
					result[column] = nil
				} else {
					result[column] = v.Time
				}
			default:
				result[column] = row[i]
			}
		}
	}

	return result, nil
}
