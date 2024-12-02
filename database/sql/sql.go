package sql

import (
	"context"
	"database/sql"
	"time"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/orm/database/sql/client"
)

// Find 查询符合要求的一条数据，返回结果为 map[string]string
func (q *Query) Find(ctx context.Context, s *Statement,
	querySQL string, params []interface{}) (result map[string]interface{}, err error) {
	next := func(rows *sql.Rows) error {
		if q.Addr.Type == consts.DBTypeClickHouse {
			result, err = q.nextRowCK(rows)
		} else {
			result, err = q.nextRow(rows)
		}

		return err
	}

	if querySQL == "" {
		querySQL = FindSQL(s)
		params = s.params
	}

	err = q.query(ctx, next, querySQL, params...)
	return
}

// FindAll 查询符合要求的所有数据，返回 []map[string]string 格式数据
func (q *Query) FindAll(ctx context.Context, s *Statement,
	querySQL string, params []interface{}) (result []map[string]interface{}, err error) {
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

	if querySQL == "" {
		querySQL = FindSQL(s)
		params = s.params
	}

	err = q.query(ctx, next, querySQL, params...)
	return
}

// Count 统计总数
func (q *Query) Count(ctx context.Context, s *Statement) (uint64, error) {
	querySQL := CountSQL(s)

	var count uint64

	next := func(rows *sql.Rows) error {
		return rows.Scan(&count)
	}

	err := q.query(ctx, next, querySQL, s.params...)
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
func (q *Query) execute(ctx context.Context, querySQL string, args ...interface{}) (int64, int64, error) {
	err := q.initClient(ctx)
	if err != nil {
		return 0, 0, err
	}

	ret, err := q.client.Exec(ctx, querySQL, args...)
	if err != nil {
		return 0, 0, q.logError(err, querySQL, args)
	}

	rowsAffected, err := ret.RowsAffected()

	if err != nil {
		db, _ := consts.DBTypeDesc[q.Addr.Type]
		q.TimeLog.Errorf(errs.RetAffectResultFailed,
			"%s query get RowsAffected Error: [%v], sql=[%s], params=[%v]", db, err, querySQL, args)
	}

	lastInsertID, err := ret.LastInsertId()
	if err != nil {
		db, _ := consts.DBTypeDesc[q.Addr.Type]
		q.TimeLog.Errorf(errs.RetAffectResultFailed,
			"%s query get LastInsertId Error: [%v], sql=[%s], params=[%v]", db, err, querySQL, args)
	}

	q.logInfo(querySQL, args)

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
		typ := MySQLTypeMap[colTypes[i].DatabaseTypeName()]

		switch typ {
		case TypeInt:
			if nullable {
				recv := NullInt{}
				row[i] = &recv
			} else {
				var recv int
				row[i] = &recv
			}
		case TypeInt8:
			if nullable {
				recv := NullInt{}
				row[i] = &recv
			} else {
				var recv int8
				row[i] = &recv
			}
		case TypeInt16:
			if nullable {
				recv := NullInt{}
				row[i] = &recv
			} else {
				var recv int16
				row[i] = &recv
			}
		case TypeInt32:
			if nullable {
				recv := NullInt{}
				row[i] = &recv
			} else {
				var recv int32
				row[i] = &recv
			}
		case TypeInt64:
			if nullable {
				recv := NullInt{}
				row[i] = &recv
			} else {
				var recv int64
				row[i] = &recv
			}
		case TypeFloat32:
			if nullable {
				recv := NullFloat{}
				row[i] = &recv
			} else {
				var recv float32
				row[i] = &recv
			}
		case TypeFloat64:
			if nullable {
				recv := NullFloat{}
				row[i] = &recv
			} else {
				var recv float64
				row[i] = &recv
			}
		case TypeString:
			if nullable {
				recv := NullString{}
				row[i] = &recv
			} else {
				var recv string
				row[i] = &recv
			}
		case TypeTime:
			if nullable {
				recv := NullTime{}
				row[i] = &recv
			} else {
				var recv time.Time
				row[i] = &recv
			}
		case TypeBytes:
			var recv []byte
			row[i] = &recv
		case TypeJSON:
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
