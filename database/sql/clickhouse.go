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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/json"
	"github.com/horm-database/common/log"
	"github.com/horm-database/common/structs"
)

// InsertToCK clickhouse 批量插入，（必须通过事务生成批量语句，然后提交）
func (q *Query) InsertToCK(ctx context.Context, desc string, batch bool, retryTime int,
	table string, datas []map[string]interface{}) ([]map[string]interface{}, map[int]error, error) {
	if len(datas) == 0 {
		return nil, nil, nil
	}

	insertSQL, keys := q.getInsertSQL(table, datas[0])

	//记录错误条目
	errIndex := map[int]error{}

	fn := func(c *Query) error {
		stmt, err := c.client.Prepare(ctx, insertSQL)
		if err != nil {
			log.Errorf(ctx, errs.ErrClickhouseInsert,
				"%s clickhouse %s insert to table [%s] prepare error:[%v] sql=[%s]",
				retryDesc(retryTime), desc, table, err, insertSQL)
			return err
		}

		var lastErr error

		for k, data := range datas {
			err = c.insertItem(ctx, desc, stmt, table, keys, data)
			if err != nil {
				if !batch { //非异步批量插入，直接返回错误
					return err
				}

				errIndex[k] = err
				lastErr = err
			}
		}

		return lastErr
	}

	err := q.Transaction(ctx, fn)
	if err == nil {
		q.logInfo(insertSQL, []interface{}{fmt.Sprintf("%s insert data_num=%d", desc, len(datas))})
		return nil, nil, nil
	}

	//非异步批量插入直接返回
	if !batch {
		return nil, nil, q.logError(err, insertSQL, []interface{}{fmt.Sprintf("%s %s insert data_num=%d, datas=[%s]",
			retryDesc(retryTime), desc, len(datas), q.firstPartData(datas))})
	}

	allFailed := map[int]error{-1: err}

	//无个别异常记录 或 全都是异常记录，直接返回
	if len(errIndex) == 0 || len(errIndex) == len(datas) {
		return datas, allFailed, q.logError(err, insertSQL,
			[]interface{}{fmt.Sprintf("%s %s insert data_num=%d, datas=[%s]",
				retryDesc(retryTime), desc, len(datas), q.firstPartData(datas))})
	}

	//剔除个别异常记录之后，剩余部分正常数据重试
	retryDatas := []map[string]interface{}{}
	for k, data := range datas {
		if _, ok := errIndex[k]; !ok {
			retryDatas = append(retryDatas, data)
		}
	}

	retryFn := func(c *Query) error {
		stmt, err := c.client.Prepare(ctx, insertSQL)
		if err != nil {
			log.Errorf(ctx, errs.ErrClickhouseInsert,
				"%s clickhouse %s insert to table [%s] retry prepare error:[%v] sql=[%s]",
				retryDesc(retryTime), desc, table, err, insertSQL)
			return err
		}

		for _, data := range retryDatas {
			err = c.insertItem(ctx, desc, stmt, table, keys, data)
			if err != nil {
				return err
			}
		}

		return nil
	}

	err = q.Transaction(ctx, retryFn)
	if err != nil { //剩余部分数据重试失败
		return datas, allFailed, q.logError(err, insertSQL,
			[]interface{}{fmt.Sprintf("%s %s insert retry_data_num=%d, retry_data=[%s]",
				retryDesc(retryTime), desc, len(retryDatas), q.firstPartData(retryDatas))})
	}

	//剩余部分数据重试成功
	fails := []map[string]interface{}{}
	for k := range errIndex {
		fails = append(fails, datas[k])
	}

	return fails, errIndex, q.logError(errIndex[0], insertSQL,
		[]interface{}{fmt.Sprintf("%s %s insert retry_success=%d, failed_num=%d, failed_data=[%s]",
			retryDesc(retryTime), desc, len(retryDatas), len(errIndex), q.firstPartData(fails))})
}

// 事务执行单条插入
func (q *Query) insertItem(ctx context.Context, desc string,
	stmt *sql.Stmt, table string, keys []string, data map[string]interface{}) error {
	var params = []interface{}{}

	for _, key := range keys {
		params = append(params, data[key])
	}

	_, err := stmt.Exec(params...)

	if err != nil {
		log.Errorf(ctx, errs.ErrClickhouseInsert, "clickhouse %s insert item stmt.Exec error: [%v], "+
			"table=[%s], data=[%s]", desc, err, table, json.MarshalToString(data))
	}

	return err
}

// 获取 查询语句
func (q *Query) getInsertSQL(table string, attributes map[string]interface{}) (string, []string) {
	if len(attributes) == 0 {
		return "", nil
	}

	var i int
	var sqlBuilder = strings.Builder{}
	sqlBuilder.WriteString("INSERT INTO `")
	sqlBuilder.WriteString(table)
	sqlBuilder.WriteString("` (")

	keys := []string{}
	for key := range attributes {
		keys = append(keys, key)
		if i == 0 {
			sqlBuilder.WriteString(columnQuote(key))
		} else {
			sqlBuilder.WriteString(`,`)
			sqlBuilder.WriteString(columnQuote(key))
		}

		i++
	}

	sqlBuilder.WriteString(") values (")

	for k := range keys {
		if k == 0 {
			sqlBuilder.WriteString("?")
		} else {
			sqlBuilder.WriteString(",?")
		}
	}

	sqlBuilder.WriteString(") ")

	return sqlBuilder.String(), keys
}

func (q *Query) nextRowCK(rows *sql.Rows) (map[string]interface{}, error) {
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

	decimalS := map[int]float64{} //小数位数

	for i := range columns {
		var array bool
		var tuple bool
		var typ structs.Type

		nullable, _ := colTypes[i].Nullable()
		typName := colTypes[i].DatabaseTypeName()

		if typName[0:3] == "Arr" {
			array = true
			typName = strings.TrimLeft(typName, "Array(")
			typName = strings.TrimRight(typName, ")")
		} else if len(typName) >= 5 && typName[0:5] == "Tuple" {
			tuple = true
		}

		if nullable {
			typName = strings.TrimLeft(typName, "Nullable(")
			typName = strings.TrimRight(typName, ")")
		}

		if typName[0:3] == "Dec" { //Decimal
			typ = structs.TypeDouble
			typName = strings.TrimLeft(typName, "Decimal(")
			typName = strings.TrimRight(typName, ")")
			typNameArr := strings.Split(typName, ",")
			if len(typNameArr) == 2 {
				decimalTmp, _ := strconv.Atoi(strings.TrimSpace(typNameArr[1]))
				decimalS[i] = math.Pow(10, float64(decimalTmp))
			}
		} else if typName[0:3] == "Fix" { //FixedString
			typ = structs.TypeString
		} else if tuple { //Tuple
			typ = structs.TypeJSON
		} else {
			typ = ClickHouseTypeMap[typName]
		}

		switch typ {
		case structs.TypeInt:
			if nullable {
				if array {
					recv := []NullInt{}
					row[i] = &recv
				} else {
					recv := NullInt{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []int
					row[i] = &recv
				} else {
					var recv int
					row[i] = &recv
				}
			}
		case structs.TypeInt8:
			if nullable {
				if array {
					recv := []NullInt{}
					row[i] = &recv
				} else {
					recv := NullInt{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []int8
					row[i] = &recv
				} else {
					var recv int8
					row[i] = &recv
				}
			}
		case structs.TypeInt16:
			if nullable {
				if array {
					recv := []NullInt{}
					row[i] = &recv
				} else {
					recv := NullInt{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []int16
					row[i] = &recv
				} else {
					var recv int16
					row[i] = &recv
				}
			}
		case structs.TypeInt32:
			if nullable {
				if array {
					recv := []NullInt{}
					row[i] = &recv
				} else {
					recv := NullInt{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []int32
					row[i] = &recv
				} else {
					var recv int32
					row[i] = &recv
				}
			}
		case structs.TypeInt64:
			if nullable {
				if array {
					recv := []NullInt{}
					row[i] = &recv
				} else {
					recv := NullInt{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []int64
					row[i] = &recv
				} else {
					var recv int64
					row[i] = &recv
				}
			}
		case structs.TypeUint:
			if nullable {
				if array {
					recv := []NullUint{}
					row[i] = &recv
				} else {
					recv := NullUint{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []uint
					row[i] = &recv
				} else {
					var recv uint
					row[i] = &recv
				}
			}
		case structs.TypeUint8:
			if nullable {
				if array {
					recv := []NullUint{}
					row[i] = &recv
				} else {
					recv := NullUint{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []uint8
					row[i] = &recv
				} else {
					var recv uint8
					row[i] = &recv
				}
			}
		case structs.TypeUint16:
			if nullable {
				if array {
					recv := []NullUint{}
					row[i] = &recv
				} else {
					recv := NullUint{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []uint16
					row[i] = &recv
				} else {
					var recv uint16
					row[i] = &recv
				}
			}
		case structs.TypeUint32:
			if nullable {
				if array {
					recv := []NullUint{}
					row[i] = &recv
				} else {
					recv := NullUint{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []uint32
					row[i] = &recv
				} else {
					var recv uint32
					row[i] = &recv
				}
			}
		case structs.TypeUint64:
			if nullable {
				if array {
					recv := []NullUint{}
					row[i] = &recv
				} else {
					recv := NullUint{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []uint64
					row[i] = &recv
				} else {
					var recv uint64
					row[i] = &recv
				}
			}
		case structs.TypeFloat:
			if nullable {
				if array {
					recv := []NullFloat{}
					row[i] = &recv
				} else {
					recv := NullFloat{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []float32
					row[i] = &recv
				} else {
					var recv float32
					row[i] = &recv
				}
			}
		case structs.TypeDouble:
			if nullable {
				if array {
					recv := []NullFloat{}
					row[i] = &recv
				} else {
					var recv NullFloat
					row[i] = &recv
				}
			} else {
				if array {
					var recv []float64
					row[i] = &recv
				} else {
					var recv float64
					row[i] = &recv
				}
			}
		case structs.TypeString:
			if nullable {
				if array {
					recv := []NullString{}
					row[i] = &recv
				} else {
					recv := NullString{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []string
					row[i] = &recv
				} else {
					var recv string
					row[i] = &recv
				}
			}
		case structs.TypeTime:
			if nullable {
				if array {
					recv := []NullTime{}
					row[i] = &recv
				} else {
					recv := NullTime{}
					row[i] = &recv
				}
			} else {
				if array {
					var recv []time.Time
					row[i] = &recv
				} else {
					var recv time.Time
					row[i] = &recv
				}
			}
		case structs.TypeBytes:
			if array {
				var recv [][]byte
				row[i] = &recv
			} else {
				var recv []byte
				row[i] = &recv
			}
		default:
			if array {
				var recv []interface{}
				row[i] = &recv
			} else {
				var recv interface{}
				row[i] = &recv
			}
		}
	}

	err = rows.Scan(row...)
	if row == nil || err != nil {
		return nil, err
	}

	for i, column := range columns {
		if row[i] == nil {
			result[column] = nil
			continue
		}

		switch v := row[i].(type) {
		case *NullInt:
			if v.IsNull {
				result[column] = nil
			} else {
				result[column] = v.Int
			}
		case *NullUint:
			if v.IsNull {
				result[column] = nil
			} else {
				result[column] = v.Uint
			}
		case *NullFloat:
			if v.IsNull {
				result[column] = nil
			} else {
				tmp := v.Float
				decS := decimalS[i]
				if decS > 0 {
					tmp = tmp / decS
				}
				result[column] = tmp
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
		case *float64:
			decS := decimalS[i]
			if decS > 0 {
				*v = *v / decS
			}
			result[column] = *v
		default:
			result[column] = row[i]
		}
	}

	return result, nil
}

// 仅展示靠前的部分数据
func (q *Query) firstPartData(datas []map[string]interface{}) string {
	if len(datas) > 10 {
		return json.MarshalToString(datas[:10], json.EncodeTypeFast) + " ..."
	} else if len(datas) > 0 {
		return json.MarshalToString(datas, json.EncodeTypeFast)
	}
	return ""
}

func retryDesc(retryTimes int) string {
	if retryTimes > 0 {
		return fmt.Sprintf("retry %d times, ", retryTimes)
	}
	return ""
}
