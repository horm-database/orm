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
	ej "encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	util2 "github.com/horm-database/common/json"
	"github.com/horm-database/orm/log"
)

var defaultTimeLayout = "2006-01-02 15:04:05 -0700 MST" // time.Parse 解析数据库时间字段到 NullTime 时的 layout，默认格式: "2006-01-02 15:04:05 -0700 MST"

var loc *time.Location // 时区位置

// SetLocation 慎重使用，此处影响的是全局的 NullTime 时间 logic 的时区
func SetLocation(l *time.Location) {
	loc = l
}

func (q *Query) logInfo(sql string, params []interface{}) {
	db, _ := consts.DBTypeDesc[q.Addr.Type]

	if q.TimeLog.OverThreshold() { //在请求相应时长超过 iDuringWarn 毫秒时打警告日志
		q.TimeLog.Warnf("%s query slow: [%s], params=[%v]", db, sql, params)
	} else if log.IsDebug(q.Addr) {
		q.TimeLog.Infof("%s query: [%s], params=[%v]", db, sql, params)
	}
}

func (q *Query) logError(err error, sql string, params []interface{}) error {
	db, _ := consts.DBTypeDesc[q.Addr.Type]
	if !log.OmitError(q.Addr) {
		q.TimeLog.Errorf(errs.Code(err), "%s query error: [%v], sql=[%s]",
			db, errs.Msg(err), GetSQLWithParams(sql, params))
	}

	return errs.NewDBf(errs.Code(err), "%s query error: [%v]", db, errs.Msg(err))
}

func GetSQLWithParams(sql string, params []interface{}) string {
	var last, paramsIndex int
	result := strings.Builder{}

	for k, c := range sql {
		if c == '?' {
			result.WriteString(sql[last:k])

			if paramsIndex < len(params) {
				if params[paramsIndex] == nil {
					result.WriteString("null")
				} else {
					switch v := params[paramsIndex].(type) {
					case string:
						result.WriteString("'")
						result.WriteString(v)
						result.WriteString("'")
					case *string:
						result.WriteString("'")
						result.WriteString(*v)
						result.WriteString("'")
					case []byte:
						result.WriteString("'")
						result.Write(v)
						result.WriteString("'")
					case *[]byte:
						result.WriteString("'")
						result.Write(*v)
						result.WriteString("'")
					case bool:
						result.WriteString(fmt.Sprintf("%v", v))
					case *bool:
						result.WriteString(fmt.Sprintf("%v", *v))
					case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
						result.WriteString(fmt.Sprintf("%d", v))
					case *int:
						result.WriteString(fmt.Sprintf("%d", *v))
					case *int8:
						result.WriteString(fmt.Sprintf("%v", *v))
					case *int16:
						result.WriteString(fmt.Sprintf("%v", *v))
					case *int32:
						result.WriteString(fmt.Sprintf("%v", *v))
					case *int64:
						result.WriteString(fmt.Sprintf("%v", *v))
					case *uint:
						result.WriteString(fmt.Sprintf("%v", *v))
					case *uint8:
						result.WriteString(fmt.Sprintf("%v", *v))
					case *uint16:
						result.WriteString(fmt.Sprintf("%v", *v))
					case *uint32:
						result.WriteString(fmt.Sprintf("%v", *v))
					case *uint64:
						result.WriteString(fmt.Sprintf("%v", *v))
					case float32, float64:
						result.WriteString(fmt.Sprintf("%f", v))
					case *float32:
						result.WriteString(fmt.Sprintf("%f", *v))
					case *float64:
						result.WriteString(fmt.Sprintf("%f", *v))
					case ej.Number:
						result.WriteString("'")
						result.WriteString(v.String())
						result.WriteString("'")
					case *ej.Number:
						result.WriteString("'")
						result.WriteString(v.String())
						result.WriteString("'")
					default:
						result.WriteString(util2.MarshalToString(v))
					}
				}
			} else {
				result.WriteString("?")
			}

			paramsIndex++
			last = k + 1
		}
	}

	return result.String()
}
