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

package elastic

import (
	"errors"
	"fmt"
	"strings"

	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/json"
	"github.com/horm-database/orm/log"

	esv6 "github.com/olivere/elastic/v6"
	esv7 "github.com/olivere/elastic/v7"
)

func (q *Query) logError(loc, id string, source interface{}, err error) error {
	e := q.formatError(loc, id, source, err)

	if !log.OmitError(q.Addr) {
		q.TimeLog.Error(errs.Code(e), "elastic query error: ", errs.Msg(e))
	}

	return e
}

func (q *Query) logInfo(f string, id string, source interface{}) {
	if q.TimeLog.OverThreshold() {
		//请求耗时超过 warn_timeout 时打警告日志
		q.TimeLog.Warn("elastic slow: ", q.formatError(f, id, source, nil).Error())
	} else if log.IsDebug(q.Addr) {
		q.TimeLog.Info("elastic query: ", q.formatError(f, id, source, nil).Error())
	}
}

func (q *Query) formatError(loc, id string, source interface{}, err error) error {
	var code int
	msg := strings.Builder{}

	msg.WriteString(loc)
	msg.WriteString(`: name=[`)
	msg.WriteString(q.Path)

	if len(q.Index) > 0 {
		msg.WriteString(`], index=[`)
		msg.WriteString(strings.Join(q.Index, ","))
	}

	if id != "" {
		msg.WriteString(`], id=[`)
		msg.WriteString(id)
	}

	if err != nil {
		cause := strings.Builder{}

		switch e := err.(type) {
		case *esv6.Error:
			msg.WriteString(`], status=[`)
			msg.WriteString(fmt.Sprint(e.Status))

			code = e.Status
			if e.Details != nil && e.Details.RootCause != nil {
				for _, rootCause := range e.Details.RootCause {
					cause.WriteString("{type: ")
					cause.WriteString(rootCause.Type)
					cause.WriteString(", reason: ")
					cause.WriteString(rootCause.Reason)
					cause.WriteString("}, ")
				}
			}

		case *esv7.Error:
			msg.WriteString(`], status=[`)
			msg.WriteString(fmt.Sprint(e.Status))

			code = e.Status
			if e.Details != nil && e.Details.RootCause != nil {
				for _, rootCause := range e.Details.RootCause {
					cause.WriteString("{type: ")
					cause.WriteString(rootCause.Type)
					cause.WriteString(", reason: ")
					cause.WriteString(rootCause.Reason)
					cause.WriteString("}, ")
				}
			}
		case *errs.Error:
			code = e.Code
		}

		msg.WriteString(`], error=[`)
		msg.WriteString(errs.Msg(err))

		msg.WriteString(`], cause=[`)
		msg.WriteString(cause.String())

		if code == 0 {
			code = errs.ErrElasticQuery
		}
	}

	if source != nil {
		msg.WriteString(`], source=[`)
		msg.Write(json.Marshal(source, json.EncodeTypeFast))
	}

	if len(q.Datas) > 0 {
		msg.WriteString(`], datas=[`)
		msg.Write(json.Marshal(q.Datas, json.EncodeTypeFast))
	} else if len(q.Data) > 0 {
		msg.WriteString(`], data=[`)
		msg.Write(json.Marshal(q.Data, json.EncodeTypeFast))
	}

	msg.WriteString("]")

	if err == nil {
		return errors.New(msg.String())
	} else {
		return errs.NewDBError(code, msg.String())
	}
}
