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

package log

import (
	"context"
	"time"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/log"
	"github.com/horm-database/common/util"
)

// NewTimeLog 获取超时日志
func NewTimeLog(ctx context.Context, addr *util.DBAddress) *log.TimeLog {
	//在请求响应时长超过 warn_timeout 毫秒的时候告警
	warnTimeout := 200 * time.Millisecond
	if addr.WarnTimeout > 0 {
		warnTimeout = time.Duration(addr.WarnTimeout) * time.Millisecond
	}

	return log.NewTimeLog(ctx, warnTimeout)
}

// OmitError 是否忽略错误
func OmitError(addr *util.DBAddress) bool {
	if addr.OmitError == consts.FALSE {
		return false
	}
	return true
}

// IsDebug 是否打印 debug 日志
func IsDebug(addr *util.DBAddress) bool {
	if addr.Debug == consts.TRUE {
		return true
	}
	return false
}
