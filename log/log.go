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
