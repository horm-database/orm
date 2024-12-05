package redis

import (
	"context"
	"fmt"
	"strconv"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/log"
	"github.com/horm-database/common/proto"
	"github.com/horm-database/common/proto/plugin"
	"github.com/horm-database/common/types"
	"github.com/horm-database/common/util"
	"github.com/horm-database/orm/database/redis/client"
	olog "github.com/horm-database/orm/log"
	"github.com/horm-database/orm/obj"

	redigo "github.com/gomodule/redigo/redis"
)

type Redis struct {
	Cmd        string
	Prefix     string
	Key        string
	Args       []interface{}
	WithScores bool

	Addr    *util.DBAddress
	TimeLog *log.TimeLog

	QL string
}

func (r *Redis) SetParams(req *plugin.Request,
	prop *obj.Property, addr *util.DBAddress, transInfo *obj.TransInfo) error {
	r.Cmd = req.Op
	r.Prefix = req.Prefix
	r.Key = req.Key
	r.Args = req.Args

	r.WithScores, _ = req.Params.GetBool("with_scores")

	r.Addr = addr
	return nil
}

// Query sql 查询
func (r *Redis) Query(ctx context.Context) (interface{}, *proto.Detail, bool, error) {
	r.TimeLog = olog.NewTimeLog(ctx, r.Addr)
	result, isNil, err := r.Do(ctx)
	return result, nil, isNil, err
}

// Do 执行 redis
func (r *Redis) Do(ctx context.Context) (interface{}, bool, error) {
	if r.WithScores {
		if len(r.Args) <= 2 {
			r.Args = append(r.Args, "WITHSCORES")
		} else {
			tmp := make([]interface{}, len(r.Args)+1)
			for k, v := range r.Args {
				if k == 2 {
					tmp[k] = "WITHSCORES"
				}
				tmp[k] = v
				r.Args = tmp
			}
		}
	}

	reply, isNil, err := r.do(ctx)
	if err != nil || isNil {
		return nil, isNil, err
	}

	var ret interface{}

	switch consts.GetRedisRetType(r.Cmd, r.WithScores) {
	case consts.RedisRetTypeNil:
		return nil, false, nil
	case consts.RedisRetTypeString:
		ret, err = redigo.String(reply, nil)
	case consts.RedisRetTypeBool:
		if r.Cmd == consts.OpSet { // for SET Key Value NX
			switch realReply := reply.(type) {
			case []byte:
				if types.BytesToString(realReply) == "OK" {
					reply = []byte("TRUE")
				}
			case string:
				if realReply == "OK" {
					reply = []byte("TRUE")
				}
			case nil:
				reply = []byte("FALSE")
			}
		}

		ret, err = redigo.Bool(reply, nil)
	case consts.RedisRetTypeInt64:
		ret, err = redigo.Int64(reply, nil)
	case consts.RedisRetTypeFloat64:
		ret, err = redigo.Float64(reply, nil)
	case consts.RedisRetTypeStrings:
		ret, err = redigo.Strings(reply, nil)
	case consts.RedisRetTypeMapString:
		if r.Cmd == consts.OpHMGet {
			var strs []string
			strs, err = redigo.Strings(reply, nil)
			if err == nil {
				var mapRet = make(map[string]string, len(r.Args))
				for k, arg := range r.Args {
					mapRet[types.InterfaceToString(arg)] = strs[k]
				}
				ret = mapRet
			}
		} else {
			ret, err = redigo.StringMap(reply, nil)
		}
	case consts.RedisRetTypeMemberScore:
		var tmp [][]byte
		tmp, err = redigo.ByteSlices(reply, nil)
		if err == nil {
			memberScore := proto.MemberScore{}
			memberScore.Member, memberScore.Score, err = decodeRangeWithScores(tmp)
			ret = &memberScore
		}
	}

	if err != nil {
		if err == redigo.ErrNil {
			return nil, true, nil
		}

		return nil, false, errs.NewDBErrorf(errs.RetRedisDecodeFailed,
			"redis decode error: [%v], cmd=[%s %s%s %v]", err, r.Cmd, r.Prefix, r.Key, util.FormatArgs(r.Args))
	}

	return ret, false, nil
}

// GetQueryStatement 获取 es 的查询语句
func (r *Redis) GetQueryStatement() string {
	return r.QL
}

func (r *Redis) do(ctx context.Context) (interface{}, bool, error) {
	c := client.NewClient(r.Addr)

	args := []interface{}{}
	args = append(args, fmt.Sprintf("%s%s", r.Prefix, r.Key))
	args = append(args, r.Args...)

	//执行 DO
	reply, err := c.Do(ctx, r.Cmd, args...)

	if err != nil && err != redigo.ErrNil {
		if !olog.OmitError(r.Addr) {
			r.TimeLog.Errorf(errs.Code(err), "redis error:[%s], cmd=[%s %v]",
				errs.Msg(err), r.Cmd, util.FormatArgs(args))
		}

		return nil, false, err
	} else if r.TimeLog.OverThreshold() {
		tmp, _ := redigo.Strings(reply, err)
		r.TimeLog.Warn("redis slow: ", r.Cmd, util.FormatArgs(args), tmp) // 慢日志
	} else if olog.IsDebug(r.Addr) {
		tmp, _ := redigo.Strings(reply, err)
		r.TimeLog.Info("redis: ", r.Cmd, util.FormatArgs(args), tmp) // debug日志
	}

	if err == redigo.ErrNil {
		return nil, true, nil
	}

	return reply, false, nil
}

func decodeRangeWithScores(src [][]byte) (member []string, score []float64, err error) {
	if len(src) == 0 {
		return nil, nil, nil
	}

	if len(src)%2 != 0 {
		return nil, nil, fmt.Errorf("decodeRangeWithScores error: src is invalid")
	}

	i := 0
	for _, bytes := range src {
		if i%2 == 0 {
			member = append(member, types.BytesToString(bytes))
		} else {
			f, err := strconv.ParseFloat(types.BytesToString(bytes), 64)
			if err != nil {
				return nil, nil, fmt.Errorf("decodeRangeWithScores error: %v", err)
			}

			score = append(score, f)
		}
		i++
	}

	return
}
