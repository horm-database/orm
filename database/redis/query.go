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

package redis

import (
	"context"
	"fmt"
	"strconv"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/json"
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
	Cmd    string
	Prefix string
	Key    string
	Field  string
	Val    interface{}
	Data   types.Map
	Datas  []map[string]interface{}
	Params types.Map
	Args   []interface{}

	WithScores bool

	Addr    *util.DBAddress
	TimeLog *log.TimeLog
}

func (r *Redis) SetParams(req *plugin.Request,
	prop *obj.Property, addr *util.DBAddress, transInfo *obj.TransInfo) error {
	r.Cmd = req.Op
	r.Prefix = req.Prefix
	r.Key = req.Key
	r.Field = req.Field
	r.Val = req.Val
	r.Data = req.Data
	r.Datas = req.Datas
	r.Params = req.Params

	r.Args = req.Args

	r.Addr = addr
	return nil
}

// Query sql 查询
func (r *Redis) Query(ctx context.Context) (interface{}, *proto.Detail, bool, error) {
	r.TimeLog = olog.NewTimeLog(ctx, r.Addr)

	err := r.parseRequest()
	if err != nil {
		return nil, nil, false, errs.NewDBf(errs.ErrRedisReqParse, "parse redis req to command error: %v", err)
	}

	reply, isNil, err := r.do(ctx)
	if err != nil || isNil {
		return nil, nil, isNil, err
	}

	return r.parseResult(reply)
}

// GetQuery 获取 redis cmmd 语句
func (r *Redis) GetQuery() string {
	return ""
}

func (r *Redis) do(ctx context.Context) (interface{}, bool, error) {
	c := client.NewClient(r.Addr)

	//执行 DO
	reply, err := c.Do(ctx, r.Cmd, r.Args...)

	if err != nil && err != redigo.ErrNil {
		if !olog.OmitError(r.Addr) {
			r.TimeLog.Errorf(errs.Code(err), "redis error:[%s], cmd=[%s %v]",
				errs.Msg(err), r.Cmd, util.FormatArgs(r.Args))
		}

		return nil, false, err
	} else if r.TimeLog.OverThreshold() {
		tmp, _ := redigo.Strings(reply, err)
		r.TimeLog.Warn("redis slow: ", r.Cmd, util.FormatArgs(r.Args), tmp) // 慢日志
	} else if olog.IsDebug(r.Addr) {
		tmp, _ := redigo.Strings(reply, err)
		r.TimeLog.Info("redis: ", r.Cmd, util.FormatArgs(r.Args), tmp) // debug日志
	}

	if err == redigo.ErrNil {
		return nil, true, nil
	}

	return reply, false, nil
}

func (r *Redis) parseRequest() error {
	if len(r.Args) > 0 {
		switch r.Cmd {
		case consts.OpMGet: // MGET 会走这里
			for _, v := range r.Args {
				r.Args = append(r.Args, fmt.Sprintf("%s%s", r.Prefix, types.ToString(v)))
			}
		case consts.OpHMGet, consts.OpHDel: // HMGET、HDEL 会走这里
			for k, v := range r.Args {
				r.Args[k] = types.ToString(v)
			}
			r.Args = append([]interface{}{fmt.Sprintf("%s%s", r.Prefix, r.Key)}, r.Args...)
		case consts.OpLPush, consts.OpRPush, consts.OpSAdd, consts.OpSRem:
			for k, v := range r.Args {
				r.Args[k] = json.MarshalBaseToString(v)
			}
			r.Args = append([]interface{}{fmt.Sprintf("%s%s", r.Prefix, r.Key)}, r.Args...)
		case consts.OpZAdd:
			for k, v := range r.Args {
				r.Args[k] = json.MarshalBaseToString(v)
			}
		default:
			r.Args = append([]interface{}{fmt.Sprintf("%s%s", r.Prefix, r.Key)}, r.Args...)
		}

		return nil
	}

	if r.Cmd != consts.OpMSet {
		r.Args = append(r.Args, fmt.Sprintf("%s%s", r.Prefix, r.Key))
	}

	switch r.Cmd {
	case consts.OpExpire, consts.OpIncrBy:
		r.Args = append(r.Args, r.Val)
	case consts.OpSet:
		r.Args = append(r.Args, r.encodeVal())
		r.getParams(consts.SetParams)
	case consts.OpSetEx: // 参数在前
		r.getParams(consts.SetExParams)
		r.Args = append(r.Args, r.encodeVal())
	case consts.OpSetNX, consts.OpGetSet, consts.OpLPush, consts.OpRPush,
		consts.OpSAdd, consts.OpSRem, consts.OpSIsMember:
		r.Args = append(r.Args, r.encodeVal())
	case consts.OpMSet:
		for k, v := range r.Data {
			r.Args = append(r.Args,
				fmt.Sprintf("%s%s", r.Prefix, k),
				json.MarshalBaseToString(v))
		}
	case consts.OpSetBit, consts.OpGetBit:
		r.getParams(consts.SetGetBitParams)
	case consts.OpBitCount:
		r.getParams(consts.BitCountParams)
	case consts.OpHSet:
		if r.Field != "" {
			r.Args = append(r.Args, r.Field, json.MarshalBaseToString(r.Val))
		}

		if r.Data != nil {
			for k, v := range r.Data {
				r.Args = append(r.Args, k, json.MarshalBaseToString(v))
			}
		}
	case consts.OpHGet, consts.OpHExists, consts.OpHStrLen:
		r.Args = append(r.Args, r.Field)
	case consts.OpHSetNx, consts.OpHmSet:
		for k, v := range r.Data {
			r.Args = append(r.Args, k, json.MarshalBaseToString(v))
		}
	case consts.OpHIncrBy, consts.OpHIncrByFloat:
		r.Args = append(r.Args, r.Field, r.Val)
	case consts.OpLPop, consts.OpRPop, consts.OpSRandMember, consts.OpSPop:
		r.getParams(consts.CountParams)
	case consts.OpSMove:
		r.getParams(consts.SMoveParams)
		r.Args = append(r.Args, r.encodeVal())
	case consts.OpZAdd:
		r.Args = append(r.Args, r.encodeVal())
	}

	return nil
}

func (r *Redis) encodeVal() string {
	if r.Datas != nil {
		return json.MarshalToString(r.Datas)
	} else if r.Data != nil {
		return json.MarshalToString(r.Data)
	} else {
		return json.MarshalBaseToString(r.Val)
	}
}

func (r *Redis) getParams(paramInfos []*consts.RedisParamInfo) {
	if len(r.Params) == 0 {
		return
	}

	for _, paramInfo := range paramInfos {
		if arg, ok := r.Params[paramInfo.Name]; ok {
			if !paramInfo.JustVal {
				r.Args = append(r.Args, paramInfo.Name)
			}
			switch paramInfo.Cnt {
			case 0, 1:
				continue
			case 2:
				r.Args = append(r.Args, arg)
			default:
				tmp, _ := types.ToArray(arg)
				r.Args = append(r.Args, tmp...)
			}
		}
	}

	withScores, _ := r.Params.GetBool("with_scores")
	if withScores {
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

	return
}

func (r *Redis) parseResult(reply interface{}) (interface{}, *proto.Detail, bool, error) {
	var ret interface{}
	var err error

	withscores, _ := r.Params.GetBool("withscores")
	_, countExists, _ := r.Params.GetInt("count")
	switch consts.GetRedisRetType(r.Cmd, withscores, countExists) {
	case consts.RedisRetTypeNil:
		return nil, nil, false, nil
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
				var mapRet = make(map[string]string, len(r.Args)-1)
				for k, arg := range r.Args {
					if k == 0 {
						continue
					}
					mapRet[types.ToString(arg)] = strs[k-1]
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
			memberScore.Member, memberScore.Score, err = r.decodeRangeWithScores(tmp)
			ret = &memberScore
		}
	}

	if err != nil {
		if err == redigo.ErrNil {
			return nil, nil, true, nil
		}

		return nil, nil, false, errs.NewDBf(errs.ErrRedisDecode,
			"redis decode error: [%v], cmd=[%s %s%s %v]", err, r.Cmd, r.Prefix, r.Key, util.FormatArgs(r.Args))
	}

	return ret, nil, false, nil
}

func (r *Redis) decodeRangeWithScores(src [][]byte) (member []string, score []float64, err error) {
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
