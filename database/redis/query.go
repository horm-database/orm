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
	Keys   []string
	Val    interface{}
	Data   types.Map
	Datas  []map[string]interface{}
	Params types.Map
	Args   []interface{}

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
	r.Keys = req.Keys
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

	args, err := r.parseRequest()
	if err != nil {
		return nil, nil, false, errs.NewDBf(errs.ErrRedisReqParse, "parse redis req to command error: %v", err)
	}

	reply, isNil, err := r.do(ctx, args)
	if err != nil || isNil {
		return nil, nil, isNil, err
	}

	return r.parseResult(reply)
}

// GetQueryStatement 获取 es 的查询语句
func (r *Redis) GetQueryStatement() string {
	return r.QL
}

func (r *Redis) do(ctx context.Context, args []interface{}) (interface{}, bool, error) {
	c := client.NewClient(r.Addr)

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

func (r *Redis) parseRequest() ([]interface{}, error) {
	r.WithScores, _ = r.Params.GetBool("with_scores")
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

	args := []interface{}{}
	args = append(args, fmt.Sprintf("%s%s", r.Prefix, r.Key))

	if len(r.Args) > 0 {
		args = append(args, r.Args...)
		return args, nil
	}

	switch r.Cmd {
	case consts.OpExpire:
		args = append(args, r.Val)
	}

	return args, nil
}

func (r *Redis) parseResult(reply interface{}) (interface{}, *proto.Detail, bool, error) {
	var ret interface{}
	var err error

	switch consts.GetRedisRetType(r.Cmd, r.WithScores) {
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
				var mapRet = make(map[string]string, len(r.Args))
				for k, arg := range r.Args {
					mapRet[types.ToString(arg)] = strs[k]
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
