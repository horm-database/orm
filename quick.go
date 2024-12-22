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

package orm

// Eq equal mean where key1=value1 AND key2=value2 ...
// input must be key, value, key, value, key, value ...
func (o *ORM) Eq(key string, value interface{}, kvs ...interface{}) *ORM {
	o.query.Eq(key, value, kvs...)
	return o
}

func (o *ORM) Not(key string, value interface{}) *ORM {
	o.query.Not(key, value)
	return o
}

func (o *ORM) Lt(key string, value interface{}) *ORM {
	o.query.Lt(key, value)
	return o
}

func (o *ORM) Gt(key string, value interface{}) *ORM {
	o.query.Gt(key, value)
	return o
}

func (o *ORM) Lte(key string, value interface{}) *ORM {
	o.query.Lte(key, value)
	return o
}

func (o *ORM) Gte(key string, value interface{}) *ORM {
	o.query.Gte(key, value)
	return o
}

func (o *ORM) Between(key string, start, end interface{}) *ORM {
	o.query.Between(key, start, end)
	return o
}

func (o *ORM) NotBetween(key string, start, end interface{}) *ORM {
	o.query.NotBetween(key, start, end)
	return o
}

func (o *ORM) Like(key string, value interface{}) *ORM {
	o.query.Like(key, value)
	return o
}

func (o *ORM) NotLike(key string, value interface{}) *ORM {
	o.query.NotLike(key, value)
	return o
}

func (o *ORM) MatchPhrase(key string, value interface{}) *ORM {
	o.query.MatchPhrase(key, value)
	return o
}

func (o *ORM) NotMatchPhrase(key string, value interface{}) *ORM {
	o.query.NotMatchPhrase(key, value)
	return o
}

func (o *ORM) Match(key string, value interface{}) *ORM {
	o.query.Match(key, value)
	return o
}

func (o *ORM) NotMatch(key string, value interface{}) *ORM {
	o.query.NotMatch(key, value)
	return o
}

// UpdateKV 更新字段，快速更新键值对 key = value
func (o *ORM) UpdateKV(key string, value interface{}, kvs ...interface{}) *ORM {
	o.query.UpdateKV(key, value, kvs...)
	return o
}
