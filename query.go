// Copyright (c) 2024 The horm-database Authors (such as CaoHao <18500482693@163.com>). All rights reserved.
//
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

import (
	"github.com/horm-database/go-horm/horm"
	"github.com/horm-database/go-horm/horm/codec"
)

// Op 设置操作
func (o *ORM) Op(op string) *ORM {
	o.query.Op(op)
	return o
}

// Name 设置表名
func (o *ORM) Name(name string) *ORM {
	o.query.Name(name)
	return o
}

// Shard 分片、分表
func (o *ORM) Shard(shard ...string) *ORM {
	o.query.Shard(shard...)
	return o
}

// Column 列
func (o *ORM) Column(columns ...string) *ORM {
	o.query.Column(columns...)
	return o
}

// Where 查询条件
func (o *ORM) Where(where horm.Where) *ORM {
	o.query.Where(where)
	return o
}

// Insert （批量）插入数据，参数可以是 struct / []struct / Map / []Map
func (o *ORM) Insert(data interface{}) *ORM {
	o.query.Insert(data)
	return o
}

// Replace (batch) （批量）替换数据，参数可以是 struct / []struct / Map / []Map
func (o *ORM) Replace(data interface{}) *ORM {
	o.query.Replace(data)
	return o
}

// Update 更新数据，参数可以是 struct / Map
func (o *ORM) Update(data interface{}, where ...horm.Where) *ORM {
	o.query.Update(data, where...)
	return o
}

// Delete 根据条件删除
func (o *ORM) Delete(where ...horm.Where) *ORM {
	o.query.Delete(where...)
	return o
}

// Find 查询满足条件的一条数据
func (o *ORM) Find(where ...horm.Where) *ORM {
	o.query.Find(where...)
	return o
}

// FindAll 查询满足条件的所有数据
func (o *ORM) FindAll(where ...horm.Where) *ORM {
	o.query.FindAll(where...)
	return o
}

// FindBy find where key1=value1 AND key2=value2 ...
// input must be key, value, key, value, key, value ...
func (o *ORM) FindBy(key string, value interface{}, kvs ...interface{}) *ORM {
	o.query.FindBy(key, value, kvs...)
	return o
}

// FindAllBy find_all where key1=value1 AND key2=value2 ...
// input must be key, value, key, value, key, value ...
func (o *ORM) FindAllBy(key string, value interface{}, kvs ...interface{}) *ORM {
	o.query.FindAllBy(key, value, kvs...)
	return o
}

// DeleteBy find_all where key1=value1 AND key2=value2 ...
// input must be key, value, key, value, key, value ...
func (o *ORM) DeleteBy(key string, value interface{}, kvs ...interface{}) *ORM {
	o.query.DeleteBy(key, value, kvs...)
	return o
}

// Page 分页
func (o *ORM) Page(page, pageSize int) *ORM {
	o.query.Page(page, pageSize)
	return o
}

// Limit 排序
func (o *ORM) Limit(limit int, offset ...uint64) *ORM {
	o.query.Limit(limit, offset...)
	return o
}

// Order 排序, 首字母 + 表示升序， - 表示降序
func (o *ORM) Order(orders ...string) *ORM {
	o.query.Order(orders...)
	return o
}

// Group 分组 group by
func (o *ORM) Group(group ...string) *ORM {
	o.query.Group(group...)
	return o
}

// Having 分组条件
func (o *ORM) Having(having horm.Where) *ORM {
	o.query.Having(having)
	return o
}

// SetParam 与数据库特性相关的附加参数，例如 redis 的WITHSCORES，以及 elastic 的 collapse、runtime_mappings、track_total_hits 等等。
func (o *ORM) SetParam(key string, value interface{}) *ORM {
	o.query.SetParam(key, value)
	return o
}

// Source 直接输入查询语句查询
func (o *ORM) Source(q string, args ...interface{}) *ORM {
	o.query.Source(q, args...)
	return o
}

// WithCoder 更换编解码器
func (o *ORM) WithCoder(coder codec.Codec) *ORM {
	o.query.WithCoder(coder)
	return o
}

// GetCoder 获取编解码器
func (o *ORM) GetCoder() codec.Codec {
	return o.query.GetCoder()
}
