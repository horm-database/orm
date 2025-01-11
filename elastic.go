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

import (
	"github.com/horm-database/go-horm/horm"
)

// Type elastic search 版本 v7 以前有 type， v7之后 type 统一为 _doc
func (o *ORM) Type(typ string) *ORM {
	o.query.Type(typ)
	return o
}

// ID elastic 按照 _id 查询
func (o *ORM) ID(value interface{}) *ORM {
	o.query.ID(value)
	return o
}

// Scroll 查询，size 为每次 scroll 大小，where 为 scroll 条件。
func (o *ORM) Scroll(scroll string, size int, where ...horm.Where) *ORM {
	o.query.Scroll(scroll, size, where...)
	return o
}

// ScrollByID 根据 scrollID 滚动查询。
func (o *ORM) ScrollByID(scrollID string) *ORM {
	o.query.ScrollByID(scrollID)
	return o
}

// Refresh 更新数据立即刷新。
func (o *ORM) Refresh() *ORM {
	o.query.Refresh()
	return o
}

// HighLight 返回高亮
func (o *ORM) HighLight(field string, preTag, postTag string) *ORM {
	o.query.HighLight(field, preTag, postTag)
	return o
}
