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
package sql

import (
	"reflect"
	"strings"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/proto/sql"
	"github.com/horm-database/common/types"
	"github.com/horm-database/common/util"
)

// Statement 查询语句结构体
type Statement struct {
	dbType         int
	selects        string
	set            string
	distinct       bool
	table          string
	alias          string
	join           []string
	where          string
	order          string
	limit          int
	offset         uint64
	group          string
	having         string
	params         []interface{}
	forUpdate      string
	indexHints     string
	conflictTarget string // PostgreSQL InsertOnDuplicateKeyUpdate: For ON CONFLICT DO UPDATE, a conflict_target must be provided.

	condBuilder *strings.Builder
	condParams  []interface{}

	op string
}

// GetOrder 获取排序 order
func (s *Statement) GetOrder() string {
	return s.order
}

// GetWhere 获取 where 条件
func (s *Statement) GetWhere() string {
	return s.where
}

// GetParams 获取 params
func (s *Statement) GetParams() []interface{} {
	return s.params
}

// SetColumn 设置列
func (s *Statement) SetColumn(column []string) *Statement {
	s.selects = "*"

	if len(column) > 0 {
		columnStr := strings.Builder{}
		for k, v := range column {
			if k > 0 {
				columnStr.WriteString(",")
			}

			columnStr.WriteString(columnQuote(v))
		}
		s.selects = columnStr.String()
	}

	return s
}

// SetTable 设置表与别名
func (s *Statement) SetTable(table, alias string) *Statement {
	s.table = table
	s.alias = alias
	return s
}

// SetDBType 设置库类型
func (s *Statement) SetDBType(typ int) *Statement {
	s.dbType = typ
	return s
}

// Limit 组装mysql limit 条件，可以使用分页配合使用
func (s *Statement) Limit(limit int) *Statement {
	s.limit = limit
	return s
}

// SetMaps insert、replace 数据
func (s *Statement) SetMaps(attributeArr []map[string]interface{}) *Statement {
	if len(attributeArr) == 0 {
		return s
	}

	var i int
	var setBuilder = strings.Builder{}
	setBuilder.WriteString("(")

	keys := []string{}
	for key := range attributeArr[0] {
		keys = append(keys, key)

		if i == 0 {
			setBuilder.WriteString(columnQuote(key))
		} else {
			setBuilder.WriteString(`,`)
			setBuilder.WriteString(columnQuote(key))
		}

		i++
	}

	setBuilder.WriteString(") values ")

	for k, attributes := range attributeArr {
		if k == 0 {
			setBuilder.WriteString("(")
		} else {
			setBuilder.WriteString(",(")
		}

		for j, key := range keys {
			if j == 0 {
				setBuilder.WriteString(`?`)
			} else {
				setBuilder.WriteString(`,?`)
			}

			s.params = append(s.params, attributes[key])
		}

		setBuilder.WriteString(")")
	}

	s.set = setBuilder.String()
	return s
}

// UpdateMap update 数据
func (s *Statement) UpdateMap(attributes map[string]interface{}) *Statement {
	if len(attributes) == 0 {
		return s
	}

	var setBuilder = strings.Builder{}

	var i int
	for key, value := range attributes {
		if i == 0 {
			setBuilder.WriteString(columnQuote(key))
			setBuilder.WriteString("=?")
		} else {
			setBuilder.WriteString(",")
			setBuilder.WriteString(columnQuote(key))
			setBuilder.WriteString("=?")
		}

		s.params = append(s.params, value)
		i++
	}

	s.set = setBuilder.String()
	return s
}

// Where 快捷 where 查询条件组装
func (s *Statement) Where(dbType int, where map[string]interface{}) *Statement {
	if len(where) == 0 {
		return s
	}

	s.condBuilder = &strings.Builder{}
	s.condParams = []interface{}{}

	var i int
	var numberIndex int
	for key, value := range where {
		s.whereImplode(dbType, &numberIndex, i, key, value, consts.AND)
		i++
	}

	s.where = s.condBuilder.String()
	s.params = append(s.params, s.condParams...)

	return s
}

// Having having语句
func (s *Statement) Having(dbType int, having map[string]interface{}) *Statement {
	if len(having) == 0 {
		return s
	}

	s.condBuilder = &strings.Builder{}
	s.condParams = []interface{}{}

	var i int
	var numberIndex int
	for key, value := range having {
		s.whereImplode(dbType, &numberIndex, i, key, value, consts.AND)
		i++
	}

	s.having = s.condBuilder.String()
	s.params = append(s.params, s.condParams...)
	return s
}

// Group 分组 group by
func (s *Statement) Group(group []string) *Statement {
	l := len(group)

	if l == 1 {
		s.group = group[0]
	} else if l > 1 {
		s.group = "`" + strings.Join(group, "`,`") + "`"
	}

	return s
}

// Order 排序
func (s *Statement) Order(orders []string) *Statement {
	if len(orders) > 0 {
		orderArr := util.FormatOrders(orders)

		orderStr := strings.Builder{}
		for k, v := range orderArr {
			if k != 0 {
				orderStr.WriteString(",")
			}

			orderStr.WriteString(v.Field)
			if !v.Ascending {
				orderStr.WriteString(" DESC")
			}
		}

		s.order = orderStr.String()
	}
	return s
}

// Join 表 join
func (s *Statement) Join(joins []*sql.Join) *Statement {
	if len(joins) == 0 {
		return s
	}

	for _, join := range joins {
		s.realJoin(join.Table, strings.ToUpper(join.Type), join.Using, join.On)
	}

	return s
}

// realJoin 表 join
func (s *Statement) realJoin(table string, joinType string, using []string, on map[string]string) *Statement {
	table, joinAlias := util.Alias(table)

	var joinBuilder = strings.Builder{}
	joinBuilder.WriteString(joinType)
	joinBuilder.WriteString(" JOIN `")
	joinBuilder.WriteString(table)
	joinBuilder.WriteString("` ")

	if joinAlias != "" {
		joinBuilder.WriteString("AS `")
		joinBuilder.WriteString(joinAlias)
		joinBuilder.WriteString("` ")
	}

	if len(using) > 0 {
		joinBuilder.WriteString("USING (`")
		joinBuilder.WriteString(strings.Join(using, "`,`"))
		joinBuilder.WriteString("`) ")
	} else if len(on) > 0 {
		joinBuilder.WriteString("ON ")

		l := len(on)
		var i int

		for key, value := range on {
			var tableColumn string

			dotIndex := strings.Index(key, ".")
			if dotIndex != -1 {
				tableColumn = columnQuote(key)
			} else {
				if s.alias != "" {
					tableColumn = "`" + s.alias + "`.`" + key + "`"
				} else {
					tableColumn = "`" + s.table + "`.`" + key + "`"
				}
			}

			i++

			if joinAlias != "" {
				joinBuilder.WriteString(tableColumn)
				joinBuilder.WriteString("=`")
				joinBuilder.WriteString(joinAlias)
				joinBuilder.WriteString("`.`")
				joinBuilder.WriteString(value)

				if i == l {
					joinBuilder.WriteString("`")
				} else {
					joinBuilder.WriteString("` AND")
				}

			} else {
				joinBuilder.WriteString(tableColumn)
				joinBuilder.WriteString("=`")
				joinBuilder.WriteString(table)
				joinBuilder.WriteString("`.`")
				joinBuilder.WriteString(value)
				if i == l {
					joinBuilder.WriteString("`")
				} else {
					joinBuilder.WriteString("` AND")
				}
			}
		}
	}

	s.join = append(s.join, joinBuilder.String())
	return s
}

// where条件
func (s *Statement) whereImplode(dbType int, index *int, i int, key string, value interface{}, connector string) {
	v := reflect.ValueOf(value)

	isRelation, isSliceAndOR, relation := util.GetRelation(consts.DBTypeMySQL, key, v)

	if i == 0 {
		connector = ""
	}

	if isRelation {
		s.condBuilder.WriteString(" ")
		s.condBuilder.WriteString(connector)

		var subRelation = relation

		if relation == consts.NOT {
			s.condBuilder.WriteString(" NOT (")
			subRelation = consts.AND
		} else {
			s.condBuilder.WriteString(" (")
		}

		if isSliceAndOR {
			vLen := v.Len()
			for arrIndex := 0; arrIndex < vLen; arrIndex++ {
				var subKey = consts.AND
				if relation == consts.AND {
					subKey = consts.OR
				} else if relation == consts.NOT {
					subRelation = consts.OR
				}

				var subVal = types.Interface(v.Index(arrIndex))

				s.whereImplode(dbType, index, arrIndex, subKey, subVal, subRelation)
			}
		} else {
			var subI int
			for _, k := range v.MapKeys() {
				s.whereImplode(dbType, index, subI, k.String(), types.Interface(v.MapIndex(k)), subRelation)
				subI++
			}
		}

		s.condBuilder.WriteString(") ")
		return
	}

	column, operator, _, _, _, _, _ := util.OperatorMatch(key, false)

	if operator == "FUNC" { //函数
		s.condBuilder.WriteString(" ")
		s.condBuilder.WriteString(connector)
		s.condBuilder.WriteString(" ")
		s.condBuilder.WriteString(column)
		s.condBuilder.WriteString(" ")

		if value == nil {
		} else if types.IsArray(v) {
			vLen := v.Len()
			for vi := 0; vi < vLen; vi++ {
				s.condParams = append(s.condParams, value)
			}
		} else {
			s.condParams = append(s.condParams, value)
		}
	} else if column != "" {
		column = columnQuote(column)
		switch operator {
		case consts.OPGt, consts.OPGte, consts.OPLt, consts.OPLte:
			s.condBuilder.WriteString(connector)
			s.condBuilder.WriteString(column)
			s.condBuilder.WriteString(operator)
			s.condBuilder.WriteString(" ? ")
			s.condParams = append(s.condParams, value)
		case consts.OPNot:
			if value == nil {
				s.condBuilder.WriteString(connector)
				s.condBuilder.WriteString(column)
				s.condBuilder.WriteString(" IS NOT NULL ")
			} else if types.IsArray(v) {
				s.condBuilder.WriteString(connector)
				s.condBuilder.WriteString(column)
				s.condBuilder.WriteString(" NOT IN (")
				s.handleWhereInOrNot(v)
				s.condBuilder.WriteString(") ")
			} else {
				s.condBuilder.WriteString(connector)
				s.condBuilder.WriteString(column)
				s.condBuilder.WriteString(" != ? ")
				s.condParams = append(s.condParams, value)
			}
		case consts.OPNotLike, consts.OPLike:
			if types.IsArray(v) {
				s.condBuilder.WriteString(connector)
				s.condBuilder.WriteString(" (")
				s.handleLikeArray(v, column, operator)
				s.condBuilder.WriteString(") ")
			} else {
				if operator == "~" {
					s.condBuilder.WriteString(connector)
					s.condBuilder.WriteString(column)
					s.condBuilder.WriteString(" LIKE ? ")
				} else {
					s.condBuilder.WriteString(connector)
					s.condBuilder.WriteString(column)
					s.condBuilder.WriteString(" NOT LIKE ? ")
				}
				s.condParams = append(s.condParams, value)
			}
		case consts.OPBetween, consts.OPNotBetween:
			if types.IsArray(v) {
				if operator == consts.OPNotBetween {
					s.condBuilder.WriteString(connector)
					s.condBuilder.WriteString(" NOT (")
					s.condBuilder.WriteString(column)
					s.condBuilder.WriteString(" BETWEEN ? AND ?) ")
				} else {
					s.condBuilder.WriteString(connector)
					s.condBuilder.WriteString(" (")
					s.condBuilder.WriteString(column)
					s.condBuilder.WriteString(" BETWEEN ? AND ?) ")
				}

				s.condParams = append(s.condParams, v.Index(0).Interface())
				s.condParams = append(s.condParams, v.Index(1).Interface())
			}
		case "", consts.OPEqual:
			s.condBuilder.WriteString(" ")
			s.condBuilder.WriteString(connector)
			s.condBuilder.WriteString(column)
			if value == nil {
				s.condBuilder.WriteString(" IS NULL ")
			} else if types.IsArray(v) {
				s.condBuilder.WriteString(" IN (")
				s.handleWhereInOrNot(v)
				s.condBuilder.WriteString(") ")
			} else {
				s.condBuilder.WriteString("= ? ")
				s.condParams = append(s.condParams, value)
			}
		}
	} else if value != nil {
		if operator == "" {
			strValue, ok := value.(string)
			if ok {
				s.condBuilder.WriteString(" ")
				s.condBuilder.WriteString(connector)
				s.condBuilder.WriteString(" ")
				s.condBuilder.WriteString(strValue)
				s.condBuilder.WriteString(" ")
			}
		}
	}
}

// 处理 IN 或者 NOT IN 语句
func (s *Statement) handleWhereInOrNot(inValue reflect.Value) {
	l := inValue.Len()
	for i := 0; i < l; i++ {
		if i == 0 {
			s.condBuilder.WriteString("?")
		} else {
			s.condBuilder.WriteString(", ?")
		}

		s.condParams = append(s.condParams, inValue.Index(i).Interface())
	}
}

// 处理 LIKE 数组
func (s *Statement) handleLikeArray(value reflect.Value, column, operator string) {
	l := value.Len()

	for i := 0; i < l; i++ {
		if operator == consts.OPLike {
			if i != 0 {
				s.condBuilder.WriteString(" OR ")
			}
			s.condBuilder.WriteString(column)
			s.condBuilder.WriteString(" LIKE ? ")
		} else {
			if i != 0 {
				s.condBuilder.WriteString(" AND ")
			}
			s.condBuilder.WriteString(column)
			s.condBuilder.WriteString(" NOT LIKE ? ")
		}

		s.condParams = append(s.condParams, value.Index(i).Interface())
	}
}

// 列处理
func columnQuote(str string) (tableColumn string) {
	str = strings.TrimSpace(str)
	isSpecial, hasDot, dotIndex := hasSpecial(str)
	if isSpecial {
		return " " + str + " "
	}

	if hasDot {
		tableColumn = " `" + strings.TrimSpace(str[:dotIndex]) + "`.`" + strings.TrimSpace(str[dotIndex+1:]) + "` "
	} else {
		tableColumn = " `" + strings.TrimSpace(str) + "` "
	}

	return
}

func hasSpecial(str string) (isSpecial bool, hasDot bool, index int) {
	index = -1

	bStr := types.StringToBytes(str)
	for k, b := range bStr {
		switch b {
		case '.':
			hasDot = true
			if index == -1 {
				index = k
			}
		case ' ', '~', '!', '@', '$', '%', '^', '&', '*', '(', ')', '+',
			'|', '[', ']', '{', '}', ':', ';', '"', '?', ',', '<', '>', '/':
			isSpecial = true
		}
	}

	return
}
