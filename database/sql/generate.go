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

package sql

import (
	"fmt"
	"strings"

	"github.com/horm-database/common/consts"
)

func (s *Statement) GetSQL() string {
	switch s.op {
	case consts.OpInsert:
		return s.InsertSQL()
	case consts.OpReplace:
		return s.ReplaceSQL()
	case consts.OpUpdate:
		return s.UpdateSQL()
	case consts.OpDelete:
		return s.DeleteSQL()
	case consts.OpFind, consts.OpFindAll:
		return s.FindSQL()
	case consts.OpCount:
		return s.CountSQL()
	}

	return ""
}

// InsertSQL 创建 insert 语句
func (s *Statement) InsertSQL() string {
	return fmt.Sprint("INSERT INTO `", s.table, "` ", s.set)
}

// ReplaceSQL 创建 replace 语句
func (s *Statement) ReplaceSQL() string {
	return fmt.Sprint("REPLACE INTO `", s.table, "` ", s.set)
}

// UpdateSQL 创建 update 语句
func (s *Statement) UpdateSQL() string {
	sqlBuilder := strings.Builder{}

	if s.dbType == consts.DBTypeClickHouse {
		sqlBuilder.WriteString("ALTER TABLE `")
		sqlBuilder.WriteString(s.table)
		sqlBuilder.WriteString("` UPDATE ")
		sqlBuilder.WriteString(s.set)
	} else {
		sqlBuilder.WriteString("UPDATE `")
		sqlBuilder.WriteString(s.table)
		sqlBuilder.WriteString("` SET ")
		sqlBuilder.WriteString(s.set)
	}
	if s.where != "" {
		sqlBuilder.WriteString(" WHERE ")
		sqlBuilder.WriteString(s.where)
	}
	if s.GetOrder() != "" {
		sqlBuilder.WriteString(" ORDER BY ")
		sqlBuilder.WriteString(s.GetOrder())
	}
	if s.limit > 0 {
		sqlBuilder.WriteString(" LIMIT ")
		sqlBuilder.WriteString(fmt.Sprint(s.limit))
	}
	if s.offset > 0 {
		sqlBuilder.WriteString(" OFFSET ")
		sqlBuilder.WriteString(fmt.Sprint(s.offset))
	}
	return sqlBuilder.String()
}

// DeleteSQL 创建 delete 语句
func (s *Statement) DeleteSQL() string {
	sqlBuilder := strings.Builder{}

	if s.dbType == consts.DBTypeClickHouse {
		sqlBuilder.WriteString("ALTER TABLE `")
		sqlBuilder.WriteString(s.table)
		sqlBuilder.WriteString("` DELETE ")
	} else {
		sqlBuilder.WriteString("DELETE FROM `")
		sqlBuilder.WriteString(s.table)
		sqlBuilder.WriteString("` ")
	}

	if s.where != "" {
		sqlBuilder.WriteString(" WHERE ")
		sqlBuilder.WriteString(s.where)
	}
	if s.GetOrder() != "" {
		sqlBuilder.WriteString(" ORDER BY ")
		sqlBuilder.WriteString(s.GetOrder())
	}
	if s.limit > 0 {
		sqlBuilder.WriteString(" LIMIT ")
		sqlBuilder.WriteString(fmt.Sprint(s.limit))
	}
	if s.offset > 0 {
		sqlBuilder.WriteString(" OFFSET ")
		sqlBuilder.WriteString(fmt.Sprint(s.offset))
	}

	return sqlBuilder.String()
}

// CountSQL 创建 count 语句
func (s *Statement) CountSQL() (sql string) {
	origin := s.selects

	if s.selects == "*" || s.selects == "" {
		s.selects = "count(*)"
	} else {
		str := "count("
		if s.distinct {
			str = "count( DISTINCT"
		}
		if len(s.selects) <= 6 {
			s.selects = fmt.Sprint(str, s.selects, ")")
		} else {
			s.selects = strings.TrimSpace(s.selects)
			prefix := strings.ToLower(s.selects[:6])
			if prefix != "count(" {
				s.selects = fmt.Sprint(str, s.selects, ")")
			}
		}
	}

	sqlBuilder := s.findSQL()
	s.selects = origin
	return sqlBuilder.String()
}

// FindSQL 组装mysql 语句
func (s *Statement) FindSQL() (sql string) {
	sqlBuilder := s.findSQL()

	if s.GetOrder() != "" {
		sqlBuilder.WriteString(" ORDER BY ")
		sqlBuilder.WriteString(s.GetOrder())
	}

	if s.limit > 0 {
		sqlBuilder.WriteString(" LIMIT ")
		sqlBuilder.WriteString(fmt.Sprint(s.limit))
	}

	if s.offset > 0 {
		sqlBuilder.WriteString(" OFFSET ")
		sqlBuilder.WriteString(fmt.Sprint(s.offset))
	}

	if s.forUpdate != "" {
		sqlBuilder.WriteString(" ")
		sqlBuilder.WriteString(s.forUpdate)
	}

	return sqlBuilder.String()
}

func (s *Statement) findSQL() *strings.Builder {
	sqlBuilder := strings.Builder{}

	if s.alias != "" {
		sqlBuilder.WriteString(" SELECT ")
		sqlBuilder.WriteString(s.selects)
		sqlBuilder.WriteString(" FROM `")
		sqlBuilder.WriteString(s.table)
		sqlBuilder.WriteString("` AS `")
		sqlBuilder.WriteString(s.alias)
		sqlBuilder.WriteString("`")
	} else {
		sqlBuilder.WriteString(" SELECT ")
		sqlBuilder.WriteString(s.selects)
		sqlBuilder.WriteString(" FROM `")
		sqlBuilder.WriteString(s.table)
		sqlBuilder.WriteString("`")
	}

	if s.indexHints != "" {
		sqlBuilder.WriteString(" ")
		sqlBuilder.WriteString(s.indexHints)
	}

	if len(s.join) > 0 {
		for _, j := range s.join {
			sqlBuilder.WriteString(" ")
			sqlBuilder.WriteString(j)
			sqlBuilder.WriteString(" ")
		}
	}

	if s.where != "" {
		sqlBuilder.WriteString(" WHERE ")
		sqlBuilder.WriteString(s.where)
	}

	if s.group != "" {
		sqlBuilder.WriteString(" GROUP BY ")
		sqlBuilder.WriteString(s.group)
	}

	if s.having != "" {
		sqlBuilder.WriteString(" HAVING ")
		sqlBuilder.WriteString(s.having)
	}

	return &sqlBuilder
}
