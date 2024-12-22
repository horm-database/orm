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
	"context"
	"fmt"
	"strings"

	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/proto"
)

func (q *Query) createTable(ctx context.Context, shard []string,
	createSql string, ifNotExists bool) (interface{}, error) {
	if len(shard) == 0 {
		return nil, errs.Newf(errs.ErrClickhouseCreate, "create table shard is empty", shard)
	}

	if createSql == "" {
		return nil, errs.Newf(errs.ErrClickhouseCreate, "create table %s `s create sql not set", shard)
	}

	q.SQL = parseSql(createSql, shard[0], ifNotExists)

	rowsAffected, lastInsertID, err := q.execute(ctx)
	if err != nil {
		return nil, err
	}

	result := proto.ModRet{
		RowAffected: rowsAffected,
		ID:          proto.ID(fmt.Sprint(lastInsertID)),
	}

	return &result, nil
}

func parseSql(oldSql, shard string, ifNotExists bool) string {
	newSql := strings.Replace(oldSql, "`", "", 2)
	strArray := strings.Fields(strings.TrimSpace(newSql))

	//取 create proto 后的表名
	oldTableName := strings.Split(strArray[2], ".")
	newTableName := ""
	if len(oldTableName) > 1 {
		newTableName = oldTableName[1]
	} else {
		newTableName = oldTableName[0]
	}
	strTable := strArray[1]
	newStrTable := strArray[1]
	if ifNotExists {
		newStrTable = strArray[1] + " IF NOT EXISTS "
	}
	s := strings.Replace(newSql, strTable, newStrTable, 1)
	return strings.Replace(s, newTableName, shard, 1)
}
