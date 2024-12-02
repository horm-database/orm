package sql

import (
	"fmt"
	"strings"

	"github.com/horm-database/common/consts"
)

// InsertSQL 创建 insert 语句
func InsertSQL(statement *Statement) string {
	return fmt.Sprint("INSERT INTO `", statement.table, "` ", statement.set)
}

// ReplaceSQL 创建 replace 语句
func ReplaceSQL(statement *Statement) string {
	return fmt.Sprint("REPLACE INTO `", statement.table, "` ", statement.set)
}

// UpdateSQL 创建 update 语句
func UpdateSQL(statement *Statement) string {
	sqlBuilder := strings.Builder{}

	if statement.dbType == consts.DBTypeClickHouse {
		sqlBuilder.WriteString("ALTER TABLE `")
		sqlBuilder.WriteString(statement.table)
		sqlBuilder.WriteString("` UPDATE ")
		sqlBuilder.WriteString(statement.set)
	} else {
		sqlBuilder.WriteString("UPDATE `")
		sqlBuilder.WriteString(statement.table)
		sqlBuilder.WriteString("` SET ")
		sqlBuilder.WriteString(statement.set)
	}
	if statement.where != "" {
		sqlBuilder.WriteString(" WHERE ")
		sqlBuilder.WriteString(statement.where)
	}
	if statement.GetOrder() != "" {
		sqlBuilder.WriteString(" ORDER BY ")
		sqlBuilder.WriteString(statement.GetOrder())
	}
	if statement.limit > 0 {
		sqlBuilder.WriteString(" LIMIT ")
		sqlBuilder.WriteString(fmt.Sprint(statement.limit))
	}
	if statement.offset > 0 {
		sqlBuilder.WriteString(" OFFSET ")
		sqlBuilder.WriteString(fmt.Sprint(statement.offset))
	}
	return sqlBuilder.String()
}

// DeleteSQL 创建 delete 语句
func DeleteSQL(statement *Statement) string {
	sqlBuilder := strings.Builder{}

	if statement.dbType == consts.DBTypeClickHouse {
		sqlBuilder.WriteString("ALTER TABLE `")
		sqlBuilder.WriteString(statement.table)
		sqlBuilder.WriteString("` DELETE ")
	} else {
		sqlBuilder.WriteString("DELETE FROM `")
		sqlBuilder.WriteString(statement.table)
		sqlBuilder.WriteString("` ")
	}

	if statement.where != "" {
		sqlBuilder.WriteString(" WHERE ")
		sqlBuilder.WriteString(statement.where)
	}
	if statement.GetOrder() != "" {
		sqlBuilder.WriteString(" ORDER BY ")
		sqlBuilder.WriteString(statement.GetOrder())
	}
	if statement.limit > 0 {
		sqlBuilder.WriteString(" LIMIT ")
		sqlBuilder.WriteString(fmt.Sprint(statement.limit))
	}
	if statement.offset > 0 {
		sqlBuilder.WriteString(" OFFSET ")
		sqlBuilder.WriteString(fmt.Sprint(statement.offset))
	}

	return sqlBuilder.String()
}

// CountSQL 创建 count 语句
func CountSQL(statement *Statement) (sql string) {
	origin := statement.selects

	if statement.selects == "*" || statement.selects == "" {
		statement.selects = "count(*)"
	} else {
		str := "count("
		if statement.distinct {
			str = "count( DISTINCT"
		}
		if len(statement.selects) <= 6 {
			statement.selects = fmt.Sprint(str, statement.selects, ")")
		} else {
			statement.selects = strings.TrimSpace(statement.selects)
			prefix := strings.ToLower(statement.selects[:6])
			if prefix != "count(" {
				statement.selects = fmt.Sprint(str, statement.selects, ")")
			}
		}
	}

	sqlBuilder := findSQL(statement)
	statement.selects = origin
	return sqlBuilder.String()
}

// FindSQL 组装mysql 语句
func FindSQL(statement *Statement) (sql string) {
	sqlBuilder := findSQL(statement)

	if statement.GetOrder() != "" {
		sqlBuilder.WriteString(" ORDER BY ")
		sqlBuilder.WriteString(statement.GetOrder())
	}

	if statement.limit > 0 {
		sqlBuilder.WriteString(" LIMIT ")
		sqlBuilder.WriteString(fmt.Sprint(statement.limit))
	}

	if statement.offset > 0 {
		sqlBuilder.WriteString(" OFFSET ")
		sqlBuilder.WriteString(fmt.Sprint(statement.offset))
	}

	if statement.forUpdate != "" {
		sqlBuilder.WriteString(" ")
		sqlBuilder.WriteString(statement.forUpdate)
	}

	return sqlBuilder.String()
}

func findSQL(statement *Statement) *strings.Builder {
	sqlBuilder := strings.Builder{}

	if statement.alias != "" {
		sqlBuilder.WriteString(" SELECT ")
		sqlBuilder.WriteString(statement.selects)
		sqlBuilder.WriteString(" FROM `")
		sqlBuilder.WriteString(statement.table)
		sqlBuilder.WriteString("` AS `")
		sqlBuilder.WriteString(statement.alias)
		sqlBuilder.WriteString("`")
	} else {
		sqlBuilder.WriteString(" SELECT ")
		sqlBuilder.WriteString(statement.selects)
		sqlBuilder.WriteString(" FROM `")
		sqlBuilder.WriteString(statement.table)
		sqlBuilder.WriteString("`")
	}

	if statement.indexHints != "" {
		sqlBuilder.WriteString(" ")
		sqlBuilder.WriteString(statement.indexHints)
	}

	if len(statement.join) > 0 {
		for _, j := range statement.join {
			sqlBuilder.WriteString(" ")
			sqlBuilder.WriteString(j)
			sqlBuilder.WriteString(" ")
		}
	}

	if statement.where != "" {
		sqlBuilder.WriteString(" WHERE ")
		sqlBuilder.WriteString(statement.where)
	}

	if statement.group != "" {
		sqlBuilder.WriteString(" GROUP BY ")
		sqlBuilder.WriteString(statement.group)
	}

	if statement.having != "" {
		sqlBuilder.WriteString(" HAVING ")
		sqlBuilder.WriteString(statement.having)
	}

	return &sqlBuilder
}
