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
	"time"

	"github.com/horm-database/common/types"
)

var MySQLTypeMap = map[string]types.Type{
	"INT":                types.TypeInt,
	"TINYINT":            types.TypeInt8,
	"SMALLINT":           types.TypeInt16,
	"MEDIUMINT":          types.TypeInt32,
	"BIGINT":             types.TypeInt64,
	"UNSIGNED INT":       types.TypeUint,
	"UNSIGNED TINYINT":   types.TypeUint8,
	"UNSIGNED SMALLINT":  types.TypeUint16,
	"UNSIGNED MEDIUMINT": types.TypeUint32,
	"UNSIGNED BIGINT":    types.TypeUint64,
	"BIT":                types.TypeBytes,
	"FLOAT":              types.TypeFloat,
	"DOUBLE":             types.TypeDouble,
	"DECIMAL":            types.TypeDouble,
	"VARCHAR":            types.TypeString,
	"CHAR":               types.TypeString,
	"TEXT":               types.TypeString,
	"BLOB":               types.TypeBytes,
	"BINARY":             types.TypeBytes,
	"VARBINARY":          types.TypeBytes,
	"TIME":               types.TypeString,
	"DATE":               types.TypeTime,
	"DATETIME":           types.TypeTime,
	"TIMESTAMP":          types.TypeTime,
	"JSON":               types.TypeJSON,
}

var ClickHouseTypeMap = map[string]types.Type{
	"Int":         types.TypeInt,
	"Int8":        types.TypeInt8,
	"Int16":       types.TypeInt16,
	"Int32":       types.TypeInt32,
	"Int64":       types.TypeInt64,
	"UInt":        types.TypeUint,
	"UInt8":       types.TypeUint8,
	"UInt16":      types.TypeUint16,
	"UInt32":      types.TypeUint32,
	"UInt64":      types.TypeUint64,
	"Float":       types.TypeFloat,
	"Float32":     types.TypeFloat,
	"Float64":     types.TypeDouble,
	"Decimal":     types.TypeDouble,
	"String":      types.TypeString,
	"FixedString": types.TypeString,
	"UUID":        types.TypeString,
	"DateTime":    types.TypeTime,
	"DateTime64":  types.TypeTime,
	"Date":        types.TypeTime,
}

// NullString 数据库 varchar NULL 类型
type NullString struct {
	String string
	IsNull bool
}

// NullInt 数据库 int NULL 类型
type NullInt struct {
	Int    int64
	IsNull bool
}

// NullUint 数据库 uint NULL 类型
type NullUint struct {
	Uint   uint64
	IsNull bool
}

// NullFloat 数据库 double/float NULL 类型
type NullFloat struct {
	Float  float64
	IsNull bool
}

// NullBool 数据库 bool NULL 类型
type NullBool struct {
	Bool   bool
	IsNull bool
}

// NullTime 数据库 time NULL 类型
type NullTime struct {
	Time       time.Time
	IsNull     bool
	TimeLayout string
}

// Scan NullString 类型实现 mysql 引擎查询赋值接口
func (ns *NullString) Scan(value interface{}) error {
	if value == nil {
		ns.IsNull = true
		return nil
	}

	ns.String = types.ToString(value)

	return nil
}

// Scan NullInt 类型实现 mysql 引擎查询赋值接口
func (ns *NullInt) Scan(value interface{}) error {
	if value == nil {
		ns.IsNull = true
		return nil
	}

	i64, err := types.ToInt64(value)
	ns.Int = i64
	return err
}

// Scan NullUint 类型实现 mysql 引擎查询赋值接口
func (ns *NullUint) Scan(value interface{}) error {
	if value == nil {
		ns.IsNull = true
		return nil
	}

	ui64, err := types.ToUint64(value)
	ns.Uint = ui64
	return err
}

// Scan NullBool 类型实现 mysql 引擎查询赋值接口
func (ns *NullBool) Scan(value interface{}) error {
	if value == nil {
		ns.IsNull = true
		return nil
	}

	ns.Bool = types.ToBool(value)
	return nil
}

// Scan NullFloat 类型实现 mysql 引擎查询赋值接口
func (ns *NullFloat) Scan(value interface{}) error {
	if value == nil {
		ns.IsNull = true
		return nil
	}

	f64, err := types.ToFloat64(value)
	ns.Float = f64

	return err
}

// Scan NullTime 类型实现 mysql 引擎查询赋值接口，
func (ns *NullTime) Scan(value interface{}) (err error) {
	if value == nil {
		ns.IsNull = true
		return nil
	}

	ns.Time, err = types.ParseTime(value, loc, ns.TimeLayout)
	return err
}

// mysql 类型转化为 golang 结构体
var typeMysqlToStruct = map[string]string{
	"bool":               "bool",
	"int":                "int",
	"integer":            "int",
	"tinyint":            "int8",
	"smallint":           "int16",
	"mediumint":          "int32",
	"bigint":             "int64",
	"int unsigned":       "uint",
	"integer unsigned":   "uint",
	"tinyint unsigned":   "unit8",
	"smallint unsigned":  "uint16",
	"mediumint unsigned": "uint32",
	"bigint unsigned":    "uint64",
	"bit":                "[]byte",
	"float":              "float32",
	"double":             "float64",
	"decimal":            "float64",
	"enum":               "string",
	"set":                "string",
	"varchar":            "string",
	"char":               "string",
	"tinytext":           "string",
	"mediumtext":         "string",
	"text":               "string",
	"longtext":           "string",
	"blob":               "[]byte",
	"tinyblob":           "[]byte",
	"mediumblob":         "[]byte",
	"longblob":           "[]byte",
	"binary":             "[]byte",
	"varbinary":          "[]byte",
	"time":               "string",
	"date":               "time.Time",
	"datetime":           "time.Time",
	"timestamp":          "time.Time",
	"json":               "interface{}",
}
