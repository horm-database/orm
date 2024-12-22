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

const (
	TypeInt = iota
	TypeInt8
	TypeInt16
	TypeInt32
	TypeInt64
	TypeUInt
	TypeUInt8
	TypeUInt16
	TypeUInt32
	TypeUInt64
	TypeFloat32
	TypeFloat64
	TypeString
	TypeTime
	TypeBytes
	TypeJSON
)

var MySQLTypeMap = map[string]int8{
	"INT":       TypeInt,
	"TINYINT":   TypeInt8,
	"SMALLINT":  TypeInt16,
	"MEDIUMINT": TypeInt32,
	"BIGINT":    TypeInt64,
	"BIT":       TypeBytes,
	"FLOAT":     TypeFloat32,
	"DOUBLE":    TypeFloat64,
	"DECIMAL":   TypeFloat64,
	"VARCHAR":   TypeString,
	"CHAR":      TypeString,
	"TEXT":      TypeString,
	"BLOB":      TypeBytes,
	"BINARY":    TypeBytes,
	"VARBINARY": TypeBytes,
	"TIME":      TypeString,
	"DATE":      TypeTime,
	"DATETIME":  TypeTime,
	"TIMESTAMP": TypeTime,
	"JSON":      TypeJSON,
}

var ClickHouseTypeMap = map[string]int8{
	"Int":         TypeInt,
	"Int8":        TypeInt8,
	"Int16":       TypeInt16,
	"Int32":       TypeInt32,
	"Int64":       TypeInt64,
	"UInt":        TypeUInt,
	"UInt8":       TypeUInt8,
	"UInt16":      TypeUInt16,
	"UInt32":      TypeUInt32,
	"UInt64":      TypeUInt64,
	"Float":       TypeFloat32,
	"Float32":     TypeFloat32,
	"Float64":     TypeFloat64,
	"Decimal":     TypeFloat64,
	"String":      TypeString,
	"FixedString": TypeString,
	"UUID":        TypeString,
	"DateTime":    TypeTime,
	"DateTime64":  TypeTime,
	"Date":        TypeTime,
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

	ns.String = types.InterfaceToString(value)

	return nil
}

// Scan NullInt 类型实现 mysql 引擎查询赋值接口
func (ns *NullInt) Scan(value interface{}) error {
	if value == nil {
		ns.IsNull = true
		return nil
	}

	i64, err := types.InterfaceToInt64(value)
	ns.Int = i64
	return err
}

// Scan NullUint 类型实现 mysql 引擎查询赋值接口
func (ns *NullUint) Scan(value interface{}) error {
	if value == nil {
		ns.IsNull = true
		return nil
	}

	ui64, err := types.InterfaceToUint64(value)
	ns.Uint = ui64
	return err
}

// Scan NullBool 类型实现 mysql 引擎查询赋值接口
func (ns *NullBool) Scan(value interface{}) error {
	if value == nil {
		ns.IsNull = true
		return nil
	}

	ns.Bool = types.InterfaceToBool(value)
	return nil
}

// Scan NullFloat 类型实现 mysql 引擎查询赋值接口
func (ns *NullFloat) Scan(value interface{}) error {
	if value == nil {
		ns.IsNull = true
		return nil
	}

	f64, err := types.InterfaceToFloat64(value)
	ns.Float = f64

	return err
}

// Scan NullTime 类型实现 mysql 引擎查询赋值接口，
func (ns *NullTime) Scan(value interface{}) error {
	if value == nil {
		ns.IsNull = true
		return nil
	}

	ns.Time = time.Time{}

	if ret, ok := value.(time.Time); ok {
		ns.Time = ret
		return nil
	}

	if ret, ok := value.(*time.Time); ok {
		ns.Time = *ret
		return nil
	}

	tmp := types.InterfaceToString(value)
	if tmp == "" {
		return nil
	}

	var i time.Time
	var err error

	if ns.TimeLayout == "" {
		ns.TimeLayout = defaultTimeLayout
	}

	if loc != nil {
		i, err = time.ParseInLocation(ns.TimeLayout, tmp, loc)
	} else {
		i, err = time.Parse(ns.TimeLayout, tmp)
	}

	if err == nil {
		ns.Time = i
	}

	return nil
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
	"time":               "string",
	"date":               "time.Time",
	"datetime":           "time.Time",
	"timestamp":          "time.Time",
	"binary":             "[]byte",
	"varbinary":          "[]byte",
	"json":               "interface{}",
}
