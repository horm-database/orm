// Package obj 提供user数据库表在go程序的结构体映射，一般用于model层调用
package obj

import (
	"time"

	"github.com/horm-database/common/util"
)

type TblDB struct {
	Id        int       `orm:"id,int,omitempty" json:"id,omitempty"`
	Name      string    `orm:"name,string" json:"name,omitempty"`                    // 数据库名称
	Intro     string    `orm:"intro,string" json:"intro,omitempty"`                  // 中文简介
	Desc      string    `orm:"desc,string" json:"desc,omitempty"`                    // 详细介绍
	ProductID int       `orm:"product_id,int,omitempty" json:"product_id,omitempty"` // 产品id
	Creator   uint64    `orm:"creator,uint64,omitempty" json:"creator,omitempty"`    // Creator
	Manager   string    `orm:"manager,string" json:"manager,omitempty"`              // 管理员，多个逗号分隔
	Status    int8      `orm:"status,int8" json:"status,omitempty"`                  // 1-正常 2-下线
	CreatedAt time.Time `orm:"created_at,datetime,omitempty" json:"created_at"`      // 记录创建时间
	UpdatedAt time.Time `orm:"updated_at,datetime,omitempty" json:"updated_at"`      // 记录最后修改时间

	// db params
	WriteTimeoutTmp int  `orm:"write_timeout,int" json:"write_timeout,omitempty"` // 写超时（毫秒）
	ReadTimeoutTmp  int  `orm:"read_timeout,int" json:"read_timeout,omitempty"`   // 读超时（毫秒）
	WarnTimeoutTmp  int  `orm:"warn_timeout,int" json:"warn_timeout,omitempty"`   // 告警超时（ms），如果请求耗时超过这个时间，就会打 warning 日志
	OmitErrorTmp    int8 `orm:"omit_error,int8" json:"omit_error,omitempty"`      // 是否忽略 error 日志，0-否 1-是
	DebugTmp        int8 `orm:"debug,int8" json:"debug,omitempty"`                // 是否开启 debug 日志，正常的数据库请求也会被打印到日志，0-否 1-是，会造成海量日志，慎重开启

	// db address
	Type       int    `orm:"type,int" json:"type,omitempty"`                  // 数据库类型 0-nil（仅执行插件） 1-elastic 2-mongo 3-redis 10-mysql 11-postgresql 12-clickhouse 13-oracle 14-DB2 15-sqlite
	Version    string `orm:"version,string" json:"version,omitempty"`         // 数据库版本，比如elastic v6，v7
	Network    string `orm:"network,string" json:"network,omitempty"`         // network
	Address    string `orm:"address,string" json:"address,omitempty"`         // address
	BakAddress string `orm:"bak_address,string" json:"bak_address,omitempty"` // backup address

	Addr *util.DBAddress
}

type TblTable struct {
	Id          int       `orm:"id,int,omitempty" json:"id"`
	Name        string    `orm:"name,string" json:"name"`                           // 数据名称（执行单元名）db::name 全局唯一
	Intro       string    `orm:"intro,string" json:"intro"`                         // 中文简介
	Desc        string    `orm:"desc,string" json:"desc"`                           // 详细描述
	TableVerify string    `orm:"table_verify,string" json:"table_verify"`           // 表校验，为空时不校验，默认同 name，即只允许访问 name 表/索引
	DB          int       `orm:"db,int" json:"db"`                                  // 所属数据库
	Definition  string    `orm:"definition,string" json:"definition"`               // 表定义
	TableFields string    `orm:"table_fields,string" json:"table_fields"`           // 表字段
	TableIndexs string    `orm:"table_indexs,string" json:"table_indexs"`           // 表索引
	Status      int8      `orm:"status,int8" json:"status"`                         // 1-正常 2-下线
	Creator     uint64    `orm:"creator,uint64,omitempty" json:"creator,omitempty"` // Creator
	CreatedAt   time.Time `orm:"created_at,datetime,omitempty" json:"created_at"`   // 记录创建时间
	UpdatedAt   time.Time `orm:"updated_at,datetime,omitempty" json:"updated_at"`   // 记录最后修改时间
}
