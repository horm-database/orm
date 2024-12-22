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

package obj

import (
	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/proto"
	"github.com/horm-database/orm/database/sql/client"
	"github.com/samber/lo"
)

// Tree 查询分析树
type Tree struct {
	Name string // 查询名

	Next     *Tree   // 同层级下一个查询节点
	Last     *Tree   // 同层级上一个查询节点
	Sub      *Tree   // 嵌套子查询节点
	Parent   *Tree   // 父查询节点
	Real     *Tree   // 真实查询信息存储节点
	SubQuery []*Tree // 子查询实际执行节点

	HasSub  bool // 是否包含子查询
	IsSub   bool // 本节点是否是子查询
	InTrans bool // 本节点是否在事务中

	// 返回结果
	Finished  int8          // 查询单元是否执行完毕 0-未执行 1-已执行 2-待回滚
	Error     error         // 查询错误
	IsNil     bool          // 结果为空
	Detail    *proto.Detail // 查询细节信息
	Result    interface{}   // 查询结果
	ParentRet interface{}   // 父查询的多条结果记录分别存入各个子查询。

	// 查询信息
	Unit     *proto.Unit // 查询单元
	Property *Property   // query 基础属性

	// 事务
	TransInfo *TransInfo // 事务信息
}

// Property query 基础属性
type Property struct {
	Op     string
	Name   string
	Alias  string
	Key    string
	Path   string
	Tables []string
	DB     *TblDB
	Table  *TblTable
}

// TransInfo 事务信息
type TransInfo struct {
	Trans     *Tree                    // 事务下的查询节点
	Rollback  bool                     // 是否回滚
	TxClients map[string]client.Client // sql 事务执行 client，（目前仅支持 sql 的事务 commit、rollback）
	DBs       []string                 // 参与事务的所有 db。
}

func (ti *TransInfo) GetTxClient(dsn string) client.Client {
	if len(ti.TxClients) == 0 {
		return nil
	}

	tmp, _ := ti.TxClients[dsn]
	return tmp
}

func (ti *TransInfo) SetTxClient(dsn string, cli client.Client) {
	if ti.TxClients == nil {
		ti.TxClients = map[string]client.Client{}
	}

	ti.TxClients[dsn] = cli

	if lo.IndexOf(ti.DBs, dsn) == -1 {
		ti.DBs = append(ti.DBs, dsn)
	}
}

func (ti *TransInfo) ResetTxClient() {
	ti.Rollback = false
	ti.TxClients = map[string]client.Client{}
	ti.DBs = []string{}
}

// SetProperty 设置查询属性
func (node *Tree) SetProperty(property *Property) *Tree {
	node.Property = property
	return node
}

// GetReal 获取执行节点的真实查询信息所在节点。
func (node *Tree) GetReal() *Tree {
	if node.Real != nil {
		return node.Real
	}
	return node
}

// IsArray 是否获取数组
func (node *Tree) IsArray() bool {
	return node.Property.Op == consts.OpFindAll
}

// GetName 获取数据名称（执行单元名）
func (node *Tree) GetName() string {
	return node.Property.Name
}

// GetAlias 获取执行单元别名
func (node *Tree) GetAlias() string {
	return node.Property.Alias
}

// GetKey 获取执行单元 key
func (node *Tree) GetKey() string {
	return node.Property.Key
}

// GetPath 获取执行单元路径
func (node *Tree) GetPath() string {
	return node.Property.Path
}

// Tables 获取表名
func (node *Tree) Tables() []string {
	return node.Property.Tables
}

// GetDB 获取数据库配置
func (node *Tree) GetDB() *TblDB {
	return node.Property.DB
}

// GetTable 获取表信息
func (node *Tree) GetTable() *TblTable {
	return node.Property.Table
}

// GetUnit 获取 unit
func (node *Tree) GetUnit() *proto.Unit {
	return node.Unit
}

// GetOp 获取 op
func (node *Tree) GetOp() string {
	return node.Property.Op
}

// IsTransaction 是否是事务
func (node *Tree) IsTransaction() bool {
	return node.Property.Op == consts.OpTransaction
}

// GetErrorCode 获取 error code
func (node *Tree) GetErrorCode() int {
	return errs.Code(node.Error)
}

// GetErrorMsg 获取 error msg
func (node *Tree) GetErrorMsg() string {
	return node.Error.Error()
}

// IsSuccess 是否执行成功
func (node *Tree) IsSuccess() bool {
	return node.Error == nil
}
