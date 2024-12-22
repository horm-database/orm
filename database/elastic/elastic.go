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
package elastic

import (
	"context"
	"strings"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/proto"
	"github.com/horm-database/common/types"
	"github.com/horm-database/orm/database/elastic/client"

	esv6 "github.com/olivere/elastic/v6"
	esv7 "github.com/olivere/elastic/v7"
)

// 插入新数据（V7 SDK 版本兼容 V6 操作）
func (q *Query) insert(ctx context.Context, op, id string) (*proto.ModRet, *proto.Detail, bool, error) {
	opts := []esv7.ClientOptionFunc{esv7.SetTraceLog(q)}
	clientV7, err := client.NewClientV7(false, q.Addr, opts...)

	if err != nil {
		return nil, nil, false, q.formatError("insert.NewClient", "", nil, err)
	}

	queryV7 := clientV7.Index().Index(q.Index[0]).Type(q.Type).BodyJson(q.Data).Refresh(q.Refresh)

	if id != "" {
		queryV7.Id(id)
	} else {
		_id, _ := types.GetString(q.Data, "_id")
		if _id != "" {
			queryV7.Id(_id)
			delete(q.Data, "_id")
		}
	}

	if q.Routing != "" {
		queryV7.Routing(q.Routing)
	}

	if op == consts.OpInsert {
		queryV7.OpType("create")
	}

	retV7, err := queryV7.Do(ctx)
	if err != nil {
		return nil, nil, false, q.logError("insert", id, nil, err)
	}

	q.logInfo("insert", id, nil)

	return &proto.ModRet{ID: proto.ID(retV7.Id), Version: retV7.Version, RowAffected: 1}, nil, false, nil
}

// 批量插入新数据，（V7 SDK 版本兼容 V6 操作）
func (q *Query) bulkInsert(ctx context.Context, op string) ([]*proto.ModRet, *proto.Detail, bool, error) {
	opts := []esv7.ClientOptionFunc{esv7.SetTraceLog(q)}
	clientV7, err := client.NewClientV7(false, q.Addr, opts...)
	if err != nil {
		return nil, nil, false, q.formatError("bulkInsert.NewClient", "", nil, err)
	}

	bulkService := clientV7.Bulk().Index(q.Index[0]).Type(q.Type).Refresh(q.Refresh)
	for _, v := range q.Datas {
		doc := esv7.NewBulkIndexRequest()

		_id, _ := types.GetString(v, "_id")
		if _id != "" {
			doc.Id(_id)
			delete(v, "_id")
		}

		if op == consts.OpInsert {
			doc.OpType("create")
		}

		bulkService.Add(doc.Doc(v))
	}

	if q.Routing != "" {
		bulkService.Routing(q.Routing)
	}

	retV7, err := bulkService.Do(ctx)
	if err != nil {
		return nil, nil, false, q.logError("bulkInsert", "", nil, err)
	}

	var affectInfo []*proto.ModRet
	var allFailed = true

	affectInfo = make([]*proto.ModRet, len(retV7.Items))
	for k, item := range retV7.Items {
		for _, idx := range item {
			var reason string
			if idx != nil && idx.Error != nil {
				reason = idx.Error.Reason
			}

			affectInfo[k] = q.getAffected(idx.Id, idx.Version, idx.Status, reason, &allFailed)
		}
	}

	if allFailed { //bulk 所有都失败了，取第一条错误。
		return nil, nil, false, q.logError("bulkInsert", "", nil,
			errs.NewDBError(affectInfo[0].Status, affectInfo[0].Reason))
	}

	q.logInfo("bulkInsert", "", nil)

	return affectInfo, nil, false, nil
}

// 通过ID更新数据（V7 SDK 版本兼容 V6 操作）
func (q *Query) updateByID(ctx context.Context, id string) (*proto.ModRet, *proto.Detail, bool, error) {
	var err error
	var clientV7 *esv7.Client
	var retV7 *esv7.UpdateResponse

	opts := []esv7.ClientOptionFunc{esv7.SetTraceLog(q)}
	clientV7, err = client.NewClientV7(false, q.Addr, opts...)

	if err != nil {
		return nil, nil, false, q.formatError("updateByID.NewClient", "", nil, err)
	}

	var script, scriptType string

	if q.ScriptType != "" {
		scriptType = q.ScriptType
	} else {
		scriptType = "inline"
	}

	if q.Script != "" {
		script = q.Script
	} else {
		// 不能这么弄，可能会导致与预期不符
		script = q.getScriptFromData()
	}

	retV7, err = clientV7.Update().Index(q.Index[0]).Type(q.Type).Id(id).Refresh(q.Refresh).
		Script(esv7.NewScript(script).Type(scriptType).Params(q.Data)).Do(ctx)

	if err != nil {
		return nil, nil, false, q.logError("updateByID", id, nil, err)
	}

	q.logInfo("updateByID", id, nil)

	return &proto.ModRet{ID: proto.ID(retV7.Id), Version: retV7.Version, RowAffected: 1}, nil, false, nil
}

// 通过query条件更新数据
func (q *Query) updateByQuery(ctx context.Context) (*proto.ModRet, *proto.Detail, bool, error) {
	var err error
	var clientV6 *esv6.Client
	var clientV7 *esv7.Client
	var retV6 *esv6.BulkIndexByScrollResponse
	var retV7 *esv7.BulkIndexByScrollResponse

	if q.Addr.Version == ElasticV6 {
		opts := []esv6.ClientOptionFunc{esv6.SetTraceLog(q)}
		clientV6, err = client.NewClientV6(false, q.Addr, opts...)
	} else {
		opts := []esv7.ClientOptionFunc{esv7.SetTraceLog(q)}
		clientV7, err = client.NewClientV7(false, q.Addr, opts...)
	}

	if err != nil {
		return nil, nil, false, q.formatError("updateByQuery.NewClient", "", nil, err)
	}

	var script, scriptType string

	if q.ScriptType != "" {
		scriptType = q.ScriptType
	} else {
		scriptType = "inline"
	}

	if q.Script != "" {
		script = q.Script
	} else {
		// 不能这么弄，可能会导致与预期不符
		script = q.getScriptFromData()
	}

	esFilter, err := esWhere(q.Where)
	if err != nil {
		return nil, nil, false, q.formatError("updateByQuery.esWhere", "", nil, err)
	}

	if clientV6 != nil {
		retV6, err = clientV6.UpdateByQuery().Index(q.Index...).Type(q.Type).Refresh(q.Refresh).Query(esFilter).
			Script(esv6.NewScript(script).Type(scriptType).Params(q.Data)).Do(ctx)
	} else {
		retV7, err = clientV7.UpdateByQuery().Index(q.Index...).Refresh(q.Refresh).Query(esFilter).
			Script(esv7.NewScript(script).Type(scriptType).Params(q.Data)).Do(ctx)
	}

	if err != nil {
		return nil, nil, false, q.logError("updateByQuery", "", esFilter, err)
	}

	q.logInfo("updateByQuery", "", nil)

	if retV6 != nil {
		return &proto.ModRet{RowAffected: retV6.Updated}, nil, false, nil
	} else {
		return &proto.ModRet{RowAffected: retV7.Updated}, nil, false, nil
	}
}

// 通过ID删除数据（V7 SDK 版本兼容 V6 操作）
func (q *Query) deleteByID(ctx context.Context, id string) (*proto.ModRet, *proto.Detail, bool, error) {
	opts := []esv7.ClientOptionFunc{esv7.SetTraceLog(q)}
	clientV7, err := client.NewClientV7(false, q.Addr, opts...)

	if err != nil {
		return nil, nil, false, q.formatError("deleteByID.NewClient", "", nil, err)
	}

	retV7, err := clientV7.Delete().Index(q.Index[0]).Id(id).Refresh(q.Refresh).Do(ctx)

	if err != nil {
		return nil, nil, false, q.logError("deleteByID", id, nil, err)
	}

	q.logInfo("deleteByID", id, nil)

	return &proto.ModRet{ID: proto.ID(retV7.Id), Version: retV7.Version, RowAffected: 1}, nil, false, nil
}

// 通过 query 条件删除数据（V7 SDK 版本兼容 V6 操作）
func (q *Query) deleteByQuery(ctx context.Context) (*proto.ModRet, *proto.Detail, bool, error) {
	opts := []esv7.ClientOptionFunc{esv7.SetTraceLog(q)}
	clientV7, err := client.NewClientV7(false, q.Addr, opts...)

	if err != nil {
		return nil, nil, false, q.formatError("deleteByQuery.NewClient", "", nil, err)
	}

	esFilter, err := esWhere(q.Where)
	if err != nil {
		return nil, nil, false, q.formatError("deleteByQuery.esWhere", "", nil, err)
	}

	retV7, err := clientV7.DeleteByQuery(q.Index...).Type(q.Type).Refresh(q.Refresh).Query(esFilter).Do(ctx)

	if err != nil {
		return nil, nil, false, q.logError("deleteByQuery", "", esFilter, err)
	}

	q.logInfo("deleteByQuery", "", esFilter)

	return &proto.ModRet{RowAffected: retV7.Deleted}, nil, false, nil
}

func (q *Query) search(ctx context.Context) ([]map[string]interface{}, *proto.Detail, bool, error) {
	var err error
	var clientV6 *esv6.Client
	var clientV7 *esv7.Client
	var retV6 *esv6.SearchResult
	var retV7 *esv7.SearchResult

	if q.Addr.Version == ElasticV6 {
		opts := []esv6.ClientOptionFunc{esv6.SetTraceLog(q)}
		clientV6, err = client.NewClientV6(true, q.Addr, opts...)
	} else {
		opts := []esv7.ClientOptionFunc{esv7.SetTraceLog(q)}
		clientV7, err = client.NewClientV7(true, q.Addr, opts...)
	}

	if err != nil {
		return nil, nil, false, q.formatError("search.NewClient", "", nil, err)
	}

	//search source
	searchSource, err := q.getSearchSource()
	if err != nil {
		return nil, nil, false, q.formatError("search.getSearchSource", "", nil,
			errs.New(errs.ErrDBParams, err.Error()))
	}

	if clientV6 != nil {
		retV6, err = clientV6.Search(q.Index...).Type(q.Type).Source(searchSource).Do(ctx)
	} else {
		retV7, err = clientV7.Search(q.Index...).Source(searchSource).Do(ctx)
	}

	if err != nil {
		return nil, nil, false, q.logError("search", "", searchSource, err)
	}

	q.logInfo("search", "", searchSource)

	if retV6 != nil {
		return formatSearchResultV6(retV6, retV6.Hits, q.Page, q.Size, q.Scroll)
	} else {
		return formatSearchResultV7(retV7, retV7.Hits, q.Page, q.Size, q.Scroll)
	}
}

func (q *Query) scrollByQuery(ctx context.Context) ([]map[string]interface{}, *proto.Detail, bool, error) {
	var err error
	var clientV6 *esv6.Client
	var clientV7 *esv7.Client
	var retV6 *esv6.SearchResult
	var retV7 *esv7.SearchResult

	if q.Addr.Version == ElasticV6 {
		opts := []esv6.ClientOptionFunc{esv6.SetTraceLog(q)}
		clientV6, err = client.NewClientV6(true, q.Addr, opts...)
	} else {
		opts := []esv7.ClientOptionFunc{esv7.SetTraceLog(q)}
		clientV7, err = client.NewClientV7(true, q.Addr, opts...)
	}

	if err != nil {
		return nil, nil, false, q.formatError("scrollByQuery.NewClient", "", nil, err)
	}

	//search source
	searchSource, err := q.getSearchSource()
	if err != nil {
		return nil, nil, false, q.formatError("scrollByQuery.getSearchSource", "", nil,
			errs.New(errs.ErrDBParams, err.Error()))
	}

	if clientV6 != nil {
		scrollSearch := clientV6.Scroll(q.Index...)

		if q.Scroll.Info != "" {
			scrollSearch.Scroll(q.Scroll.Info)
		}

		if q.Size > 0 {
			scrollSearch.Size(q.Size)
		}

		retV6, err = scrollSearch.Body(searchSource).Do(ctx)
	} else {
		scrollSearch := clientV7.Scroll(q.Index...)

		if q.Scroll.Info != "" {
			scrollSearch.Scroll(q.Scroll.Info)
		}

		if q.Size > 0 {
			scrollSearch.Size(q.Size)
		}

		retV7, err = scrollSearch.Body(searchSource).Do(ctx)
	}

	if err != nil {
		return nil, nil, false, q.logError("scrollByQuery", "", searchSource, err)
	}

	q.logInfo("scrollByQuery", "", searchSource)

	if retV6 != nil {
		return formatSearchResultV6(retV6, retV6.Hits, q.Page, q.Size, q.Scroll)
	} else {
		return formatSearchResultV7(retV7, retV7.Hits, q.Page, q.Size, q.Scroll)
	}
}

// 根据游标 ID 获取下一页
func (q *Query) scrollByScrollID(ctx context.Context) ([]map[string]interface{}, *proto.Detail, bool, error) {
	var err error
	var clientV6 *esv6.Client
	var clientV7 *esv7.Client
	var retV6 *esv6.SearchResult
	var retV7 *esv7.SearchResult

	if q.Addr.Version == ElasticV6 {
		opts := []esv6.ClientOptionFunc{esv6.SetTraceLog(q)}
		clientV6, err = client.NewClientV6(true, q.Addr, opts...)
	} else {
		opts := []esv7.ClientOptionFunc{esv7.SetTraceLog(q)}
		clientV7, err = client.NewClientV7(true, q.Addr, opts...)
	}

	if err != nil {
		return nil, nil, false, q.formatError("scrollByScrollID.NewClient", "", nil, err)
	}

	if clientV6 != nil {
		retV6, err = clientV6.Scroll(q.Index...).Type(q.Type).ScrollId(q.Scroll.ID).Do(ctx)
	} else {
		retV7, err = clientV7.Scroll(q.Index...).ScrollId(q.Scroll.ID).Do(ctx)
	}

	if err != nil {
		return nil, nil, false, q.logError("scrollByScrollID", q.Scroll.ID, nil, err)
	}

	q.logInfo("scrollByScrollID", q.Scroll.ID, nil)

	if retV6 != nil {
		return formatSearchResultV6(retV6, retV6.Hits, q.Page, q.Size, q.Scroll)
	} else {
		return formatSearchResultV7(retV7, retV7.Hits, q.Page, q.Size, q.Scroll)
	}
}

func (q *Query) getScriptFromData() string {
	script := strings.Builder{}

	for k := range q.Data {
		script.WriteString("ctx._source.")
		script.WriteString(k)
		script.WriteString("=params.")
		script.WriteString(k)
		script.WriteString(";")
	}

	return strings.TrimRight(script.String(), ";")
}

func (q *Query) getAffected(id string, version int64, status int,
	reason string, allFailed *bool) *proto.ModRet {
	var rowAffected int64

	if status >= 200 && status <= 299 {
		rowAffected = 1
		status = 0
	}

	if status == 0 {
		*allFailed = false
	} else {
		_ = q.logError("getAffected", id, nil, errs.NewDBError(status, reason))
	}

	return &proto.ModRet{
		ID:          proto.ID(id),
		Version:     version,
		RowAffected: rowAffected,
		Status:      status,
		Reason:      reason,
	}
}
