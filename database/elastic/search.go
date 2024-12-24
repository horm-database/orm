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

package elastic

import (
	"fmt"

	"github.com/horm-database/common/json"
	"github.com/horm-database/common/proto"
	"github.com/horm-database/common/proto/elastic"
	"github.com/horm-database/common/util"
	"github.com/samber/lo"

	esv6 "github.com/olivere/elastic/v6"
	esv7 "github.com/olivere/elastic/v7"
)

func (q *Query) getSearchSource() (interface{}, error) {
	searchSource := esv7.NewSearchSource()

	if len(q.Column) > 0 {
		searchSource.FetchSourceContext(esv7.NewFetchSourceContext(true).Include(q.Column...))
	}

	if q.HighLight != nil && len(q.HighLight.Fields) > 0 {
		searchSource.Highlight(getHighLight(q.HighLight))
	}

	esFilter, err := esWhere(q.Where)
	if err != nil {
		return nil, err
	}

	if esFilter != nil {
		searchSource.Query(esFilter)
	}

	collapse, err := getCollapse(q.Params)
	if err != nil {
		return nil, err
	} else if collapse != nil {
		searchSource.Collapse(collapse)
	}

	sorter := formatOrder(q.Order)
	searchSource.SortBy(sorter...)

	if q.Page > 0 {
		q.From = uint64((q.Page - 1) * q.Size)
	}

	if q.Size > 0 {
		searchSource.Size(q.Size).From(int(q.From))
	}

	iSearchSource, err := searchSource.Source()
	if err != nil {
		return nil, fmt.Errorf("search source error: %s", err.Error())
	}

	return iSearchSource, nil
}

func formatSearchResultV6(res *esv6.SearchResult, hits *esv6.SearchHits,
	page, size int, scroll *proto.Scroll) ([]map[string]interface{}, *proto.Detail, bool, error) {
	var result []map[string]interface{}

	for _, hit := range hits.Hits {
		data := map[string]interface{}{}
		hitSource, _ := hit.Source.MarshalJSON()
		_ = json.Api.Unmarshal(hitSource, &data)

		metaInfo := elastic.MetaInfo{}
		metaInfo.Score = hit.Score
		metaInfo.Index = hit.Index
		metaInfo.Id = hit.Id

		if hit.Nested != nil {
			metaInfo.Nested = &elastic.NestedHit{
				Field:  hit.Nested.Field,
				Offset: hit.Nested.Offset,
			}
		}

		data["_elastic"] = metaInfo

		if len(hit.InnerHits) > 0 {
			for name, innerHit := range hit.InnerHits {
				innerRet, detail, _, err := formatSearchResultV6(nil, innerHit.Hits, 0, 0, nil)
				if err != nil {
					return nil, nil, false, err
				}

				data[fmt.Sprintf("innerhit_%s", name)] = map[string]interface{}{
					"detail": detail,
					"data":   innerRet,
				}
			}
		}

		highLightResultHandle(data, hit.Highlight)

		result = append(result, data)
	}

	return result, getDetailV6(res, hits, page, size, scroll), len(result) == 0, nil
}

func formatSearchResultV7(res *esv7.SearchResult, hits *esv7.SearchHits,
	page, size int, scroll *proto.Scroll) ([]map[string]interface{}, *proto.Detail, bool, error) {
	var result []map[string]interface{}

	for _, hit := range hits.Hits {
		data := map[string]interface{}{}
		hitSource, _ := hit.Source.MarshalJSON()
		_ = json.Api.Unmarshal(hitSource, &data)

		metaInfo := elastic.MetaInfo{}
		metaInfo.Score = hit.Score
		metaInfo.Index = hit.Index
		metaInfo.Id = hit.Id

		if hit.Nested != nil {
			metaInfo.Nested = &elastic.NestedHit{}
			copyNestedFromEs(metaInfo.Nested, hit.Nested)
		}

		data["_elastic"] = metaInfo

		if len(hit.InnerHits) > 0 {
			for name, innerHit := range hit.InnerHits {
				innerRet, detail, _, err := formatSearchResultV7(nil, innerHit.Hits, 0, 0, nil)
				if err != nil {
					return nil, nil, false, err
				}

				data[fmt.Sprintf("innerhit_%s", name)] = map[string]interface{}{
					"detail": detail,
					"data":   innerRet,
				}
			}
		}

		highLightResultHandle(data, hit.Highlight)

		result = append(result, data)
	}

	return result, getDetailV7(res, hits, page, size, scroll), len(result) == 0, nil
}

func getDetailV6(res *esv6.SearchResult, hits *esv6.SearchHits, page, size int, scroll *proto.Scroll) *proto.Detail {
	var detail *proto.Detail

	if page > 0 || scroll != nil || res == nil {
		total := uint64(hits.TotalHits)

		detail = &proto.Detail{Page: page, Size: size}
		detail.Total = total

		if size > 0 {
			detail.TotalPage = util.CalcTotalPage(total, size)
		}

		if scroll != nil {
			detail.Scroll = &proto.Scroll{Info: scroll.Info, ID: res.ScrollId}
		}

		detail.Extras = map[string]interface{}{}
		detail.Extras["total"] = hits.TotalHits
		detail.Extras["max_score"] = hits.MaxScore

		if res != nil {
			detail.Extras["took"] = res.TookInMillis
			detail.Extras["timed_out"] = res.TimedOut
			detail.Extras["_shards"] = res.Shards
		}
	}

	return detail
}

func getDetailV7(res *esv7.SearchResult, hits *esv7.SearchHits, page, size int, scroll *proto.Scroll) *proto.Detail {
	var detail *proto.Detail

	if page > 0 || scroll != nil || res == nil {
		total := uint64(hits.TotalHits.Value)

		detail = &proto.Detail{Page: page, Size: size}
		detail.Total = total

		if size > 0 {
			detail.TotalPage = util.CalcTotalPage(total, size)
		}

		if scroll != nil {
			detail.Scroll = &proto.Scroll{Info: scroll.Info, ID: res.ScrollId}
		}

		detail.Extras = map[string]interface{}{}
		detail.Extras["total"] = hits.TotalHits
		detail.Extras["max_score"] = hits.MaxScore

		if res != nil {
			detail.Extras["took"] = res.TookInMillis
			detail.Extras["timed_out"] = res.TimedOut
			detail.Extras["_shards"] = res.Shards
		}
	}

	return detail
}

func copyNestedFromEs(nestedHit *elastic.NestedHit, esNested *esv7.NestedHit) {
	nestedHit.Field = esNested.Field
	nestedHit.Offset = esNested.Offset
	if esNested.Child != nil {
		nestedHit.Child = &elastic.NestedHit{}
		copyNestedFromEs(nestedHit.Child, esNested.Child)
	}
}

func formatOrder(orders []string) []esv7.Sorter {
	orderArr := util.FormatOrders(orders)

	ret := []esv7.Sorter{}
	for _, order := range orderArr {
		ret = append(ret, &esv7.SortInfo{Field: order.Field, Ascending: order.Ascending})
	}

	return ret
}

func unknownFieldError(desc string, params map[string]interface{}, supportField ...string) error {
	for field := range params {
		if lo.IndexOf(supportField, field) == -1 {
			return fmt.Errorf("[%s] query does not support field [%s]", desc, field)
		}
	}

	return nil
}
