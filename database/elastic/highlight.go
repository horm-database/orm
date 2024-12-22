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

	"github.com/horm-database/common/types"
	esv7 "github.com/olivere/elastic/v7"
)

// HighLight 返回结果高亮
type HighLight struct {
	Fields  []string `json:"fields,omitempty"`   // 要加高亮的字段
	PreTag  string   `json:"pre_tag,omitempty"`  // 高亮前标签
	PostTag string   `json:"post_tag,omitempty"` // 高亮后标签
}

func getHighLightParam(params types.Map) *HighLight {
	highLight, ok, err := params.GetMap("highlight")
	if ok && err != nil && highLight != nil {
		fields, _, _ := highLight.GetStringArray("fields")
		preTag, _ := highLight.GetString("pre_tag")
		postTag, _ := highLight.GetString("post_tag")

		return &HighLight{
			Fields:  fields,
			PreTag:  preTag,
			PostTag: postTag,
		}
	}

	return nil
}

func highLightResultHandle(data map[string]interface{}, hitHighLight map[string][]string) {
	if len(hitHighLight) > 0 {
		for k, v := range hitHighLight {
			key := fmt.Sprintf("highlight_%s", k)
			data[key] = v
		}
	}
}

func getHighLight(highLight *HighLight) *esv7.Highlight {
	fields := []*esv7.HighlighterField{}
	for _, field := range highLight.Fields {
		fields = append(fields, esv7.NewHighlighterField(field))
	}

	h := esv7.NewHighlight().Fields(fields...)

	if highLight.PreTag != "" && highLight.PostTag != "" {
		h.PreTags(highLight.PreTag).PostTags(highLight.PostTag)
	}

	return h
}
