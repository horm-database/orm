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

	"github.com/horm-database/common/errs"
	"github.com/horm-database/common/types"
	esv7 "github.com/olivere/elastic/v7"
)

// HighLight 返回结果高亮
type HighLight struct {
	Field   string `json:"field,omitempty"`    // 要加高亮的字段
	PreTag  string `json:"pre_tag,omitempty"`  // 高亮前标签
	PostTag string `json:"post_tag,omitempty"` // 高亮后标签
	Replace bool   `json:"replace,omitempty"`  // 是否替换原字段，就是原字段内容不返回，只返回带标签的内容，减少内容大小。默认为 false
}

func getHighLightParam(params types.Map) ([]*HighLight, error) {
	highLights, ok, err := params.GetMapArray("highlights")
	if err != nil {
		return nil, errs.Newf(errs.ErrParamInvalid, "get highlight from params error: %v", err)
	}

	if ok && len(highLights) > 0 {
		ret := []*HighLight{}

		for _, highLight := range highLights {
			field, _ := highLight.GetString("field")
			preTag, _ := highLight.GetString("pre_tag")
			postTag, _ := highLight.GetString("post_tag")
			replace, _ := highLight.GetBool("replace")
			ret = append(ret, &HighLight{
				Field:   field,
				PreTag:  preTag,
				PostTag: postTag,
				Replace: replace,
			})
		}

		return ret, nil
	}

	return nil, nil
}

func highLightResultHandle(data map[string]interface{}, hitHighLight map[string][]string, highLights []*HighLight) {
	if len(hitHighLight) > 0 {
		for _, highLight := range highLights {
			key := fmt.Sprintf("highlight_%s", highLight.Field)
			if v, ok := hitHighLight[highLight.Field]; ok {
				if highLight.Replace {
					delete(data, highLight.Field)
				}
				data[key] = v
			}
		}
	}
}

func getHighLight(highLights []*HighLight) *esv7.Highlight {
	fields := []*esv7.HighlighterField{}
	for _, highLight := range highLights {
		h := esv7.NewHighlighterField(highLight.Field)
		h.PreTags(highLight.PreTag)
		h.PostTags(highLight.PostTag)

		fields = append(fields, h)
	}

	h := esv7.NewHighlight().Fields(fields...)

	return h
}
