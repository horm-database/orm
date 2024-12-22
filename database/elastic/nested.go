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
	"errors"

	"github.com/horm-database/common/types"
	esv7 "github.com/olivere/elastic/v7"
)

func getNested(nested map[string]interface{}) (*esv7.NestedQuery, error) {
	path, _ := types.GetString(nested, "path")

	where, ok, err := types.GetMap(nested, "query")
	if err != nil {
		return nil, errors.New("nested`s param [query] is not map")
	}

	if !ok {
		return nil, errors.New("nested`s param [query] is not input")
	}

	query, err := esWhere(where)
	if err != nil {
		return nil, err
	}

	nestedQuery := esv7.NewNestedQuery(path, query)

	innerHitsMap, ok, err := types.GetMap(nested, "inner_hits")
	if err != nil {
		return nil, errors.New("nested`s param [inner_hits] is not map")
	}

	if ok {
		if len(innerHitsMap) == 0 {
			nestedQuery.InnerHit(esv7.NewInnerHit())
		} else {
			innerHit, err := getInnerHit(innerHitsMap)
			if err != nil {
				return nil, err
			}
			nestedQuery.InnerHit(innerHit)
		}
	}

	return nestedQuery, unknownFieldError("nested", nested, "path", "query", "inner_hits")
}
