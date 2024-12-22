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

func getInnerHits(innerHits map[string]interface{}) (map[string]*esv7.InnerHit, error) {
	ret := map[string]*esv7.InnerHit{}

	for name, v := range innerHits {
		params, ok := v.(map[string]interface{})
		if !ok {
			return nil, errors.New("[inner_hits] is not map")
		}

		innerHit, err := getInnerHit(params)
		if err != nil {
			return nil, err
		}

		ret[name] = innerHit
	}

	return ret, nil
}

func getInnerHit(params map[string]interface{}) (*esv7.InnerHit, error) {
	innerHit := esv7.NewInnerHit()

	name, _ := types.GetString(params, "name")
	innerHit.Name(name)

	path, _ := types.GetString(params, "path")
	innerHit.Path(path)

	typ, _ := types.GetString(params, "type")
	innerHit.Type(typ)

	size, _, _ := types.GetInt(params, "size")
	innerHit.Size(size)

	from, _, _ := types.GetInt(params, "from")
	innerHit.From(from)

	orders, _, err := types.GetStringArray(params, "order")
	if err != nil {
		return nil, errors.New("inner_hits`s param [order] is not string array")
	}

	sorter := formatOrder(orders)
	innerHit.SortBy(sorter...)

	column, ok, err := types.GetStringArray(params, "column")
	if err != nil {
		return nil, errors.New("inner_hits`s param [column] is not string array")
	}

	if ok {
		query := esv7.NewFetchSourceContext(true).Include(column...)
		innerHit.FetchSourceContext(query)
	}

	return innerHit, unknownFieldError("inner_hits", params, "name", "path", "type", "size", "from", "order", "column")
}
