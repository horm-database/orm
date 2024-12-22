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

func getParentID(parentID map[string]interface{}) (*esv7.ParentIdQuery, error) {
	typ, _ := types.GetString(parentID, "type")
	id, _ := types.GetString(parentID, "id")

	if typ == "" {
		return nil, errors.New("parent_id`s param [type] is empty")
	}

	if id == "" {
		return nil, errors.New("parent_id`s param [id] is empty")
	}

	return esv7.NewParentIdQuery(typ, id), unknownFieldError("parent_id", parentID, "type", "id")
}

func getHasChild(hasChild map[string]interface{}) (*esv7.HasChildQuery, error) {
	typ, _ := types.GetString(hasChild, "type")

	if typ == "" {
		return nil, errors.New("has_child`s param [type] is empty")
	}

	where, ok, err := types.GetMap(hasChild, "query")
	if err != nil {
		return nil, errors.New("has_child`s param [query] is not map")
	}

	if !ok {
		return nil, errors.New("has_child`s param [query] is not input")
	}

	query, err := esWhere(where)
	if err != nil {
		return nil, err
	}

	hasChildQuery := esv7.NewHasChildQuery(typ, query)

	minChildren, ok, err := types.GetInt(hasChild, "min_children")
	if err != nil {
		return nil, errors.New("has_child`s param [min_children] is not int")
	}

	if ok {
		hasChildQuery.MinChildren(minChildren)
	}

	maxChildren, ok, err := types.GetInt(hasChild, "max_children")
	if err != nil {
		return nil, errors.New("has_child`s param [max_children] is not int")
	}

	if ok {
		hasChildQuery.MaxChildren(maxChildren)
	}

	innerHitsMap, ok, err := types.GetMap(hasChild, "inner_hits")
	if err != nil {
		return nil, errors.New("has_child`s param [inner_hits] is not map")
	}

	if ok {
		if len(innerHitsMap) == 0 {
			hasChildQuery.InnerHit(esv7.NewInnerHit())
		} else {
			innerHit, err := getInnerHit(innerHitsMap)
			if err != nil {
				return nil, err
			}
			hasChildQuery.InnerHit(innerHit)
		}
	}

	return hasChildQuery, unknownFieldError("has_child", hasChild,
		"type", "query", "min_children", "max_children", "inner_hits")
}

func getHasParent(hasParent map[string]interface{}) (*esv7.HasParentQuery, error) {
	parentType, _ := types.GetString(hasParent, "parent_type")

	if parentType == "" {
		return nil, errors.New("has_parent`s param [parent_type] is empty")
	}

	where, ok, err := types.GetMap(hasParent, "query")
	if err != nil {
		return nil, errors.New("has_parent`s param [query] is not map")
	}

	if !ok {
		return nil, errors.New("has_parent`s param [query] is not input")
	}

	query, err := esWhere(where)
	if err != nil {
		return nil, err
	}

	//esv7.NewScriptScoreQuery(query)

	hasParentQuery := esv7.NewHasParentQuery(parentType, query)

	innerHitsMap, ok, err := types.GetMap(hasParent, "inner_hits")
	if err != nil {
		return nil, errors.New("has_parent`s param [inner_hits] is not map")
	}

	if ok {
		if len(innerHitsMap) == 0 {
			hasParentQuery.InnerHit(esv7.NewInnerHit())
		} else {
			innerHit, err := getInnerHit(innerHitsMap)
			if err != nil {
				return nil, err
			}
			hasParentQuery.InnerHit(innerHit)
		}
	}

	return hasParentQuery, unknownFieldError("has_parent", hasParent, "parent_type", "query", "inner_hits")
}
