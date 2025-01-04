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
	"reflect"

	"github.com/horm-database/common/consts"
	"github.com/horm-database/common/types"
	"github.com/horm-database/common/util"

	esv7 "github.com/olivere/elastic/v7"
)

const (
	BoolTypeFilter = iota
	BoolTypeMust
	BoolTypeShould
	BoolTypeMustNot
)

type EsFilter interface {
	Source() (interface{}, error)
}

// where 查询条件解析为 es query 语句
func esWhere(where map[string]interface{}) (EsFilter, error) {
	q := esv7.NewBoolQuery()

	if len(where) == 0 {
		return q, nil
	}

	subQueryMaps := map[int8][]esv7.Query{}
	for key, value := range where {
		boolType, subQuery, err := esWhereImplode(key, value, consts.AND)
		if err != nil {
			return nil, err
		}
		subQueryMaps[boolType] = append(subQueryMaps[boolType], subQuery)
	}

	for typ, subQuerys := range subQueryMaps {
		switch typ {
		case BoolTypeFilter:
			q.Filter(subQuerys...)
		case BoolTypeMust:
			q.Must(subQuerys...)
		case BoolTypeShould:
			q.Should(subQuerys...)
		default:
			q.MustNot(subQuerys...)
		}
	}

	return q, nil
}

func esWhereImplode(key string, value interface{}, connector string) (int8, EsFilter, error) {
	rv := reflect.ValueOf(value)

	isRelation, isSliceAndOR, relation := util.GetRelation(consts.DBTypeElastic, key, rv)

	if isRelation {
		var searchQuery EsFilter
		var err error

		switch relation {
		case "NESTED":
			searchQuery, err = getNested(value.(map[string]interface{}))
		case "PARENT_ID":
			searchQuery, err = getParentID(value.(map[string]interface{}))
		case "HAS_CHILD":
			searchQuery, err = getHasChild(value.(map[string]interface{}))
		case "HAS_PARENT":
			searchQuery, err = getHasParent(value.(map[string]interface{}))
		}

		if err != nil {
			return 0, nil, err
		}

		if searchQuery != nil {
			switch connector {
			case consts.OR:
				return BoolTypeShould, searchQuery, nil
			case consts.NOT:
				return BoolTypeFilter, esv7.NewBoolQuery().MustNot(searchQuery), nil
			default:
				return BoolTypeFilter, searchQuery, nil
			}
		}

		subBool := esv7.NewBoolQuery()

		subQueryMaps := map[int8][]esv7.Query{}

		var subRelation = relation
		if relation == consts.NOT {
			subRelation = consts.AND
		}

		if isSliceAndOR {
			rvLen := rv.Len()
			for index := 0; index < rvLen; index++ {
				var subKey = consts.AND
				if relation == consts.AND {
					subKey = consts.OR
				} else if relation == consts.NOT {
					subRelation = consts.OR
				}

				var subVal = types.Interface(rv.Index(index))

				boolType, subQuery, err := esWhereImplode(subKey, subVal, subRelation)
				if err != nil {
					return 0, nil, err
				}
				subQueryMaps[boolType] = append(subQueryMaps[boolType], subQuery)
			}
		} else {
			for _, k := range rv.MapKeys() {
				boolType, subQuery, err := esWhereImplode(k.String(), types.Interface(rv.MapIndex(k)), subRelation)
				if err != nil {
					return 0, nil, err
				}
				subQueryMaps[boolType] = append(subQueryMaps[boolType], subQuery)
			}
		}

		for typ, subQuery := range subQueryMaps {
			switch typ {
			case BoolTypeFilter:
				subBool.Filter(subQuery...)
			case BoolTypeMust:
				subBool.Must(subQuery...)
			case BoolTypeShould:
				subBool.Should(subQuery...)
			default:
				subBool.MustNot(subQuery...)
			}
		}

		switch connector {
		case consts.AND:
			return BoolTypeFilter, subBool, nil
		case consts.OR:
			return BoolTypeShould, subBool, nil
		case consts.NOT:
			return BoolTypeFilter, esv7.NewBoolQuery().MustNot(subBool), nil
		}
	}

	column, operator, subOperator, matchType, minShouldMatch, boost, slop := util.OperatorMatch(key, true)

	var boolType int8 = BoolTypeFilter
	if connector == consts.OR {
		boolType = BoolTypeShould
	}

	if column != "" {
		switch operator {
		case consts.OPGt:
			return boolType, esv7.NewRangeQuery(column).Gt(value), nil
		case consts.OPGte:
			return boolType, esv7.NewRangeQuery(column).Gte(value), nil
		case consts.OPLt:
			return boolType, esv7.NewRangeQuery(column).Lt(value), nil
		case consts.OPLte:
			return boolType, esv7.NewRangeQuery(column).Lte(value), nil
		case consts.OPBetween, consts.OPNotBetween:
			if types.IsArray(rv) {
				v1 := rv.Index(0).Interface()
				v2 := rv.Index(1).Interface()

				betweenQuery := esv7.NewRangeQuery(column).Gte(v1).Lte(v2)

				if operator == consts.OPBetween {
					return boolType, betweenQuery, nil
				} else {
					if connector == consts.AND {
						return BoolTypeMustNot, betweenQuery, nil
					} else {
						return BoolTypeShould, esv7.NewBoolQuery().MustNot(betweenQuery), nil
					}
				}
			}
		case consts.OPNot:
			if connector == consts.AND {
				if value == nil {
					return BoolTypeFilter, esv7.NewExistsQuery(column), nil
				} else if types.IsArray(rv) {
					vArr, _ := types.ToArray(value)
					return BoolTypeMustNot, esv7.NewTermsQuery(column, vArr...), nil
				} else {
					return BoolTypeMustNot, esv7.NewTermsQuery(column, value), nil
				}
			} else {
				if value == nil {
					return BoolTypeShould, esv7.NewExistsQuery(column), nil
				} else if types.IsArray(rv) {
					vArr, err := types.ToArray(value)
					if err != nil {
						return BoolTypeFilter, nil, err
					}
					return BoolTypeShould, esv7.NewBoolQuery().MustNot(esv7.NewTermsQuery(column, vArr...)), nil
				} else {
					return BoolTypeShould, esv7.NewBoolQuery().MustNot(esv7.NewTermsQuery(column, value)), nil
				}
			}
		case consts.OPMatchPhrase, consts.OPNotMatchPhrase, consts.OPMatch,
			consts.OPNotMatch, consts.OPLike, consts.OPNotLike:
			var matchQuery esv7.Query

			if types.IsArray(rv) {
				vArr, _ := types.ToArray(value)
				matchQuerys := []esv7.Query{}
				for _, val := range vArr {
					tmpQuery := getMatch(operator, column, subOperator, minShouldMatch, matchType, boost, slop, val)

					matchQuerys = append(matchQuerys, tmpQuery)
				}

				matchQuery = esv7.NewBoolQuery().Should(matchQuerys...)
			} else {
				matchQuery = getMatch(operator, column, subOperator, minShouldMatch, matchType, boost, slop, value)
			}

			if connector == consts.AND {
				if operator[0] == '!' {
					return BoolTypeMustNot, matchQuery, nil
				} else {
					return BoolTypeMust, matchQuery, nil
				}
			} else {
				if operator[0] == '!' {
					return BoolTypeShould, esv7.NewBoolQuery().MustNot(matchQuery), nil
				} else {
					return BoolTypeShould, matchQuery, nil
				}
			}
		case "":
			if value == nil {
				if connector == consts.AND {
					return BoolTypeMustNot, esv7.NewExistsQuery(column), nil
				} else {
					return BoolTypeShould, esv7.NewBoolQuery().MustNot(esv7.NewExistsQuery(column)), nil
				}
			} else if types.IsArray(rv) {
				arrV, _ := types.ToArray(value)
				return boolType, esv7.NewTermsQuery(column, arrV...), nil
			} else {
				return boolType, esv7.NewTermsQuery(column, value), nil
			}
		}
	}

	return BoolTypeFilter, nil, nil
}

func getMatch(operator, column, subOperator, minShouldMatch,
	matchType string, boost float64, slop int, value interface{}) EsFilter {
	switch operator {
	case consts.OPMatchPhrase, consts.OPNotMatchPhrase:
		matchPhraseQuery := esv7.NewMatchPhraseQuery(column, value)
		if boost > 0 {
			matchPhraseQuery.Boost(boost)
		}
		if slop > 0 {
			matchPhraseQuery.Slop(slop)
		}

		return matchPhraseQuery
	case consts.OPMatch, consts.OPNotMatch:
		matchQuery := esv7.NewMatchQuery(column, value)
		if boost > 0 {
			matchQuery.Boost(boost)
		}
		if subOperator != "" {
			matchQuery.Operator(subOperator)
		}
		if minShouldMatch != "" {
			matchQuery.MinimumShouldMatch(minShouldMatch)
		}
		return matchQuery
	default:
		switch matchType {
		case "wildcard":
			return esv7.NewWildcardQuery(column, value.(string))
		case "regexp":
			return esv7.NewRegexpQuery(column, value.(string))
		default:
			return esv7.NewPrefixQuery(column, value.(string))
		}
	}
}
