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
