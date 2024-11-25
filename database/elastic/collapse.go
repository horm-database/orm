package elastic

import (
	"errors"

	"github.com/horm-database/common/types"
	esv7 "github.com/olivere/elastic/v7"
)

func getCollapse(params types.Map) (*esv7.CollapseBuilder, error) {
	collapse, ok, err := params.GetMap("collapse")
	if err != nil {
		return nil, errors.New("[collapse] is not map")
	}

	if ok {
		collapseField, _ := collapse.GetString("field")
		if collapseField == "" {
			return nil, errors.New("collapse`s param field is empty")
		}

		collapseBuilder := esv7.NewCollapseBuilder(collapseField)
		return collapseBuilder, nil
	}

	return nil, nil
}
