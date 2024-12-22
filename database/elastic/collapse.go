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
