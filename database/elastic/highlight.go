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
