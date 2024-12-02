package orm

import (
	"github.com/horm-database/go-horm/horm"
)

// Type elastic search 版本 v7 以前有 type， v7之后 type 统一为 _doc
func (o *ORM) Type(typ string) *ORM {
	o.query.Type(typ)
	return o
}

// ID elastic 按照 _id 查询
func (o *ORM) ID(value interface{}) *ORM {
	o.query.ID(value)
	return o
}

// Scroll 查询，size 为每次 scroll 大小，where 为 scroll 条件。
func (o *ORM) Scroll(scroll string, size int, where ...horm.Where) *ORM {
	o.query.Scroll(scroll, size, where...)
	return o
}

// ScrollByID 根据 scrollID 滚动查询。
func (o *ORM) ScrollByID(scrollID string) *ORM {
	o.query.ScrollByID(scrollID)
	return o
}

// Refresh 更新数据立即刷新。
func (o *ORM) Refresh() *ORM {
	o.query.Refresh()
	return o
}

// HighLight 返回高亮
func (o *ORM) HighLight(fields []string, preTag, postTag string) *ORM {
	o.query.HighLight(fields, preTag, postTag)
	return o
}
