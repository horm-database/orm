module github.com/horm/orm

go 1.14

replace github.com/horm/common => ../common

replace github.com/horm/go-horm/horm => ../go-horm/horm

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.13.4
	github.com/go-sql-driver/mysql v1.7.1
	github.com/gomodule/redigo v1.8.9
	github.com/horm/common v0.0.0-00010101000000-000000000000
	github.com/horm/go-horm/horm v0.0.0-00010101000000-000000000000
	github.com/olivere/elastic v6.2.37+incompatible // indirect
	github.com/olivere/elastic/v6 v6.2.1
	github.com/olivere/elastic/v7 v7.0.32
)
