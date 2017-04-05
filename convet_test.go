package elasticsql

import (
	"github.com/vus520/elasticsql"
	"testing"
)

//currently not support join syntax
var sqls = []string{
	"select field from `tables` where process_id= 1",
}

var dsls = []string{
	`{"query" : {"bool" : {"must" : [{"match" : {"process_id" : {"query" : "1", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
}

func Test_Convert(t *testing.T) {
	for k, str := range sqls {
		elasticsql.Pretty = false
		a, b, c := elasticsql.Convert(str)

		if c != nil {
			t.Error("Error catched:" + str + c.Error())
			continue
		}

		if b != "tables" {
			t.Error("Table not matched:" + b)
			continue
		}

		if dsls[k] != a {
			t.Error("Result not matched:" + a)
		}
	}
}
