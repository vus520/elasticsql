package elasticsql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xwb1989/sqlparser"
)

var (
	Pretty = false
	Host   = "http://admin:admin@127.0.0.1:9200/"
	Index  = ""
)

// Convert will transform sql to elasticsearch dsl string
func Convert(sql string) (dsl string, table string, err error) {
	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		return "", "", err
	}

	//sql valid, start to handle
	switch stmt.(type) {
	case *sqlparser.Select:
		dsl, table, err = handleSelect(stmt.(*sqlparser.Select))
	case *sqlparser.Update:
		return handleUpdate(stmt.(*sqlparser.Update))
	case *sqlparser.Insert:
		return handleInsert(stmt.(*sqlparser.Insert))
	case *sqlparser.Delete:
		return handleDelete(stmt.(*sqlparser.Delete))
	}

	if err != nil {
		return "", "", err
	}

	// convertion dsl to json to check if it is right.
	if Pretty {
		var prettyJSON bytes.Buffer
		err = json.Indent(&prettyJSON, []byte(dsl), "", "  ")
		if err != nil {
			dsl = ""
		} else {
			dsl = string(prettyJSON.Bytes())
		}
	}

	return dsl, table, nil
}

// 格式化成 curl shell
func Curlshell(sql string) (dsl string, err error) {
	Pretty = false
	dsl, index, err := Convert(sql)

	if err != nil {
		return "", err
	}

	if Index != "" {
		index = Index
	}

	index = fmt.Sprintf(`{"index":["%s"],"ignore_unavailable":true}`, index)
	url := fmt.Sprintf(`'%s/_msearch?timeout=0&ignore_unavailable=true'`, Host)
	return fmt.Sprintf("curl %s -H 'Connection: keep-alive' -H 'Accept: application/json, text/plain, */*' -H 'Accept-Encoding: gzip, deflate' --data-binary $'%s\\n%s\\n' --compressed", url, index, dsl), nil
}

func handleUpdate(upd *sqlparser.Update) (string, string, error) {
	return "", "", errors.New("update not supported")
}

func handleInsert(ins *sqlparser.Insert) (string, string, error) {
	return "", "", errors.New("insert not supported")
}

func handleDelete(del *sqlparser.Delete) (string, string, error) {
	return "", "", errors.New("delete not supported")
}
