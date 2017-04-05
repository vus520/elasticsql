package elasticsql

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/xwb1989/sqlparser"
)

var (
	Pretty = true
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

func handleUpdate(upd *sqlparser.Update) (string, string, error) {
	return "", "", errors.New("update not supported")
}

func handleInsert(ins *sqlparser.Insert) (string, string, error) {
	return "", "", errors.New("insert not supported")
}

func handleDelete(del *sqlparser.Delete) (string, string, error) {
	return "", "", errors.New("delete not supported")
}
