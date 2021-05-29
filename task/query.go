package task

import (
	"context"

	con "jyv.com/goarchive/connection"
	util "jyv.com/goarchive/util"
)

type Query struct {
	id string

	name        string
	description string
	connection  string
	sql         string
	output      string
	file        string
}

func (q Query) Run(ctx context.Context) {
	if q.output == "excel" {
		db, err := con.GetDB(q.connection)
		if err != nil {
			util.QuerySaveExcel(q.name, db, q.sql, q.file)
		}
	}
}
