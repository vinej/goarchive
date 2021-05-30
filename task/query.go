package task

import (
	con "jyv.com/goarchive/connection"
	util "jyv.com/goarchive/util"
)

func RunQuery(task Task) {
	if task.Output == "excel" {
		db, err := con.GetDB(task.Connection)
		if err == nil {
			util.QuerySaveExcel(task.Name, db, task.Sql, task.File)
		}
	}
}
