package task

import (
	"strings"

	con "jyv.com/goarchive/connection"
	util "jyv.com/goarchive/util"
)

func adjust_quote(name string) string {
	if name[0] == '\'' && name[1] != '\'' {
		name = name[1:]
	}
	if name[len(name)-1] == '\'' && name[len(name)-2] != '\'' {
		name = name[:len(name)-1]
	}
	name = strings.ReplaceAll(name, "''", "'")
	return name
}

func RunQuery(task Task) {
	if task.OutputType == "excel" {
		db, err := con.GetDB(task.Connection)
		if err == nil {
			if len(task.Parameters) > 0 {
				for _, p := range task.Parameters {
					switch p.Source {
					case "array":
						array := GetArray(p.SourceName)
						for _, a := range array {
							ma := adjust_quote(a)
							cmd := strings.ReplaceAll(task.Command, p.Name, a)
							out := "p" + ma + "_" + task.OutputName
							util.QuerySaveExcel(task.Name, db, cmd, out)
						}
					}
				}
			} else {
				util.QuerySaveExcel(task.Name, db, task.Command, task.OutputName)
			}
		}
	}
}
