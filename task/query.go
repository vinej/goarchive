package task

import (
	"strings"

	con "jyv.com/goarchive/connection"
	util "jyv.com/goarchive/util"
)

type Memory struct {
	columnNames []string
	rows        []interface{}
}

var mapqry map[string]*Memory = make(map[string]*Memory)

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

func query_excel(task *Task) {
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
				case "memory":
					mem := GetMemory(p.SourceName)
					for _, r := range mem.rows {
						mr := *r.(*map[string]string)
						ma := adjust_quote(mr[p.Field])
						cmd := strings.ReplaceAll(task.Command, p.Name, ma)
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

func GetMemory(name string) *Memory {
	return mapqry[name]
}

func query_memory(task *Task) {
	db, err := con.GetDB(task.Connection)
	if err == nil {
		if len(task.Parameters) > 0 {
			for _, p := range task.Parameters {
				switch p.Source {
				case "array":
					array := GetArray(p.SourceName)
					for _, a := range array {
						cmd := strings.ReplaceAll(task.Command, p.Name, a)
						m := new(Memory)
						m.columnNames, m.rows = util.Query(db, cmd)
						mapqry[task.Name] = m
					}
				case "memory":
					mem := GetMemory(p.SourceName)
					for _, r := range mem.rows {
						mr := r.(map[string]string)
						cmd := strings.ReplaceAll(task.Command, p.Name, mr[p.Field])
						m := new(Memory)
						m.columnNames, m.rows = util.Query(db, cmd)
						mapqry[task.Name] = m
					}
				}
			}
		} else {
			m := new(Memory)
			m.columnNames, m.rows = util.Query(db, task.Command)
			mapqry[task.Name] = m
		}
	}
}

func RunQuery(task *Task) {
	switch task.OutputType {
	case "excel":
		query_excel(task)
	case "memory":
		query_memory(task)
	}
}
