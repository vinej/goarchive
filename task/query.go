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

func query_excel_memory(task *Task) {
	db, _ := con.GetDB(task.Connection)
	p1 := task.Parameters[0]
	mem := GetMemory(p1.SourceName)
	for _, r := range mem.rows {
		mr := *r.(*map[string]string)
		cmd := task.Command
		out := task.OutputName
		for i := 0; i < len(p1.Field); i++ {
			ma := adjust_quote(mr[p1.Field[i]])
			cmd = strings.ReplaceAll(cmd, p1.Name[0], ma)
			out = "p" + ma + "_" + out
		}
		if len(task.Parameters) == 2 {
			p2 := task.Parameters[1]
			mem2 := GetMemory(p2.SourceName)
			isFirst := true
			for r2 := 0; r2 < len(mem2.rows); r2++ {
				mr := *mem2.rows[r2].(*map[string]string)
				cmd2 := cmd
				out2 := out
				for i := 0; i < len(p2.Field); i++ {
					// if same field fir i and i + 1, means that the first one is for previous record
					// and the second one for the current record
					// we must skip to record i+1 because there is no record[-1]
					if i+1 < len(p2.Field) && p2.Field[i] == p2.Field[i+1] {
						// period type, goto second record
						if isFirst {
							r2 = r2 + 1
							isFirst = false
						}
						// use previous record
						mr2 := *mem2.rows[r2-1].(*map[string]string)
						ma2 := adjust_quote(mr2[p2.Field[i]])
						cmd2 = strings.ReplaceAll(cmd2, p2.Name[i], ma2)
						out2 = "p" + ma2 + "_" + out2

						i = i + 1
						// take next field
						// use current record
						mr2 = *mem2.rows[r2].(*map[string]string)
						ma2 = adjust_quote(mr2[p2.Field[i]])
						cmd2 = strings.ReplaceAll(cmd2, p2.Name[i], ma2)
						out2 = "p" + ma2 + "_" + out2
					} else {
						ma2 := adjust_quote(mr[p2.Field[i]])
						cmd2 = strings.ReplaceAll(cmd2, p2.Name[i], ma2)
						out2 = "p" + ma2 + "_" + out2
					}
				}
				util.QuerySaveExcel(task.Name, db, cmd2, out2)
			}
		} else {
			util.QuerySaveExcel(task.Name, db, cmd, out)
		}
	}
}

func query_excel(task *Task) {
	if len(task.Parameters) > 0 {
		query_excel_memory(task)
	} else {
		db, _ := con.GetDB(task.Connection)
		util.QuerySaveExcel(task.Name, db, task.Command, task.OutputName)
	}
}

func GetMemory(name string) *Memory {
	return mapqry[name]
}

/*
	query the database and put the result into memory
	no parameter managed by the option
*/
func query_memory(task *Task) {
	db, err := con.GetDB(task.Connection)
	if err == nil {
		m := new(Memory)
		m.columnNames, m.rows = util.Query(db, task.Command)
		mapqry[task.Name] = m
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
