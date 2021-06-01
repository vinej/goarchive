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

func query_excel_array(task *Task) {

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
			switch p2.Source {
			case "memory":
				mem := GetMemory(p2.SourceName)
				isFirst := true
				for r := 0; r < len(mem.rows); r++ {
					mr := *mem.rows[r].(*map[string]string)
					cmd2 := cmd
					out2 := out
					for i := 0; i < len(p2.Field); i++ {
						if p2.Field[i][0] == '-' {
							if isFirst {
								isFirst = false
								continue
							} else {
								mr := *mem.rows[r-1].(*map[string]string)
								ma := adjust_quote(mr[p2.Field[i][1:]])
								cmd2 = strings.ReplaceAll(cmd2, p2.Name[i], ma)
								out2 = "p" + ma + "_" + out2
							}
						} else {
							ma := adjust_quote(mr[p2.Field[i]])
							cmd2 = strings.ReplaceAll(cmd2, p2.Name[i], ma)
							out2 = "p" + ma + "_" + out2
						}
					}
					util.QuerySaveExcel(task.Name, db, cmd2, out2)
				}
			}
		} else {
			util.QuerySaveExcel(task.Name, db, cmd, out)
		}
	}
}

func query_excel(task *Task) {
	if len(task.Parameters) > 0 {
		p1 := task.Parameters[0]
		switch p1.Source {
		case "memory":
			query_excel_memory(task)
		case "array":
			query_excel_array(task)
		}
	} else {
		db, _ := con.GetDB(task.Connection)
		util.QuerySaveExcel(task.Name, db, task.Command, task.OutputName)
	}
}

func GetMemory(name string) *Memory {
	return mapqry[name]
}

func query_memory(task *Task) {
	db, err := con.GetDB(task.Connection)
	if err == nil {
		if len(task.Parameters) > 0 {
			cmd := task.Command
			m := new(Memory)
			for _, p := range task.Parameters {
				switch p.Source {
				case "array":
					array := GetArray(p.SourceName)
					for _, a := range array {
						cmd = strings.ReplaceAll(cmd, p.Name[0], a)
					}
				case "memory":
					mem := GetMemory(p.SourceName)
					for _, r := range mem.rows {
						mr := *r.(*map[string]string)
						for i := 0; i < len(p.Field); i++ {
							cmd = strings.ReplaceAll(cmd, p.Name[i], mr[p.Field[i]])
						}
					}
				}
			}
			m.columnNames, m.rows = util.Query(db, cmd)
			mapqry[task.Name] = m
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
