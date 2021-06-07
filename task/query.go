package task

import (
	"database/sql"
	"log"
	"strings"

	"github.com/jinzhu/copier"
	con "jyv.com/goarchive/connection"
	util "jyv.com/goarchive/util"
)

type Memory struct {
	columnNames []string
	rows        []map[string]string
}

var mapqry map[string]*Memory = make(map[string]*Memory)
var mapref map[string]Task = make(map[string]Task)

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

func adjust_cmd_out_all(cmd string, out string, p Parameter, row map[string]string) (string, string) {
	for i := 0; i < len(p.Fields); i++ {
		cmd, out = adjust_cmd_out_index(cmd, out, p, row, i)
	}
	return cmd, out
}

func adjust_cmd_out_index(cmd string, out string, param Parameter, row map[string]string, index int) (string, string) {
	paramvalue := adjust_quote(row[param.Fields[index]])
	cmd = strings.ReplaceAll(cmd, param.Names[index], paramvalue)
	out = "p" + paramvalue + "_" + out
	return cmd, out
}

func adjust_cmd_all(cmd string, param Parameter, row map[string]string) string {
	for i := 0; i < len(param.Fields); i++ {
		paramvalue := adjust_quote(row[param.Fields[i]])
		cmd = strings.ReplaceAll(cmd, param.Names[i], paramvalue)
	}
	return cmd
}

func query_task(param1 Parameter, param2 Parameter, row map[string]string) {
	task := mapref[param2.Source]
	cmd := adjust_cmd_all(task.Command, param1, row)
	tmpTask := new(Task)
	copier.Copy(tmpTask, &task)
	tmpTask.Command = cmd
	query_memory(*tmpTask)
}

func use_database(db *sql.DB, p Parameter, row map[string]string) {
	// set current DB with the field of UseDatabaseField
	dbFieldValue := row[p.UseDatabase]
	util.Query(db, "use "+dbFieldValue)
}

func query_excel(task Task) {
	db, _ := con.GetDB(task.Connection)
	if len(task.Parameters) == 0 {
		util.QuerySaveExcel(task.Name, db, task.Command, task.OutputName)
		return
	}

	p1 := task.Parameters[0]
	mem := GetMemory(p1.Source)
	if mem == nil {
		log.Fatalln("Task source error: the source:", p1.Source, "is not available. Maybe you used a <reference> instead of <memory> OutputType for the task")
	}
	for _, row := range mem.rows {
		if p1.UseDatabase != "" {
			use_database(db, p1, row)
		}
		cmd, out := adjust_cmd_out_all(task.Command, task.OutputName, p1, row)
		if len(task.Parameters) == 2 {
			// with 2 parameters, the second one is related to the first one
			// we need to get the new values for parameter 2, related to parameter 1
			p2 := task.Parameters[1]
			if p2.Kind == "child" {
				query_task(p1, p2, row)
				mem2 := GetMemory(p2.Source)
				if mem2 == nil {
					log.Fatalln("Task source error: the source:", p2.Source, "is not available. Maybe you used a <reference> instead of <memory> OutputType for the task")
				}

				isFirst := true
				for r := 0; r < len(mem2.rows); r++ {
					cmd2 := cmd
					out2 := out
					for i := 0; i < len(p2.Fields); i++ {
						if i+1 < len(p2.Fields) && p2.Fields[i] == p2.Fields[i+1] {
							if isFirst {
								// for revious-next, we don't use the first record
								r = r + 1
								isFirst = false
							}
							// current: the current field is the second one in that case: i+1
							cmd2, out2 = adjust_cmd_out_index(cmd2, out2, p2, mem2.rows[r], i+1)
							// previous: the previous field is the fiest one in that case: i
							cmd2, out2 = adjust_cmd_out_index(cmd2, out2, p2, mem2.rows[r-1], i)
							// go to next field, because we did 2 here
							i = i + 1
						} else {
							cmd2, out2 = adjust_cmd_out_index(cmd2, out2, p2, mem2.rows[r], i)
						}
					}
					util.QuerySaveExcel(task.Name, db, cmd2, out2)
				}
			} else {
				mem2 := GetMemory(p2.Source)
				if mem2 == nil {
					log.Fatalln("Task source error: the source:", p2.Source, "is not available. Maybe you used a <reference> instead of <memory> OutputType for the task")
				}
				for r := 0; r < len(mem2.rows); r++ {
					cmd2 := cmd
					out2 := out
					for i := 0; i < len(p2.Fields); i++ {
						cmd2, out2 = adjust_cmd_out_index(cmd2, out2, p2, mem2.rows[r], i)
					}
					util.QuerySaveExcel(task.Name, db, cmd2, out2)
				}
			}
		} else {
			util.QuerySaveExcel(task.Name, db, cmd, out)
		}
	}
}

func GetMemory(name string) *Memory {
	return mapqry[name]
}

/*
	query the database and put the result into memory
	no parameter managed by the option
*/
func query_memory(task Task) {
	db, err := con.GetDB(task.Connection)
	if err == nil {
		m := new(Memory)
		m.columnNames, m.rows = util.Query(db, task.Command)
		mapqry[task.Name] = m
	} else {
		log.Fatal(err)
	}
}

func query_reference(task Task) {
	mapref[task.Name] = task
}

func RunQuery(task Task) {
	switch task.OutputType {
	case "excel":
		query_excel(task)
	case "memory":
		query_memory(task)
	case "reference":
		query_reference(task)
	default:
		log.Fatalf("The output type '%s' is not supported,  check for a typo", task.OutputType)
	}
}
