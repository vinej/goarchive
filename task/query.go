package task

import (
	"log"
	"strings"

	"github.com/jinzhu/copier"
	con "jyv.com/goarchive/connection"
	msql "jyv.com/goarchive/msql"
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

func adjust_cmd_out_index(cmd string, out string, param Parameter, row map[string]string, index int) (string, string) {
	paramvalue := adjust_quote(row[param.Fields[index]])
	cmd = strings.ReplaceAll(cmd, param.Names[index], paramvalue)
	out = "p" + strings.Trim(paramvalue, " ") + "_" + out
	return cmd, out
}

func adjust_cmd_all(cmd string, param Parameter, row map[string]string) string {
	for i := 0; i < len(param.Fields); i++ {
		paramvalue := adjust_quote(row[param.Fields[i]])
		cmd = strings.ReplaceAll(cmd, param.Names[i], paramvalue)
	}
	return cmd
}

func query_task(ctx *con.Connection, param1 Parameter, param2 Parameter, row map[string]string) {
	task := mapref[param2.Source]
	cmd := adjust_cmd_all(task.Command, param1, row)
	tmpTask := new(Task)
	copier.Copy(tmpTask, &task)
	tmpTask.Command = cmd
	query_memory(ctx, *tmpTask)
}

func use_database(ctx *con.Connection, p Parameter, row map[string]string) {
	// set current DB with the field of UseDatabaseField
	dbFieldValue := row[p.UseDatabase]
	msql.Query(ctx, "use "+dbFieldValue)
}

func query_excel_level(ctx *con.Connection, cmd string, out string, task Task, level int, row map[string]string) {
	p2 := task.Parameters[level]
	if p2.Kind == "child" {
		if level == 0 {
			log.Fatalln("Task source error: first parameter cannot be a <kind> child")
		}
		p1 := task.Parameters[level-1]
		query_task(ctx, p1, p2, row)
		mem2 := GetMemory(p2.Source)
		if mem2 == nil {
			log.Fatalln("Task source error: the source:", p2.Source, "is not available. Maybe you used a <reference> instead of <memory> OutputType for the task")
		}

		isFirst := true
		for r := 0; r < len(mem2.rows); r++ {
			cmd2 := cmd
			out2 := out
			if p2.UseDatabase != "" {
				use_database(ctx, p2, mem2.rows[r])
			}
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
			if level == len(task.Parameters)-1 {
				// we can use a goroutine here
				go msql.QuerySaveExcel(ctx, task.Name, cmd2, out2)
			} else {
				query_excel_level(ctx, cmd2, out2, task, level+1, mem2.rows[r])
			}
		}
	} else {
		mem2 := GetMemory(p2.Source)
		if mem2 == nil {
			log.Fatalln("Task source error: the source:", p2.Source, "is not available. Maybe you used a <reference> instead of <memory> OutputType for the task")
		}
		for r := 0; r < len(mem2.rows); r++ {
			if p2.UseDatabase != "" {
				use_database(ctx, p2, mem2.rows[r])
			}
			cmd2 := cmd
			out2 := out
			for i := 0; i < len(p2.Fields); i++ {
				cmd2, out2 = adjust_cmd_out_index(cmd2, out2, p2, mem2.rows[r], i)
			}
			if level == len(task.Parameters)-1 {
				// we can use a goroutine here
				go msql.QuerySaveExcel(ctx, task.Name, cmd2, out2)
			} else {
				query_excel_level(ctx, cmd2, out2, task, level+1, mem2.rows[r])
			}
		}
	}
}

func query_excel(ctx *con.Connection, task Task) {
	if len(task.Parameters) == 0 {
		// we can use a goroutine here
		go msql.QuerySaveExcel(ctx, task.Name, task.Command, task.FileName)
		return
	}
	query_excel_level(ctx, task.Command, task.FileName, task, 0, nil)
}

func GetMemory(name string) *Memory {
	return mapqry[name]
}

/*
	query the database and put the result into memory
	no parameter managed by the option
*/
func query_memory(ctx *con.Connection, task Task) {
	m := new(Memory)
	m.columnNames, m.rows = msql.Query(ctx, task.Command)
	mapqry[task.Name] = m
}

func query_reference(task Task) {
	mapref[task.Name] = task
}

func RunQuery(ctx *con.Connection, task Task) {
	switch task.OutputType {
	case "excel":
		query_excel(ctx, task)
	case "memory":
		query_memory(ctx, task)
	case "reference":
		query_reference(task)
	default:
		log.Fatalf("The output type '%s' is not supported,  check for a typo", task.OutputType)
	}
}
