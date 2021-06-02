package task

import (
	"log"
	"strings"

	"github.com/jinzhu/copier"
	con "jyv.com/goarchive/connection"
	util "jyv.com/goarchive/util"
)

type Memory struct {
	columnNames []string
	rows        []interface{}
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

func validate_parameter(p Parameter) {
	if p.Names == nil {
		log.Fatal("The json file for 'Parameters' does not contains the field :  'Names'   ,check for a typo")
	}
	if p.Fields == nil {
		log.Fatal("The json file for 'Parameters' does not contains the field :  'Fields'  ,check for a typo")
	}
	if p.Source == "" {
		log.Fatal("the json file for 'Parameters' does not contains the field :  'Source'   ,check for a typo")
	}
	if p.Kind == "" {
		log.Fatal("the json file for 'Parameters' does not contains the field :  'Kind'    ,check for a typo")
	}
}

func query_excel_memory(task Task) {
	db, _ := con.GetDB(task.Connection)
	p1 := task.Parameters[0]
	validate_parameter(p1)
	mem := GetMemory(p1.Source)
	for _, r := range mem.rows {
		mr := *r.(*map[string]string)
		cmd := task.Command
		out := task.OutputName
		for i := 0; i < len(p1.Fields); i++ {
			ma := adjust_quote(mr[p1.Fields[i]])
			cmd = strings.ReplaceAll(cmd, p1.Names[i], ma)
			out = "p" + ma + "_" + out
		}
		if len(task.Parameters) == 2 {
			p2 := task.Parameters[1]
			validate_parameter(p2)
			if p2.Kind == "reference" {
				task2 := mapref[p2.Source]
				cmd := task2.Command
				// replace the field of the parent
				for i := 0; i < len(p1.Fields); i++ {
					ma := adjust_quote(mr[p1.Fields[i]])
					cmd = strings.ReplaceAll(cmd, p1.Names[i], ma)
				}
				tmpTask := new(Task)
				copier.Copy(tmpTask, &task2)
				tmpTask.Command = cmd
				query_memory(*tmpTask)
			}
			mem2 := GetMemory(p2.Source)
			isFirst := true
			for r2 := 0; r2 < len(mem2.rows); r2++ {
				mr := *mem2.rows[r2].(*map[string]string)
				cmd2 := cmd
				out2 := out
				for i := 0; i < len(p2.Fields); i++ {
					// if same field fir i and i + 1, means that the first one is for previous record
					// and the second one for the current record
					// we must skip to record i+1 because there is no record[-1]
					if i+1 < len(p2.Fields) && p2.Fields[i] == p2.Fields[i+1] {
						// period type, goto second record
						if isFirst {
							r2 = r2 + 1
							isFirst = false
						}

						i = i + 1
						// take next field
						mr2 := *mem2.rows[r2].(*map[string]string)
						ma2 := adjust_quote(mr2[p2.Fields[i]])
						cmd2 = strings.ReplaceAll(cmd2, p2.Names[i], ma2)
						out2 = "p" + ma2 + "_" + out2

						i = i - 1
						// use previous record
						mr2 = *mem2.rows[r2-1].(*map[string]string)
						ma2 = adjust_quote(mr2[p2.Fields[i]])
						cmd2 = strings.ReplaceAll(cmd2, p2.Names[i], ma2)
						out2 = "p" + ma2 + "_" + out2

						i = i + 1
					} else {
						ma2 := adjust_quote(mr[p2.Fields[i]])
						cmd2 = strings.ReplaceAll(cmd2, p2.Names[i], ma2)
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

func query_excel(task Task) {
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
