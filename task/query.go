package task

import (
	"log"
	"strings"

	"github.com/jinzhu/copier"
	con "jyv.com/goarchive/connection"
	msql "jyv.com/goarchive/msql"
	util "jyv.com/goarchive/util"
)

type Query struct {
	Task
	Description string
	Connection  string
	Command     string
	OutputType  string
	FileName    string
	Parameters  []Parameter
}

type Memory struct {
	columnNames []string
	rows        []map[string]string
}

var mapqry map[string]*Memory = make(map[string]*Memory)
var mapref map[string]interface{} = make(map[string]interface{})

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
	query := mapref[param2.Source].(*Query)
	cmd := adjust_cmd_all(query.Command, param1, row)
	queryTemp := new(Query)
	copier.Copy(&queryTemp, &query)
	queryTemp.Command = cmd
	query_memory(ctx, queryTemp)
}

func use_database(ctx *con.Connection, p Parameter, row map[string]string) {
	// set current DB with the field of UseDatabaseField
	dbFieldValue := row[p.UseDatabase]
	msql.Query(ctx, "use "+dbFieldValue)
}

func query_excel_level(ctx *con.Connection, cmd string, out string, query *Query, level int, row map[string]string) {
	p2 := query.Parameters[level]
	if p2.Kind == "child" {
		if level == 0 {
			log.Fatalln("Task source error: first parameter cannot be a <kind> child")
		}
		p1 := query.Parameters[level-1]
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
			if level == len(query.Parameters)-1 {
				// we can use a goroutine here
				go msql.QuerySaveExcel(ctx, query.Task.Name, cmd2, out2)
			} else {
				query_excel_level(ctx, cmd2, out2, query, level+1, mem2.rows[r])
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
			if level == len(query.Parameters)-1 {
				// we can use a goroutine here
				go msql.QuerySaveExcel(ctx, query.Task.Name, cmd2, out2)
			} else {
				query_excel_level(ctx, cmd2, out2, query, level+1, mem2.rows[r])
			}
		}
	}
}

func query_csv_level(ctx *con.Connection, cmd string, out string, query *Query, level int, row map[string]string) {
	p2 := query.Parameters[level]
	if p2.Kind == "child" {
		if level == 0 {
			log.Fatalln("Task source error: first parameter cannot be a <kind> child")
		}
		p1 := query.Parameters[level-1]
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
			if level == len(query.Parameters)-1 {
				// we can use a goroutine here
				go msql.QuerySaveCsv(ctx, query.Task.Name, cmd2, out2)
			} else {
				query_csv_level(ctx, cmd2, out2, query, level+1, mem2.rows[r])
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
			if level == len(query.Parameters)-1 {
				// we can use a goroutine here
				go msql.QuerySaveCsv(ctx, query.Task.Name, cmd2, out2)
			} else {
				query_csv_level(ctx, cmd2, out2, query, level+1, mem2.rows[r])
			}
		}
	}
}

func query_excel(ctx *con.Connection, query *Query) {
	if len(query.Parameters) == 0 {
		// we can use a goroutine here
		go msql.QuerySaveExcel(ctx, query.Task.Name, query.Command, query.FileName)
		return
	}
	query_excel_level(ctx, query.Command, query.FileName, query, 0, nil)
}

func query_csv(ctx *con.Connection, query *Query) {
	if len(query.Parameters) == 0 {
		// we can use a goroutine here
		go msql.QuerySaveCsv(ctx, query.Task.Name, query.Command, query.FileName)
		return
	}
	query_csv_level(ctx, query.Command, query.FileName, query, 0, nil)
}

func GetMemory(name string) *Memory {
	return mapqry[name]
}

/*
	query the database and put the result into memory
	no parameter managed by the option
*/
func query_memory(ctx *con.Connection, query *Query) {
	m := new(Memory)
	m.columnNames, m.rows = msql.Query(ctx, query.Command)
	mapqry[query.Task.Name] = m
}

func query_reference(query *Query) {
	mapref[query.Task.Name] = query
}

/*
func RunQuery(ctx *con.Connection, task Task) {
	switch task.OutputType {
	case "csv":
		query_csv(ctx, task)
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
*/

func (query *Query) Run(acon []con.Connection, position int) {
	switch query.OutputType {
	case "csv":
		ctx := con.GetConnection(acon, query.Connection)
		query_csv(ctx, query)
	case "excel":
		ctx := con.GetConnection(acon, query.Connection)
		query_excel(ctx, query)
	case "memory":
		ctx := con.GetConnection(acon, query.Connection)
		query_memory(ctx, query)
	case "reference":
		query_reference(query)
	default:
		log.Fatalf("The output type '%s' is not supported,  check for a typo", query.OutputType)
	}
}

func (query *Query) Validate(acon []con.Connection, position int) {
	ValidateQueryConnection(query, acon, position)
	ValidateQueryTask(query, position)
	ValidateQueryParameters(query.Parameters, query, position)
	for i, p := range query.Parameters {
		ValidateQueryParameter(p, i, position)
	}

	/*
		switch query.OutputType {
		case "csv":
			query_csv(ctx, query)
		case "excel":
			query_excel(ctx, query)
		case "memory":
			query_memory(ctx, query)
		case "reference":
			query_reference(query)
		default:
			log.Fatalf("The output type '%s' is not supported,  check for a typo", query.OutputType)
		}
	*/
}

func (query *Query) Transform(m map[string]interface{}) {
	query.Task.Kind = util.GetFieldValueFromMap(m, "Kind")
	query.Task.Name = util.GetFieldValueFromMap(m, "Name")
	query.Description = util.GetFieldValueFromMap(m, "Description")
	query.Command = util.GetFieldValueFromMap(m, "Command")
	query.Connection = util.GetFieldValueFromMap(m, "Connection")
	query.OutputType = util.GetFieldValueFromMap(m, "OutputType")
	query.FileName = util.GetFieldValueFromMap(m, "FileName")
	query.Parameters = make([]Parameter, 0)
	field := util.GetFieldFromMap(m, "Parameters")
	if field != "" {
		pm := m[field].([]interface{})
		for _, p := range pm {
			mp := p.(map[string]interface{})
			param := new(Parameter)

			param.Fields = make([]string, 0)
			field = util.GetFieldFromMap(mp, "Fields")
			if field != "" {
				for _, f := range mp[field].([]interface{}) {
					param.Fields = append(param.Fields, f.(string))
				}
			}

			param.Kind = util.GetFieldValueFromMap(mp, "Kind")

			param.Names = make([]string, 0)
			field = util.GetFieldFromMap(mp, "Names")
			if field != "" {
				for _, n := range mp[field].([]interface{}) {
					param.Names = append(param.Names, n.(string))
				}
			}

			param.Source = util.GetFieldValueFromMap(mp, "Source")
			param.UseDatabase = util.GetFieldValueFromMap(mp, "UseDatabase")

			query.Parameters = append(query.Parameters, *param)
		}
	}
}

func (query *Query) GetTask() Task { return query.Task }
