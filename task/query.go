package task

import (
	"log"
	"strings"

	"github.com/jinzhu/copier"
	con "jyv.com/goarchive/connection"
	"jyv.com/goarchive/message"
	msql "jyv.com/goarchive/msql"
	util "jyv.com/goarchive/util"
)

const SQL_USE = "use "

const PARAM_PARENT = "parent"
const PARAM_NAMES = "Names"
const PARAM_FIELDS = "Fields"
const PARAM_SOURCE = "Source"
const PARAM_KIND = "Kind"
const PARAM_CHILD = "child"
const PARAM_CSV = "csv"

const QUERY_PARAMETERS = "Parameters"
const QUERY_EXCLUDED_COLUMNS = "ExcludedColumns"
const QUERY_ANONYMIZED_COLUMNS = "AnonymizedColumns"
const QUERY_NAME = "Name"
const QUERY_KIND = "Kind"
const QUERY_DESCRIPTION = "Description"
const QUERY_FILENAME = "FileName"
const QUERY_COMMAND = "Command"
const QUERY_CONNECTION = "Connection"
const QUERY_OUTPUT_TYPE = "OutputType"
const QUERY_USE_DATABASE = "UseDatabase"
const QUERY_KIND_QUERY = "query"
const QUERY_KIND_ARRAY = "array"
const QUERY_KIND_CSV = "csv"
const QUERY_OUTPUT_TYPE_EXCEL = "excel"
const QUERY_OUTPUT_TYPE_MEMORY = "memory"
const QUERY_OUTPUT_TYPE_REFERENCE = "reference"
const QUERY_OUTPUT_TYPE_CSV = "csv"

type SaveOutput func(ctx *con.Connection, name string, query string, output string, excludedColumns []string, AnonymizedColumns []string)

type Parameter struct {
	Names       []string
	Fields      []string
	Source      string
	UseDatabase string
	Kind        string
}

// todo
type Anonymized struct {
	Columns []string
	Prefix  string
	Suffix  string
	Pattern string // prefix+incr, meta+incr
}

type Query struct {
	Task
	Description       string
	Connection        string
	Command           string
	OutputType        string
	FileName          string
	ExcludedColumns   []string
	AnonymizedColumns []string
	Parameters        []Parameter
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
	cmd = strings.ReplaceAll(cmd, param.Names[index], strings.TrimSpace(paramvalue))
	out = "p" + strings.TrimSpace(paramvalue) + "_" + out
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
	msql.Query(ctx, SQL_USE+dbFieldValue)
}

func query_level(ctx *con.Connection, cmd string, out string, query *Query, level int, row map[string]string, save SaveOutput,
	excludedColumns []string, anonymizedColumns []string) {
	p2 := query.Parameters[level]
	if strings.ToLower(p2.Kind) == PARAM_CHILD {
		if level == 0 {
			log.Fatalf(message.GetMessage(44), PARAM_CHILD)
		}
		p1 := query.Parameters[level-1]
		query_task(ctx, p1, p2, row)
		mem2 := GetMemory(p2.Source)
		if mem2 == nil {
			log.Fatalf(message.GetMessage(45), p2.Source)
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
				go save(ctx, query.Task.Name, cmd2, out2, excludedColumns, anonymizedColumns)
			} else {
				query_level(ctx, cmd2, out2, query, level+1, mem2.rows[r], save, excludedColumns, anonymizedColumns)
			}
		}
	} else {
		mem2 := GetMemory(p2.Source)
		if mem2 == nil {
			log.Fatalf(message.GetMessage(45), p2.Source)
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
				go save(ctx, query.Task.Name, cmd2, out2, excludedColumns, anonymizedColumns)
			} else {
				query_level(ctx, cmd2, out2, query, level+1, mem2.rows[r], save, excludedColumns, anonymizedColumns)
			}
		}
	}
}

func query_excel(ctx *con.Connection, query *Query) {
	if len(query.Parameters) == 0 {
		// we can use a goroutine here
		go msql.QuerySaveExcel(ctx, query.Task.Name, query.Command, query.FileName, query.ExcludedColumns, query.AnonymizedColumns)
		return
	}
	query_level(ctx, query.Command, query.FileName, query, 0, nil, msql.QuerySaveExcel, query.ExcludedColumns, query.AnonymizedColumns)
}

func query_csv(ctx *con.Connection, query *Query) {
	if len(query.Parameters) == 0 {
		// we can use a goroutine here
		go msql.QuerySaveCsv(ctx, query.Task.Name, query.Command, query.FileName, query.ExcludedColumns, query.AnonymizedColumns)
		return
	}
	query_level(ctx, query.Command, query.FileName, query, 0, nil, msql.QuerySaveCsv, query.ExcludedColumns, query.AnonymizedColumns)
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

func (query *Query) Run(acon []con.Connection, position int) {
	switch strings.ToLower(query.OutputType) {
	case QUERY_OUTPUT_TYPE_CSV:
		ctx := con.GetConnection(acon, query.Connection)
		query_csv(ctx, query)
	case QUERY_OUTPUT_TYPE_EXCEL:
		ctx := con.GetConnection(acon, query.Connection)
		query_excel(ctx, query)
	case QUERY_OUTPUT_TYPE_MEMORY:
		ctx := con.GetConnection(acon, query.Connection)
		query_memory(ctx, query)
	case QUERY_OUTPUT_TYPE_REFERENCE:
		query_reference(query)
	default:
		log.Fatalf(message.GetMessage(46), query.OutputType)
	}
}

func (query *Query) Validate(acon []con.Connection, position int) {
	ValidateQueryConnection(query, acon, position)
	ValidateQueryTask(query, position)
	ValidateQueryParameters(query.Parameters, query, position)
	for i, p := range query.Parameters {
		ValidateQueryParameter(p, i, position)
	}
}

func (query *Query) Transform(m map[string]interface{}) {
	query.Task.Kind = util.GetFieldValueFromMap(m, QUERY_KIND)
	query.Task.Name = util.GetFieldValueFromMap(m, QUERY_NAME)
	query.Description = util.GetFieldValueFromMap(m, QUERY_DESCRIPTION)
	query.Command = util.GetFieldValueFromMap(m, QUERY_COMMAND)
	query.Connection = util.GetFieldValueFromMap(m, QUERY_CONNECTION)
	query.OutputType = util.GetFieldValueFromMap(m, QUERY_OUTPUT_TYPE)
	query.FileName = util.GetFieldValueFromMap(m, QUERY_FILENAME)
	query.ExcludedColumns = make([]string, 0)
	field := util.GetFieldFromMap(m, QUERY_EXCLUDED_COLUMNS)
	if field != "" {
		pm := m[field].([]interface{})
		for _, f := range pm {
			query.ExcludedColumns = append(query.ExcludedColumns, f.(string))
		}

	}

	query.AnonymizedColumns = make([]string, 0)
	field = util.GetFieldFromMap(m, QUERY_ANONYMIZED_COLUMNS)
	if field != "" {
		pm := m[field].([]interface{})
		for _, f := range pm {
			query.AnonymizedColumns = append(query.AnonymizedColumns, f.(string))
		}
	}

	query.Parameters = make([]Parameter, 0)
	field = util.GetFieldFromMap(m, QUERY_PARAMETERS)
	if field != "" {
		pm := m[field].([]interface{})
		for _, p := range pm {
			mp := p.(map[string]interface{})
			param := new(Parameter)

			param.Fields = make([]string, 0)
			field = util.GetFieldFromMap(mp, PARAM_FIELDS)
			if field != "" {
				for _, f := range mp[field].([]interface{}) {
					param.Fields = append(param.Fields, f.(string))
				}
			}

			param.Kind = util.GetFieldValueFromMap(mp, PARAM_KIND)

			param.Names = make([]string, 0)
			field = util.GetFieldFromMap(mp, PARAM_NAMES)
			if field != "" {
				for _, n := range mp[field].([]interface{}) {
					param.Names = append(param.Names, n.(string))
				}
			}

			param.Source = util.GetFieldValueFromMap(mp, PARAM_SOURCE)

			param.UseDatabase = util.GetFieldValueFromMap(mp, QUERY_USE_DATABASE)

			query.Parameters = append(query.Parameters, *param)
		}
	}
}

func (query *Query) GetTask() Task { return query.Task }

func (query *Query) ValidateEtl(tasks []ITask, position int) {
	for i, p := range query.Parameters {
		ValidateQueryParameterSource(p, i, position, tasks)
	}
}
