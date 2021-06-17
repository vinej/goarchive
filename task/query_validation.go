package task

import (
	"log"

	con "jyv.com/goarchive/connection"
	"jyv.com/goarchive/message"
)

func ValidateQueryParameterSource(p Parameter, position int, taskposition int, tasks []ITask) {
	for _, t := range tasks {
		if p.Source == t.GetTask().Name {
			return
		}
	}
	log.Fatalf(message.GetMessage(30), position, taskposition, p.Source)
}

func ValidateQueryParameters(params []Parameter, query *Query, position int) {
	if len(params) >= 1 {
		if params[0].Kind != PARAM_PARENT {
			log.Fatalf(message.GetMessage(31), query.Task.Name, position, PARAM_PARENT)
		}
	}
}

func ValidateQueryParameter(p Parameter, position int, taskposition int) {
	if p.Names == nil {
		log.Fatalf(message.GetMessage(32), position, taskposition, PARAM_NAMES)
	}
	/*
		if reflect.TypeOf(p.Names).Kind() != reflect.Array {
			log.Fatal("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, "> the <Names> field must be an array of string, brakets are missing")
		}
	*/
	if p.Fields == nil {
		log.Fatalf(message.GetMessage(32), position, taskposition, PARAM_FIELDS)
	}
	/*
		if reflect.TypeOf(p.Fields).Kind() != reflect.Array {
			log.Fatal("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, "> the <Fields> field must be an array of string, brakets are missing")
		}
	*/
	if p.Source == "" {
		log.Fatalf(message.GetMessage(32), position, taskposition, PARAM_SOURCE)
	}
	if p.Kind == "" {
		log.Fatalf(message.GetMessage(32), position, taskposition, PARAM_KIND)
	}
	if p.Kind != PARAM_PARENT && p.Kind != PARAM_CHILD {
		log.Printf(message.GetMessage(33), position, taskposition, p.Kind)
		log.Fatalf(message.GetMessage(34), PARAM_PARENT, PARAM_CHILD)
	}
	if p.UseDatabase != "" && p.Kind == PARAM_CHILD {
		log.Fatalf(message.GetMessage(35), position, taskposition, PARAM_CHILD)
	}
}

func ValidateQueryConnection(query *Query, connections []con.Connection, position int) {
	if query.Task.Kind == PARAM_CSV {
		return
	}
	for _, c := range connections {
		if query.Connection == c.Name {
			return
		}
	}
	log.Fatalf(message.GetMessage(36), position, query.Connection)
}

func ValidateQueryTask(query *Query, position int) {
	if query.Task.Name == "" {
		log.Fatalf(message.GetMessage(37), position, QUERY_NAME)
	}
	if query.Task.Kind == "" {
		log.Fatalf(message.GetMessage(38), position, query.Task.Name, QUERY_KIND)
	}
	if query.Task.Kind != QUERY_KIND_QUERY && query.Task.Kind != QUERY_KIND_ARRAY && query.Task.Kind != QUERY_KIND_CSV {
		log.Printf(message.GetMessage(39), position, query.Task.Name, query.Task.Kind)
		log.Fatalf(message.GetMessage(40), QUERY_KIND_QUERY, QUERY_KIND_ARRAY, QUERY_KIND_CSV)
	}
	if query.Command == "" && query.Task.Kind != QUERY_KIND_CSV {
		log.Fatalf(message.GetMessage(37), position, QUERY_COMMAND)
	}
	if query.Connection == "" && query.Task.Kind != QUERY_KIND_ARRAY && query.Task.Kind != QUERY_KIND_CSV {
		log.Fatalf(message.GetMessage(37), position, QUERY_CONNECTION)
	}
	if query.OutputType == "" && query.Task.Kind != QUERY_KIND_CSV {
		log.Fatalf(message.GetMessage(37), position, QUERY_OUTPUT_TYPE)
	}
	if query.OutputType != QUERY_OUTPUT_TYPE_EXCEL && query.OutputType != QUERY_OUTPUT_TYPE_MEMORY &&
		query.OutputType != QUERY_OUTPUT_TYPE_REFERENCE && query.OutputType != QUERY_OUTPUT_TYPE_CSV {
		log.Printf(message.GetMessage(41), position, query.Task.Name, query.OutputType)
		log.Fatalf(message.GetMessage(42), QUERY_OUTPUT_TYPE_EXCEL, QUERY_OUTPUT_TYPE_MEMORY, QUERY_OUTPUT_TYPE_REFERENCE, QUERY_OUTPUT_TYPE_CSV)
	}
	if query.OutputType == QUERY_OUTPUT_TYPE_EXCEL && query.FileName == "" {
		log.Fatalf(message.GetMessage(43), position, query.Task.Name, QUERY_OUTPUT_TYPE_EXCEL, QUERY_FILENAME)
	}
	if query.Task.Kind == QUERY_KIND_CSV && query.FileName == "" {
		log.Fatalf(message.GetMessage(43), position, query.Task.Name, QUERY_OUTPUT_TYPE_CSV, QUERY_FILENAME)
	}
}
