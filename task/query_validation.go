package task

import (
	"log"

	con "jyv.com/goarchive/connection"
)

/*
func ValidateQueryParameterSource(p Parameter, position int, taskposition int, tasks []Task) {
	for _, t := range tasks {
		if p.Source == t.Name {
			return
		}
	}
	log.Fatalln("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, ">: the <Source:", p.Source, "> does not exist")
}
*/

func ValidateQueryParameters(params []Parameter, query *Query, position int) {
	if len(params) >= 1 {
		if params[0].Kind != "parent" {
			log.Fatal("Parameter Error in the json file:  <Task:", query.Task.Name, "> of <Task #", position, "> the first parameter must have a <Kind> equal to <parent>")
		}
	}
}

func ValidateQueryParameter(p Parameter, position int, taskposition int) {
	if p.Names == nil {
		log.Fatal("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, "> does not contains the field : <Names>")
	}
	/*
		if reflect.TypeOf(p.Names).Kind() != reflect.Array {
			log.Fatal("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, "> the <Names> field must be an array of string, brakets are missing")
		}
	*/
	if p.Fields == nil {
		log.Fatal("Parameter Error in the json file: <Parameters #", position, " of <Task #", taskposition, "> does not contains the field : <Fields>")
	}
	/*
		if reflect.TypeOf(p.Fields).Kind() != reflect.Array {
			log.Fatal("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, "> the <Fields> field must be an array of string, brakets are missing")
		}
	*/
	if p.Source == "" {
		log.Fatal("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, "> does not contains the field : <Source>")
	}
	if p.Kind == "" {
		log.Println("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, "> does not contains the field : <Kind>")
	}
	if p.Kind != "parent" && p.Kind != "child" {
		log.Println("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, "> <Kind:", p.Kind, " is not supported")
		log.Fatalln("Parameter Error: supported values are <parent>,<child>")
	}
	if p.UseDatabase != "" && p.Kind == "child" {
		log.Println("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, "> <UseDatabase not supported for <Kind:child>")
	}
}

func ValidateQueryConnection(query *Query, connections []con.Connection, position int) {
	if query.Task.Kind == "csv" {
		return
	}
	for _, c := range connections {
		if query.Connection == c.Name {
			return
		}
	}
	log.Fatalln("Task Error in the json file: <Tasks #", position, ">: the <Connection:", query.Connection, "> does not exist")
}

func ValidateQueryTask(query *Query, position int) {
	if query.Task.Name == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> does not contains the field <Name>")
	}
	if query.Task.Kind == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", query.Task.Name, "> does not contains the field <Kind>")
	}
	if query.Task.Kind != "query" && query.Task.Kind != "array" && query.Task.Kind != "csv" {
		log.Println("Task Error in the json file: <Tasks #", position, "> of <", query.Task.Name, ">, <Kind:", query.Task.Kind, "  is not supported")
		log.Fatalln("Task Error: supported values are <query>,<array>,<csv>")
	}
	if query.Command == "" && query.Task.Kind != "csv" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", query.Task.Name, "> does not contains the field <Command>")
	}
	if query.Connection == "" && query.Task.Kind != "array" && query.Task.Kind != "csv" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", query.Task.Name, "> does not contains the field <Connection>")
	}
	if query.OutputType == "" && query.Task.Kind != "csv" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", query.Task.Name, "> does not contains the field <OutputType>")
	}
	if query.OutputType != "excel" && query.OutputType != "memory" && query.OutputType != "reference" && query.OutputType != "csv" {
		log.Println("Task Error in the json file: <Tasks #", position, "> of <", query.Task.Name, ">, <OutputType:", query.OutputType, "  is not supported")
		log.Fatalln("Task Error: supported values are <memory>,<excel>,<reference>,<csv>")
	}
	if query.OutputType == "excel" && query.FileName == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", query.Task.Name, "> output type <Excel> must have a field <FileName>")
	}
	if query.Task.Kind == "csv" && query.FileName == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", query.Task.Name, "> the field <FileName> cannot be empty for a task <csv>")
	}
}
