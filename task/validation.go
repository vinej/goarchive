package task

import (
	"log"

	con "jyv.com/goarchive/connection"
	util "jyv.com/goarchive/util"
)

func ValidateTaskUniqueNames(tasks []Task) {
	names := make([]string, 0)
	isFirst := true
	for i, t := range tasks {
		if util.Contains(names, t.Name) && !isFirst {
			log.Fatalln("Task error in the json file: the <Task:", t.Name, "> of <Task:", i+1, "> already exists")
		} else {
			names = append(names, t.Name)
		}
		isFirst = false
	}
}

func ValidateParameterSource(p Parameter, position int, taskposition int, tasks []Task) {
	for _, t := range tasks {
		if p.Source == t.Name {
			return
		}
	}
	log.Fatalln("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, ">: the <Source:", p.Source, "> does not exist")
}

func ValidateParameters(params []Parameter, t Task, taskposition int) {
	if len(params) >= 1 {
		if params[0].Kind != "parent" {
			log.Fatal("Parameter Error in the json file:  <Task:", t.Name, "> of <Task #", taskposition, "> the first parameter must have a <Kind> equal to <parent>")
		}
	}
}

func ValidateParameter(p Parameter, position int, taskposition int) {
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

func ValidateTaskConnection(t Task, connections []con.Connection, position int) {
	if t.Kind == "csv" {
		return
	}
	for _, c := range connections {
		if t.Connection == c.Name {
			return
		}
	}
	log.Fatalln("Task Error in the json file: <Tasks #", position, ">: the <Connection:", t.Connection, "> does not exist")
}

func ValidateTask(t Task, position int) {
	if t.Name == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> does not contains the field <Name>")
	}
	if t.Kind == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> does not contains the field <Kind>")
	}
	if t.Kind != "query" && t.Kind != "array" && t.Kind != "csv" {
		log.Println("Task Error in the json file: <Tasks #", position, "> of <", t.Name, ">, <Kind:", t.Kind, "  is not supported")
		log.Fatalln("Task Error: supported values are <query>,<array>,<csv>")
	}
	if t.Command == "" && t.Kind != "csv" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> does not contains the field <Command>")
	}
	if t.Connection == "" && t.Kind != "array" && t.Kind != "csv" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> does not contains the field <Connection>")
	}
	if t.OutputType == "" && t.Kind != "csv" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> does not contains the field <OutputType>")
	}
	if t.OutputType != "excel" && t.OutputType != "memory" && t.OutputType != "reference" && t.OutputType != "csv" {
		log.Println("Task Error in the json file: <Tasks #", position, "> of <", t.Name, ">, <OutputType:", t.OutputType, "  is not supported")
		log.Fatalln("Task Error: supported values are <memory>,<excel>,<reference>,<csv>")
	}
	if t.OutputType == "excel" && t.FileName == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> output type <Excel> must have a field <FileName>")
	}
	if t.Kind == "csv" && t.FileName == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> the field <FileName> cannot be empty for a task <csv>")
	}
}
