package task

import (
	"log"

	con "jyv.com/goarchive/connection"
)

type Parameter struct {
	Names  []string
	Fields []string
	Source string
	Kind   string
}

type Task struct {
	Name        string
	Kind        string
	Description string
	Connection  string
	Command     string
	OutputType  string
	OutputName  string
	Parameters  []Parameter
}

type ETL struct {
	Connections []con.Connection
	Tasks       []Task
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
		log.Fatal("Parameter Error in the json file: <Parameters #", position, "> of <Task #", taskposition, "> does not contains the field : <Kind>")
	}
}

func ValidateTask(t Task, position int) {
	if t.Name == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> does not contains the field <Name>")
	}
	if t.Kind == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> does not contains the field <Kind>")
	}
	if t.Command == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> does not contains the field <Command>")
	}
	if t.Connection == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> does not contains the field <Connection>")
	}
	if t.OutputType == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> does not contains the field <OutputType>")
	}
	if t.OutputType == "excel" && t.OutputName == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> output type <Excel> must have a field <OutputName>")
	}
}

func RunAll(tasks []Task) {
	for _, t := range tasks {
		switch t.Kind {
		case "query":
			RunQuery(t)
		default:
			log.Fatal("Task: Invalid task kind" + t.Kind)
		}
	}
}
