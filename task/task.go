package task

import (
	"log"

	con "jyv.com/goarchive/connection"
	"jyv.com/goarchive/util"
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
	if len(params) > 2 {
		log.Fatal("Parameter Error in the json file:  <Task:", t.Name, "> of <Task #", taskposition, "> has more than 2 parameters")
	}
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
}

func ValidateTaskConnection(t Task, connections []con.Connection, position int) {
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
	if t.Kind != "query" {
		log.Println("Task Error in the json file: <Tasks #", position, "> of <", t.Name, ">, <Kind:", t.Kind, "  is not supported")
		log.Fatalln("Task Error: supported values are query")
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
	if t.OutputType != "excel" && t.OutputType != "memory" && t.OutputType != "reference" {
		log.Println("Task Error in the json file: <Tasks #", position, "> of <", t.Name, ">, <OutputType:", t.OutputType, "  is not supported")
		log.Fatalln("Task Error: supported values are <memory>,<excel>,<reference>")
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
