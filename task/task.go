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
	Id          string
	Kind        string
	Name        string
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

func validate_task(t Task) {
	if t.Kind == "" {
		log.Fatalln("The json file for 'Tasks' does not contains the field 'Kind'   , check for a typo")
	}
	if t.Id == "" {
		log.Fatalln("The json file for 'Tasks' does not contains the field 'Id'   ,check for a typo")
	}
	if t.Command == "" {
		log.Fatalln("The json file for 'Tasks' does not contains the field 'Command'   ,check for a typo")
	}
	if t.Connection == "" {
		log.Fatalln("The json file for 'Tasks' does not contains the field 'Connection'   ,check for a typo")
	}
	if t.Name == "" {
		log.Fatalln("The json file for 'Tasks' does not contains the field 'Name'   ,check for a typo")
	}
	if t.OutputType == "" {
		log.Fatalln("The json file for 'Tasks' does not contains the field 'OutputType'   ,check for a typo")
	}
	if t.OutputType == "excel" && t.OutputName == "" {
		log.Fatalln("The json file for 'Tasks' : A task of output type 'Excel' must have a field 'OutputName'  ,check for a typo")
	}
}

func RunAll(tasks []Task) {
	for _, t := range tasks {
		validate_task(t)
		switch t.Kind {
		case "query":
			RunQuery(t)
		default:
			log.Fatal("Invalid task kind" + t.Kind)
		}
	}
}
