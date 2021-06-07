package task

import (
	"log"

	con "jyv.com/goarchive/connection"
)

type Parameter struct {
	Names       []string
	Fields      []string
	Source      string
	UseDatabase string
	Kind        string
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

func RunAll(tasks []Task) {
	for _, t := range tasks {
		switch t.Kind {
		case "array":
			RunArray(t)
		case "query":
			RunQuery(t)
		default:
			log.Fatal("Task: Invalid task kind" + t.Kind)
		}
	}
}
