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

func get_connection(conlist []con.Connection, name string) *con.Connection {
	if name == "" {
		return nil
	}

	for _, c := range conlist {
		if c.Name == name {
			return &c
		}
	}
	log.Fatalln("Connection <" + name + "> not found")
	return nil
}

func RunAll(conlist []con.Connection, tasks []Task) {
	for _, t := range tasks {
		ctx := get_connection(conlist, t.Connection)
		switch t.Kind {
		case "array":
			RunArray(t)
		case "query":
			RunQuery(ctx, t)
		default:
			log.Fatal("Task: Invalid task kind" + t.Kind)
		}
	}
}
