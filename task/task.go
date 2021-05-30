package task

import (
	"context"

	con "jyv.com/goarchive/connection"
)

type Status string

const (
	Running   Status = "runnning"
	Paused    Status = "paused"
	Cancelled Status = "cancelled"
	Completed Status = "completed"
	Failed    Status = "failed"
	Success   Status = "success"
)

type ITask interface {
	//GetStatus() Status
	Run(ctx context.Context) (Status, error)
	//Stop(ctx context.Context) (Status, error)
	//Start(ctx context.Context) (Status, error)
	//Cancel(ctx context.Context) (Status, error)
}

type Task struct {
	Id          string
	Kind        string
	Name        string
	Description string
	Connection  string
	Sql         string
	Output      string
	File        string
}

type ETL struct {
	Connections []con.Connection
	Tasks       []Task
}

func RunAll(tasks []Task) {
	for _, t := range tasks {
		switch t.Kind {
		case "query":
			RunQuery(t)
		}
	}
}
