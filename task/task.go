package task

import (
	con "jyv.com/goarchive/connection"
)

// declare interface

type ITask interface {
	Run(acon []con.Connection, position int)
	Validate(acon []con.Connection, position int)
	Transform(m map[string]interface{})
	GetTask() Task
}

type Parameter struct {
	Names       []string
	Fields      []string
	Source      string
	UseDatabase string
	Kind        string
}

type Task struct {
	Kind string
	Name string
}

type ETL struct {
	Connections []con.Connection
	Tasks       []ITask
}

type ETLJson struct {
	Connections []con.Connection
	Tasks       []interface{}
}

func (task Task) Run(acon []con.Connection, position int)      {}
func (task Task) Validate(acon []con.Connection, position int) {}
func (task Task) Transform(m map[string]interface{})           {}
func (task Task) GetTask() Task                                { return task }

func RunETL(etl *ETL) {
	ValidateAll(etl)
	RunAll(etl)
}

func ValidateAll(etl *ETL) {
	ValidateTaskUniqueNames(etl.Tasks)
	for i, t := range etl.Tasks {
		ValidateTask(t.GetTask(), i)
		t.Validate(etl.Connections, i)
	}
}

func RunAll(etl *ETL) {
	for i, t := range etl.Tasks {
		t.Run(etl.Connections, i)
	}
}

func RemapETL(etl *ETLJson) (etlout *ETL) {
	etlout = new(ETL)
	etlout.Connections = make([]con.Connection, 0)
	etlout.Connections = append(etlout.Connections, etl.Connections...)
	// transform task
	for _, c := range etl.Tasks {
		t := c.(map[string]interface{})
		kind := t["Kind"].(string)
		switch kind {
		case "array":
			ar := new(Array)
			ar.Transform(t)
			etlout.Tasks = append(etlout.Tasks, ar)
		case "csv":
			csv := new(Csv)
			csv.Description = "ok"
			csv.Transform(t)
			etlout.Tasks = append(etlout.Tasks, csv)
		case "query":
			query := new(Query)
			(*query).Transform(t)
			etlout.Tasks = append(etlout.Tasks, query)
		}
	}
	return etlout
}
