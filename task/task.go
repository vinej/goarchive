package task

import (
	con "jyv.com/goarchive/connection"
)

// declare interface

type ITask interface {
	Run(acon []con.Connection, position int)
	Validate(acon []con.Connection, position int)
	ValidateEtl(Tasks []ITask, position int)
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

func (task *Task) Run(acon []con.Connection, position int)      {}
func (task *Task) Validate(acon []con.Connection, position int) {}
func (task *Task) Transform(m map[string]interface{})           {}
func (task *Task) GetTask() Task                                { return *task }
func (task *Task) ValidateEtl(Tasks []ITask, position int)      {}

func RunETL(etl *ETL) {
	ValidateAll(etl)
	RunAll(etl)
}

func ValidateAll(etl *ETL) {
	ValidateTaskUniqueNames(etl.Tasks)
	for i, t := range etl.Tasks {
		ValidateTask(t.GetTask(), i)
		t.Validate(etl.Connections, i)
		t.ValidateEtl(etl.Tasks, i)
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
		kind := t[TASK_KIND].(string)
		switch kind {
		case TASK_KIND_ARRAY:
			ar := new(Array)
			ar.Transform(t)
			etlout.Tasks = append(etlout.Tasks, ar)
		case TASK_KIND_CSV:
			csv := new(Csv)
			csv.Description = "ok"
			csv.Transform(t)
			etlout.Tasks = append(etlout.Tasks, csv)
		case TASK_KIND_QUERY:
			query := new(Query)
			(*query).Transform(t)
			etlout.Tasks = append(etlout.Tasks, query)
		}
	}
	return etlout
}
