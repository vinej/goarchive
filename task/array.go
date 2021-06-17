package task

import (
	"log"
	"strings"

	con "jyv.com/goarchive/connection"
	"jyv.com/goarchive/message"
	"jyv.com/goarchive/util"
)

const ARRAY_KIND = "Kind"
const ARRAY_NAME = "Name"
const ARRAY_COMMAND = "Command"
const ARRAY_DESCRIPTION = "Description"
const ARRAY_OUTPUTTYPE = "OutputType"
const OUTPUT_TYPE_MEMORY = "memory"

type Array struct {
	Task
	Description string
	Command     string
	OutputType  string
}

func (array *Array) Run(acon []con.Connection, position int) {
	m := new(Memory)
	m.columnNames = make([]string, 1)
	m.columnNames[0] = array.Task.Name
	m.rows = make([]map[string]string, 0)
	values := strings.Split(array.Command, "|")
	for _, v := range values {
		r := make(map[string]string, 1)
		r[array.Task.Name] = v
		m.rows = append(m.rows, r)
	}
	mapqry[array.Task.Name] = m
}

func (array *Array) Validate(acon []con.Connection, position int) {
	if array.Task.Name == "" {
		log.Fatalf(message.GetMessage(26), position, ARRAY_NAME)
	}
	if array.Task.Kind == "" {
		log.Fatalf(message.GetMessage(27), position, array.Task.Name, ARRAY_KIND)
	}
	if array.Command == "" {
		log.Fatalf(message.GetMessage(27), position, array.Task.Name, ARRAY_COMMAND)
	}
	if array.OutputType != OUTPUT_TYPE_MEMORY {
		log.Printf(message.GetMessage(28), position, array.Task.Name, array.OutputType)
		log.Fatal(message.GetMessage(29))
	}
}

func (array *Array) Transform(m map[string]interface{}) {
	array.Task.Kind = util.GetFieldValueFromMap(m, ARRAY_KIND)
	array.Task.Name = util.GetFieldValueFromMap(m, ARRAY_NAME)
	array.Command = util.GetFieldValueFromMap(m, ARRAY_COMMAND)
	array.Description = util.GetFieldValueFromMap(m, ARRAY_DESCRIPTION)
	array.OutputType = util.GetFieldValueFromMap(m, ARRAY_OUTPUTTYPE)
}

func (array *Array) GetTask() Task { return array.Task }

func (array *Array) ValidateEtl(Tasks []ITask, position int) {}
