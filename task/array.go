package task

import (
	"log"
	"strings"

	con "jyv.com/goarchive/connection"
	"jyv.com/goarchive/util"
)

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
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> does not contains the field <Name>")
	}
	if array.Task.Kind == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", array.Task.Name, "> does not contains the field <Kind>")
	}
	if array.Command == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", array.Task.Name, "> does not contains the field <Command>")
	}
	if array.OutputType != "memory" {
		log.Println("Task Error in the json file: <Tasks #", position, "> of <", array.Task.Name, ">, <OutputType:", array.OutputType, "  is not supported")
		log.Fatalln("Task Error: supported values are <memory>,<excel>,<reference>,<csv>")
	}
}

func (array *Array) Transform(m map[string]interface{}) {
	array.Task.Kind = util.GetFieldValueFromMap(m, "Kind")
	array.Task.Name = util.GetFieldValueFromMap(m, "Name")
	array.Command = util.GetFieldValueFromMap(m, "Command")
	array.Description = util.GetFieldValueFromMap(m, "Description")
	array.OutputType = util.GetFieldValueFromMap(m, "OutputType")
}

func (array *Array) GetTask() Task { return array.Task }
