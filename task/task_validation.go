package task

import (
	"log"

	util "jyv.com/goarchive/util"
)

func ValidateTaskUniqueNames(tasks []ITask) {
	names := make([]string, 0)
	isFirst := true
	for i, t := range tasks {
		atask := t.GetTask()
		if util.Contains(names, atask.Name) && !isFirst {
			log.Fatalln("Task error in the json file: the <Task:", atask.Name, "> of <Task:", i+1, "> already exists")
		} else {
			names = append(names, atask.Name)
		}
		isFirst = false
	}
}

func ValidateTask(t Task, position int) {
	if t.Name == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> does not contains the field <Name>")
	}
	if t.Kind == "" {
		log.Fatalln("Task Error in the json file: <Tasks #", position, "> of <", t.Name, "> does not contains the field <Kind>")
	}
	if t.Kind != "query" && t.Kind != "array" && t.Kind != "csv" {
		log.Println("Task Error in the json file: <Tasks #", position, "> of <", t.Name, ">, <Kind:", t.Kind, "  is not supported")
		log.Fatalln("Task Error: supported values are <query>,<array>,<csv>")
	}
}
