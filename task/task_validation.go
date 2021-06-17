package task

import (
	"log"

	"jyv.com/goarchive/message"
	util "jyv.com/goarchive/util"
)

const TASK_NAME = "Name"
const TASK_KIND = "Kind"
const TASK_KIND_QUERY = "query"
const TASK_KIND_ARRAY = "array"
const TASK_KIND_CSV = "csv"

func ValidateTaskUniqueNames(tasks []ITask) {
	names := make([]string, 0)
	isFirst := true
	for i, t := range tasks {
		atask := t.GetTask()
		if util.Contains(names, atask.Name) && !isFirst {
			log.Fatalf(message.GetMessage(47), i+1, atask.Name)
		} else {
			names = append(names, atask.Name)
		}
		isFirst = false
	}
}

func ValidateTask(t Task, position int) {
	if t.Name == "" {
		log.Fatalf(message.GetMessage(48), position, TASK_NAME)
	}
	if t.Kind == "" {
		log.Fatalf(message.GetMessage(49), position, t.Name, TASK_KIND)
	}
	if t.Kind != TASK_KIND_QUERY && t.Kind != TASK_KIND_ARRAY && t.Kind != TASK_KIND_CSV {
		log.Printf(message.GetMessage(50), position, t.Name, t.Kind)
		log.Fatalf(message.GetMessage(51), TASK_KIND_QUERY, TASK_KIND_ARRAY, TASK_KIND_CSV)
	}
}
