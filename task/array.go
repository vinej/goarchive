package task

import "strings"

var mapele map[string][]string = make(map[string][]string)

func RunArray(task *Task) {
	a := strings.Split(task.Command, ",")
	mapele[task.Name] = a
}

func GetArray(name string) []string {
	return mapele[name]
}
