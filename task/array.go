package task

import "strings"

//var mapqry map[string]*Memory = make(map[string]*Memory)

func RunArray(task Task) {
	m := new(Memory)
	m.columnNames = make([]string, 1)
	m.columnNames[0] = task.Name
	m.rows = make([]map[string]string, 0)
	values := strings.Split(task.Command, "|")
	for _, v := range values {
		r := make(map[string]string, 1)
		r[task.Name] = v
		m.rows = append(m.rows, r)
	}
	mapqry[task.Name] = m
}
