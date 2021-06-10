package task

import (
	"encoding/csv"
	"os"
)

func RunCsv(task Task) {
	m := new(Memory)
	m.columnNames, m.rows, _ = read_data(task.FileName)
	mapqry[task.Name] = m
}

func read_data(fileName string) (columns []string, rows []map[string]string, err error) {
	f, err := os.Open(fileName)

	if err != nil {
		return []string{}, []map[string]string{}, err
	}

	defer f.Close()

	r := csv.NewReader(f)

	columns, err = r.Read()
	if err != nil {
		return []string{}, []map[string]string{}, err
	}

	rows = make([]map[string]string, 0)

	for {
		rec, _ := r.Read()
		if rec == nil {
			break
		}
		m := make(map[string]string, 1)
		for i, c := range columns {
			m[c] = rec[i]
		}
		rows = append(rows, m)
	}

	return columns, rows, nil
}
