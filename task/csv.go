package task

import (
	"encoding/csv"
	"os"

	con "jyv.com/goarchive/connection"
	"jyv.com/goarchive/util"
)

type Csv struct {
	Task
	Description string
	FileName    string
}

func (csv *Csv) Run(acon []con.Connection, position int) {
	m := new(Memory)
	m.columnNames, m.rows, _ = read_data(csv.FileName)
	mapqry[csv.Task.Name] = m
}

func (csv *Csv) Validate(acon []con.Connection, position int) {

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

func (csv *Csv) Transform(m map[string]interface{}) {
	csv.Task.Kind = util.GetFieldValueFromMap(m, "Kind")
	csv.Task.Name = util.GetFieldValueFromMap(m, "Name")
	csv.Description = util.GetFieldValueFromMap(m, "Description")
	csv.FileName = util.GetFieldValueFromMap(m, "FileName")
}

func (csv *Csv) GetTask() Task { return csv.Task }
