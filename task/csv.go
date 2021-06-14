package task

import (
	ecsv "encoding/csv"
	"os"

	con "jyv.com/goarchive/connection"
	"jyv.com/goarchive/util"
)

type Csv struct {
	Task
	Description string
	FileName    string
}

const CSV_KIND = "Kind"
const CSV_NAME = "Name"
const CSV_DESCRIPTION = "Description"
const CSV_FILENAME = "FileName"

func (csv *Csv) Run(_ []con.Connection, position int) {
	m := new(Memory)
	m.columnNames, m.rows, _ = csv.read_data()
	mapqry[csv.Task.Name] = m
}

func (csv *Csv) Validate(_ []con.Connection, position int) {

}

func (csv *Csv) Transform(m map[string]interface{}) {
	csv.Task.Kind = util.GetFieldValueFromMap(m, CSV_KIND)
	csv.Task.Name = util.GetFieldValueFromMap(m, CSV_NAME)
	csv.Description = util.GetFieldValueFromMap(m, CSV_DESCRIPTION)
	csv.FileName = util.GetFieldValueFromMap(m, CSV_FILENAME)
}

func (csv *Csv) GetTask() Task { return csv.Task }

func (csv *Csv) ValidateEtl(Tasks []ITask, position int) {}

func (csv *Csv) read_data() (columns []string, rows []map[string]string, err error) {
	fileName := csv.FileName
	f, err := os.Open(fileName)

	if err != nil {
		return []string{}, []map[string]string{}, err
	}

	defer f.Close()

	r := ecsv.NewReader(f)

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
