package util

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/360EntSecGroup-Skylar/excelize/v2"
	valid "github.com/asaskevich/govalidator"
	"jyv.com/goarchive/connection"
)

type MapStringScan struct {
	// cp are the column pointers
	cp []interface{}
	// row contains the final result
	row      map[string]string
	colCount int
	colNames []string
}

func NewMapStringScan(columnNames []string) *MapStringScan {
	lenCN := len(columnNames)
	s := &MapStringScan{
		cp:       make([]interface{}, lenCN),
		row:      make(map[string]string, lenCN),
		colCount: lenCN,
		colNames: columnNames,
	}
	for i := 0; i < lenCN; i++ {
		s.cp[i] = new(sql.RawBytes)
	}
	return s
}

func (s *MapStringScan) Update(rows *sql.Rows) error {
	if err := rows.Scan(s.cp...); err != nil {
		return err
	}

	for i := 0; i < s.colCount; i++ {
		if rb, ok := s.cp[i].(*sql.RawBytes); ok {
			s.row[s.colNames[i]] = string(*rb)
			*rb = nil // reset pointer to discard current value to avoid a bug
		} else {
			return fmt.Errorf("Cannot convert index %d column %s to type *sql.RawBytes", i, s.colNames[i])
		}
	}
	return nil
}

func (s *MapStringScan) Get() map[string]string {
	return s.row
}

func mssql_isvalid_guid(val string) (string, bool) {
	if len(val) == 16 {
		// reorder bytes
		// 3D451C51-823B-4F35-83CF-BD9F642012D9
		nbytes := make([]byte, 16)
		nbytes[0] = val[3]
		nbytes[1] = val[2]
		nbytes[2] = val[1]
		nbytes[3] = val[0]

		nbytes[4] = val[5]
		nbytes[5] = val[4]

		nbytes[6] = val[7]
		nbytes[7] = val[6]

		nbytes[8] = val[8]
		nbytes[9] = val[9]

		nbytes[10] = val[10]
		nbytes[11] = val[11]
		nbytes[12] = val[12]
		nbytes[13] = val[13]
		nbytes[14] = val[14]
		nbytes[15] = val[15]

		guid := hex.EncodeToString(nbytes)
		guid = guid[0:8] + "-" + guid[8:12] + "-" + guid[12:16] + "-" + guid[16:20] + "-" + guid[20:32]
		if valid.IsUUID(guid) {
			guid = strings.ToUpper(guid)
			return guid, true
		} else {
			return "", false
		}
	} else {
		return "", false
	}
}

func saveExcel(excel *excelize.File, sheet string, coor string, val string) {
	if valid.IsFloat(val) {
		fl, _ := valid.ToFloat(val)
		excel.SetCellFloat(sheet, coor, fl, -1, 64)
	} else if valid.IsInt(val) {
		iv, _ := valid.ToInt(val)
		excel.SetCellInt(sheet, coor, int(iv))
	} else if valid.IsTime(val, time.RFC3339) {
		t, _ := time.Parse(time.RFC3339, val)
		excel.SetCellValue(sheet, coor, t)
	} else {
		guid, isvalid := mssql_isvalid_guid(val)
		if isvalid {
			excel.SetCellValue(sheet, coor, guid)
		} else {
			excel.SetCellValue(sheet, coor, val)
		}
	}
}

func saveExcelType(excel *excelize.File, sheet string, coor string, t reflect.StructField, val reflect.Value) {
	//excel.SetCellValue(sheet, coor, val.String())
	switch t.Type.Kind() {
	case reflect.Int, reflect.Int16, reflect.Int8, reflect.Int32, reflect.Int64:
		excel.SetCellInt(sheet, coor, int(val.Int()))
	case reflect.Float32, reflect.Float64:
		excel.SetCellFloat(sheet, coor, val.Float(), -1, 64)
	case reflect.String:
		excel.SetCellValue(sheet, coor, val.String())
	default:
		stype := val.String()
		if stype == "<time.Time Value>" {
			ti := reflect.NewAt(val.Type(), unsafe.Pointer(val.UnsafeAddr())).Elem().Interface().(time.Time)
			s := ti.Format(time.RFC3339)
			timeout, _ := time.Parse(time.RFC3339, s)
			excel.SetCellValue(sheet, coor, timeout)
		} else {
			excel.SetCellValue(sheet, coor, val)
		}
	}
}

func QuerySaveExcel(name string, db *sql.DB, query string, output string) {
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	log.Println(columnNames)
	if err != nil {
		log.Fatal(err)
	}

	// put the columns into the Excel file
	f := excelize.NewFile()
	for col_count, col_name := range columnNames {
		coor, err := excelize.CoordinatesToCellName(col_count+1, 1, false)
		if err != nil {
			log.Fatal(err)
		}
		f.SetCellValue("Sheet1", coor, col_name)
	}

	// put each row into the Excel file
	row_count := 2
	rc := NewMapStringScan(columnNames)
	for rows.Next() {
		err := rc.Update(rows)
		if err != nil {
			log.Fatal(err)
		}
		row := rc.Get()
		for col_count, col_name := range columnNames {
			coor, err := excelize.CoordinatesToCellName(col_count+1, row_count, false)
			if err != nil {
				log.Fatal(err)
			}
			val := row[col_name]
			saveExcel(f, "Sheet1", coor, val)
		}
		row_count++
	}
	if err := f.SaveAs(output); err != nil {
		log.Println(err)
	}
	log.Println("QuerySaveExcel success")

}

func Query(driver string, con string, query string, callback func(rows *sql.Rows) interface{}) ([]string, []interface{}) {
	db, err := connection.CreateOrGetDB("corpo", driver, con)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	log.Println(columnNames)
	if err != nil {
		log.Fatal(err)
	}

	out := make([]interface{}, 0)
	for rows.Next() {
		rec := callback(rows)
		if err != nil {
			log.Fatal(err)
		}
		out = append(out, rec)
	}
	return columnNames, out
}

func SaveExcel(columnNames []string, list []interface{}, output string) {
	f := excelize.NewFile()
	for col_count, col_name := range columnNames {
		coor, err := excelize.CoordinatesToCellName(col_count+1, 1, false)
		if err != nil {
			log.Fatal(err)
		}
		f.SetCellValue("Sheet1", coor, col_name)
	}

	for row_count, rec := range list {
		getType := reflect.TypeOf(rec).Elem()
		getValue := reflect.ValueOf(rec).Elem()
		for i := 0; i < getValue.NumField(); i++ {
			coor, _ := excelize.CoordinatesToCellName(i+1, row_count+2, false)
			value := getValue.Field(i)
			t := getType.Field(i)
			saveExcelType(f, "Sheet1", coor, t, value)
		}
	}

	if err := f.SaveAs(output); err != nil {
		log.Println(err)
	}
}
