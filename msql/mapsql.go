package msql

import (
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"log"
	"os"
	"strings"
	"time"

	"github.com/360EntSecGroup-Skylar/excelize/v2"
	valid "github.com/asaskevich/govalidator"
	"github.com/jinzhu/copier"
	con "jyv.com/goarchive/connection"
	"jyv.com/goarchive/message"
)

const TIME_FORMAT = "2006-01-02 15:04:05"
const SHEET1 = "Sheet1"

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
			// cannot convert index i of col s
			log.Fatalf(message.GetMessage(17), i, s.colNames[i])
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
	field := getField(val)
	excel.SetCellValue(sheet, coor, field)
}

func getField(val string) interface{} {
	if valid.IsFloat(val) {
		fl, _ := valid.ToFloat(val)
		return fl
	} else if valid.IsInt(val) {
		iv, _ := valid.ToInt(val)
		return iv
	} else if valid.IsTime(val, time.RFC3339) {
		t, _ := time.Parse(time.RFC3339, val)
		return t
	} else {
		guid, isvalid := mssql_isvalid_guid(val)
		if isvalid {
			return guid
		} else {
			return val
		}
	}
}

func getStringField(val string) string {
	if valid.IsFloat(val) {
		//fl, _ := valid.ToFloat(val)
		//return fl
		return val
	} else if valid.IsInt(val) {
		//iv, _ := valid.ToInt(val)
		//return iv
		return val
	} else if valid.IsTime(val, time.RFC3339) {
		t, _ := time.Parse(time.RFC3339, val)
		//return t
		return t.Format(TIME_FORMAT)
	} else {
		guid, isvalid := mssql_isvalid_guid(val)
		if isvalid {
			return guid
		} else {
			return val
		}
	}
}

func QuerySaveCsv(ctx *con.Connection, name string, query string, output string) {
	log.Printf(message.GetMessage(22), output)
	db, _ := con.GetDB(ctx)
	defer db.Close()
	log.Printf(message.GetMessage(18), query)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create(output)
	if err != nil {
		log.Printf(message.GetMessage(20), output)
		log.Fatal(err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// put the columns into the Excel file
	w.Write(columnNames)

	rc := NewMapStringScan(columnNames)
	for rows.Next() {
		err := rc.Update(rows)
		if err != nil {
			log.Fatal(err)
		}
		row := rc.Get()
		// ceate a []string for map[string]
		ar := make([]string, 0)
		for _, col_name := range columnNames {
			// TODO remove excluded columns
			// TODO anonymized columns
			field := getStringField(row[col_name])
			ar = append(ar, field)
		}
		w.Write(ar)
	}
	log.Printf(message.GetMessage(21), output)
}

func QuerySaveExcel(ctx *con.Connection, name string, query string, output string) {
	log.Printf(message.GetMessage(23), output)
	db, _ := con.GetDB(ctx)
	defer db.Close()
	log.Printf(message.GetMessage(24), query)
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
		f.SetCellValue(SHEET1, coor, col_name)
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
			saveExcel(f, SHEET1, coor, val)
		}
		row_count++
	}
	if err := f.SaveAs(output); err != nil {
		log.Println(err)
	}
	log.Printf(message.GetMessage(25), output)
}

func Query(ctx *con.Connection, query string) ([]string, []map[string]string) {
	db, _ := con.GetDB(ctx)
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

	rc := NewMapStringScan(columnNames)
	out := make([]map[string]string, 0)
	for rows.Next() {
		err := rc.Update(rows)
		if err != nil {
			log.Fatal(err)
		}
		rec := rc.Get()
		if err != nil {
			log.Fatal(err)
		}
		tmp := new(map[string]string)
		copier.Copy(tmp, rec)
		out = append(out, *tmp)
	}
	return columnNames, out
}
