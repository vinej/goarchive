package message

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
)

const MSG_NOT_FOUND = "Message ID not found into the list"

type MapMsG map[int]string

var mapmsg = make(MapMsG)

func GetMessage(id int) string {
	msg, ok := mapmsg[id]
	if !ok {
		return fmt.Sprint(id) + ":" + MSG_NOT_FOUND
	} else {
		return fmt.Sprint(id) + ":" + msg
	}
}

func FillMessage(filename string) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Panic(err)
	}
	err = json.Unmarshal(data, &mapmsg)
	if err != nil {
		log.Fatal(err)
	}
}

func FillInternalMessage() {
	mapmsg[1] = "INI Error: Fail to read ini file: '%s'"
	mapmsg[2] = "Message ID not found into the list"
	mapmsg[3] = "INI Error: Parameter <con> is mandatory>"
	mapmsg[4] = "INI Error: Parameter <query> is mandatory>"
	mapmsg[5] = "INI Error: Syntaxe error"
	mapmsg[6] = "INI Error: Unknown parameter '%s'"
	mapmsg[7] = "RUN Error: Error opening log file"
	mapmsg[8] = "START processing"
	mapmsg[9] = "END processing"
	mapmsg[10] = "RUN Error: connection' name '%s' is not found"
	mapmsg[11] = "JSON Connection Error: the connection' name '%s' at the position '%d' already exists"
	mapmsg[12] = "JSON Connection Error: the connection at the position '%d' does not contains the field <Name>"
	mapmsg[13] = "JSON Connection Error: the connection at the position '%d' does not contains the field <Driver>"
	mapmsg[14] = "JSON Connection Error: the driver '%s' is not supported at the connection position '%d'"
	mapmsg[15] = "JSON Connection Error: the supported driver(s) are <sqlserver>"
	mapmsg[16] = "JSON Connection Error: the connection at the position '%d' does not contains the field <ConnectionString>"
	mapmsg[17] = "MAPSQL Error: Cannot convert index '%d' of column '%s' to type *sql.RawBytes"
	mapmsg[18] = "INFO: Saving into CSV with the query: '%s'"
	mapmsg[19] = "FATAL: Failed to open file"
	mapmsg[20] = "RUN Error: failed to open file output CSV file '%s'"
	mapmsg[21] = "INFO: Success saved into the CSV file '%s'"
	mapmsg[22] = "INFO: Starting saving into the CSV file '%s'"
	mapmsg[23] = "INFO: Starting saving into the Excel file '%s'"
	mapmsg[24] = "INFO: Saving into Excel with the query: '%s'"
	mapmsg[25] = "INFO: Success saved into the Excel file '%s'"
	mapmsg[26] = "JSON Array Error: The task at position '%d' does not contains the field '%s'"
	mapmsg[27] = "JSON Array Error: The task at position '%d' with the name '%s' does not contains the field '%s'"
	mapmsg[28] = "JSON Array Error: The task at position '%d' with the name '%s' does not support the OutputType: '%s'"
	mapmsg[29] = "JSON Array Error: The supported OutputType is <memory> only"
	mapmsg[30] = "JSON Parameter Error at position %d of the task at position %d: the <Source> reference' name does not exist in other tasks"
	mapmsg[31] = "JSON Parameter Error: the task '%s' at position '%d': the first parameter must have a <Kind> equal to '%s'"
	mapmsg[32] = "JSON Parameter Error: the parameter at position '%d' of the task at position '%d' does not contains the field <%>"
	mapmsg[33] = "JSON Parameter Error at position '%d' of the task at position '%d': the kind '%s' is not supported"
	mapmsg[34] = "JSON Parameter Error: The supported kind are '%s' and '%s' only"
	mapmsg[35] = "JSON Parameter Error at position '%d' of the task at position '%d': <UseDatabase> is not supported for kind '%s'"
	mapmsg[36] = "JSON Query Error at the task position '%d': the connection '%s' does not exist into the connections' section:"
	mapmsg[37] = "JSON Query Error: The task at position '%d' does not contains the field '%s'"
	mapmsg[38] = "JSON Query Error: The task at position '%d' with the name '%s' does not contains the field '%s'"
	mapmsg[30] = "JSON Query Error at the task at position '%d' with the name '%s': the kind '%s' is not supported"
	mapmsg[40] = "JSON Query Error: the supported kind are '%s','%s','%s'"
	mapmsg[41] = "JSON Query Error: the task at position '%d' with the name '%s' does not suport the OutputType '%s'"
	mapmsg[42] = "JSON Query Error: the supported type are '%s','%s','%s','%s'"
	mapmsg[43] = "JSON Query Error: the task at position '%d' with the name '%s' the output type '%s' must have a field '%s'"
	mapmsg[44] = "JSON Query Error: the first parameter of a query task cannot have the kind <%>"
	mapmsg[45] = "JSON Query Error: the source '%s' is not available. Maybe you used a <reference> instead of <memory> OutputType for the task"
	mapmsg[46] = "JSON Query Error: the output type '%s' is not supported, check for a typo"
	mapmsg[47] = "JSON Task Error at the task position '%d': the task name '%s' already exist"
	mapmsg[48] = "JSON Task Error: the task at the position '%d' does not contain the field '%s'"
	mapmsg[49] = "JSON Task Error: the task at the position '%d' with the name '%s' does not contain the field '%s'"
	mapmsg[50] = "JSON Task Error: the task at the position '%d' with the name '%s' the kind '%s' is not supported"
	mapmsg[51] = "JSON Task Error: the supported tasks are '%s','%s','%s'"
}

func WriteMessageToFile(filename string) {
	filedata, _ := json.MarshalIndent(mapmsg, "", " ")
	_ = ioutil.WriteFile(filename, filedata, 0644)
}
