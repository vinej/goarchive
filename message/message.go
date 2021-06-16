package message

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

type MapMsG map[string]string

var mapmsg = make(MapMsG)

func GetMessage(id string) string {
	msg, ok := mapmsg[id]
	if !ok {
		return id + ": Message ID not found into the list"
	} else {
		return id + ":" + msg
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
	mapmsg["0001"] = "INI Error: Fail to read ini file: %s"
	mapmsg["0002"] = ""
	mapmsg["0003"] = "INI Error: Parameter <con> is mandatory>"
	mapmsg["0004"] = "INI Error: Parameter <query> is mandatory>"
	mapmsg["0005"] = "INI Error: Syntaxe error"
	mapmsg["0006"] = "INI Error: Unknown parameter <%s>"
	mapmsg["0007"] = "RUN Error: Error opening log file"
	mapmsg["0008"] = "START processing"
	mapmsg["0009"] = "END processing"
	mapmsg["0010"] = "RUN Error: connection' name <%s> is not found"
	mapmsg["0011"] = "JSON Connection Error: the connection' name <%s> at the position <%d> already exists"
	mapmsg["0012"] = "JSON Connection Error: the connection at the position <%d> does not contains the field <Name>"
	mapmsg["0013"] = "JSON Connection Error: the connection at the position <%d> does not contains the field <Driver>"
	mapmsg["0014"] = "JSON Connection Error: the driver <%s> is not supported at the connection position <%d>"
	mapmsg["0015"] = "JSON Connection Error: the supported driver(s) are <sqlserver>"
	mapmsg["0016"] = "JSON Connection Error: the connection at the position <%d> does not contains the field <ConnectionString>"
}

func WriteMessageToFile(filename string) {
	filedata, _ := json.MarshalIndent(mapmsg, "", " ")
	_ = ioutil.WriteFile(filename, filedata, 0644)
}
