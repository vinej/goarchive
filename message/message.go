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
	mapmsg["0001"] = "Fail to read ini file: %s"
	mapmsg["0002"] = "Parameter <driver> is mandatory>"
	mapmsg["0003"] = "Parameter <con> is mandatory>"
	mapmsg["0004"] = "Parameter <query> is mandatory>"
	mapmsg["0005"] = "Syntaxe error"
	mapmsg["0006"] = "Unknown parameter <%s>"
	mapmsg["0007"] = "Error opening log file"
	mapmsg["0008"] = "START processing"
	mapmsg["0009"] = "END processing"
}

func WriteMessageToFile(filename string) {
	filedata, _ := json.MarshalIndent(mapmsg, "", " ")
	_ = ioutil.WriteFile(filename, filedata, 0644)
}
