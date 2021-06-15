package message

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

var mapmsg map[string]string = make(map[string]string)

type MSGJson struct {
	Messages []interface{}
}

func GetMessage(id string) string {
	msg, ok := mapmsg[id]
	if !ok {
		return id + ":" + id
	} else {
		return id + ":" + msg
	}
}

func FillMessage(filename string) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Panic(err)
	}
	m := new(MSGJson)
	err = json.Unmarshal(data, m)
	if err == nil {
		for _, msg := range m.Messages {
			mt := msg.(map[string]interface{})
			mapmsg[mt["id"].(string)] = mt["message"].(string)
		}
	} else {
		log.Fatal(err)
	}
}
