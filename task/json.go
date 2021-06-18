package task

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

func LoadJson(file string) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Panic(err)
	}
	etljson := new(ETLJson)
	err = json.Unmarshal(data, etljson)
	if err != nil {
		log.Panic(err)
	}
	etl := RemapETL(etljson)
	RunETL(etl)
}
