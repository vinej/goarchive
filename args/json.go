package args

import (
	"encoding/json"
	"io/ioutil"
	"log"

	"jyv.com/goarchive/task"
)

func LoadJson(file string) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Panic(err)
	}
	etljson := new(task.ETLJson)
	err = json.Unmarshal(data, etljson)
	if err != nil {
		log.Panic(err)
	}
	etl := task.RemapETL(etljson)
	task.RunETL(etl)
}
