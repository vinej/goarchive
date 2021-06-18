package util

import (
	"fmt"
	"log"
	"sync"

	"jyv.com/goarchive/message"
)

var mapanonyme map[string]map[string]string = make(map[string]map[string]string)
var mapcount map[string]int = make(map[string]int)
var lock = sync.RWMutex{}

func Anonymized(name string, columnName string, columnValue string) string {
	// lock needed, this routine is called by go routine when saving data
	//log.Printf(message.GetMessage(53), columnName, columnValue)
	lock.RLock()
	defer lock.RUnlock()

	fname := fmt.Sprintf("%s|||%s", name, columnName)
	icount, ok := mapcount[fname]
	if !ok {
		// initialize count to 1 to anonymized data for a name and ad column name
		mapcount[fname] = 1
		icount = 1
	}

	// get the map related to the name and column name
	anonymap, ok := mapanonyme[fname]
	if !ok {
		// create a new map, because the map does not exist
		mapanonyme[fname] = make(map[string]string)
		anonymap = mapanonyme[fname]
	}

	// find the anonymized value into the map for the name and column name
	anonymized_data, ok := anonymap[columnValue]
	if !ok {
		// if the anonymized value is not there, create a new one and put it into the map
		anonymap[columnValue] = fmt.Sprintf("%s_%d", columnName, icount)
		// increase the map counter for this name and column name
		mapcount[fname] = icount + 1
		anonymized_data = anonymap[columnValue]
	}

	log.Printf(message.GetMessage(54), columnName, columnValue, anonymized_data)
	return anonymized_data
}
