package util

import (
	"fmt"
)

var mapanonyme map[string]map[string]string = make(map[string]map[string]string)
var mapcount map[string]int = make(map[string]int)

func Anonymized(name string, columnName string, columnValue string) string {
	fname := fmt.Sprintf("%s|||%s", name, columnName)
	icount, iok := mapcount[fname]
	if !iok {
		// initialize count to 1
		mapcount[fname] = 1
		icount = 1
	}

	cmap, cok := mapanonyme[fname]
	if !cok {
		// create the map for the column name
		mapanonyme[fname] = make(map[string]string)
		cmap = mapanonyme[fname]
	}

	cname, nok := cmap[columnValue]
	if !nok {
		cmap[columnValue] = fmt.Sprintf("%s_%d", columnName, icount)
		mapcount[fname] = icount + 1
		cname = cmap[columnValue]
	}
	return cname
}
