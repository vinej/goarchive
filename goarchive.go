package main

import (
	"io"
	"log"
	"os"

	args "jyv.com/goarchive/args"
)

func doit(inifile *args.IniFile) {
	if inifile.Json != "" {
		args.LoadJson(inifile.Json)
	} else {
		args.Runquery(inifile)
	}
}

func main() {
	inifile := args.LoadParameterFromArg()
	if inifile != nil {
		// setup log
		file, err := os.OpenFile(inifile.Log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err, "Error opening log file")
		}
		defer file.Close()
		mw := io.MultiWriter(os.Stdout, file)
		log.SetOutput(mw)

		// do it
		log.Println("START processing")
		doit(inifile)
		log.Println("END processing")
	}
}
