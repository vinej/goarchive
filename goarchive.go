package main

import (
	"fmt"
	"io"
	"log"
	"os"

	args "jyv.com/goarchive/args"
	message "jyv.com/goarchive/message"
)

func doit(inifile *args.IniFile) {
	if inifile.Json != "" {
		args.LoadJson(inifile.Json)
	} else {
		args.Runquery(inifile)
	}
}

func main() {
	message.FillMessage("./locales/en-US/out.gotext.json")
	fmt.Println(message.GetMessage("0001"))

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
