package main

import (
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
	//message.FillInternalMessage()
	//message.WriteMessageToFile("./locales/en-US/out.gotext.json")
	//return
	//message.FillMessage("./locales/en-US/out.gotext.json")
	//return

	inifile := args.LoadParameterFromArg()
	if inifile != nil {
		if inifile.MessageFile != "" {
			message.FillMessage(inifile.MessageFile)
		} else {
			message.FillInternalMessage()
		}
		// setup log
		file, err := os.OpenFile(inifile.Log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			// Error opening log file
			log.Fatal(err, message.GetMessage(7))
		}
		defer file.Close()
		mw := io.MultiWriter(os.Stdout, file)
		log.SetOutput(mw)

		// start
		log.Println(message.GetMessage(8))
		doit(inifile)
		// end
		log.Println(message.GetMessage(9))
	}
}
