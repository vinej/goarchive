package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	"gopkg.in/ini.v1"
	"jyv.com/goarchive/connection"
	con "jyv.com/goarchive/connection"
	task "jyv.com/goarchive/task"
	util "jyv.com/goarchive/util"
)

type IniFile struct {
	name   string
	json   string
	driver string
	con    string
	query  string
	log    string
	output string
}

func load_ini_file(filename string) *IniFile {
	cfg, err := ini.Load(filename)
	if err != nil {
		log.Fatal(err, "Fail to read ini file: "+filename)
	}
	inifile := new(IniFile)
	inifile.name = cfg.Section("goarchive").Key("name").String()
	inifile.driver = cfg.Section("goarchive").Key("driver").String()
	inifile.con = cfg.Section("goarchive").Key("con").String()
	inifile.query = cfg.Section("goarchive").Key("query").String()
	inifile.output = cfg.Section("goarchive").Key("output").String()
	inifile.log = cfg.Section("goarchive").Key("log").String()
	return inifile
}

func validate_inifile(inifile *IniFile) {
	if len(inifile.name) == 0 {
		inifile.name = "master"
	}
	if len(inifile.log) == 0 {
		inifile.log = "goarchive.log"
	}
	if len(inifile.output) == 0 {
		inifile.output = "goarchive.xlsx"
	}

	if inifile.json == "" {
		if len(inifile.driver) == 0 {
			log.Panic("parameter <driver> is mandatory>")
		}
		if len(inifile.con) == 0 {
			log.Panic("parameter <con> is mandatory>")
		}
		if len(inifile.query) == 0 {
			log.Panic("parameter <query> is mandatory>")
		}
	}
}

func validate_json(etl *task.ETL) {
	for i, c := range etl.Connections {
		con.ValidateConnection(c, i+1)
	}
	for i, t := range etl.Tasks {
		task.ValidateTask(t, i+1)
		for j, p := range t.Parameters {
			task.ValidateParameter(p, j+1, i+1)
		}
	}
}

func load_json(file string) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Panic(err)
	}
	obj := new(task.ETL)
	err = json.Unmarshal(data, obj)
	if err != nil {
		log.Panic(err)
	}
	validate_json(obj)
	con.CreateAll(obj.Connections)
	task.RunAll(obj.Tasks)
}

func load_parameter() *IniFile {
	// if first parameter starts init -i or -ini, load ini file and forget about other parameter
	var inifile *IniFile
	if len(os.Args) > 1 {
		v := strings.SplitN(os.Args[1], "=", 2)
		if len(v) < 2 {
			log.Panic("Syntaxe error")
		}
		argname := v[0]
		arginfo := v[1]
		if argname == "i" || argname == "--ini" {
			inifile = load_ini_file(arginfo)
		} else if argname == "j" || argname == "--json" {
			inifile = new(IniFile)
			inifile.json = v[1]
		} else {
			inifile = new(IniFile)
			for i := 1; i < len(os.Args); i++ {
				v := strings.SplitN(os.Args[i], "=", 2)
				if len(v) < 2 {
					log.Panic("Syntaxe error")
				}
				argname := v[0]
				arginfo := v[1]

				switch argname {
				case "n":
					inifile.name = arginfo
				case "--name":
					inifile.name = arginfo
				case "d":
					inifile.driver = arginfo
				case "--driver":
					inifile.driver = arginfo
				case "o":
					inifile.output = arginfo
				case "--output":
					inifile.output = arginfo
				case "l":
					inifile.log = arginfo
				case "--log":
					inifile.log = arginfo
				case "q":
					inifile.query = arginfo
				case "query":
					inifile.query = arginfo
				case "c":
					inifile.con = arginfo
				case "--con":
					inifile.con = arginfo
				default:
					log.Panic("unknown parameter")
				}
			}
		}
	} else {
		log.Panic("Syntaxe error")
	}
	validate_inifile(inifile)
	return inifile
}

func doit(inifile *IniFile) {
	if inifile.json != "" {
		load_json(inifile.json)
	} else {
		db, _ := connection.CreateOrGetDB(inifile.name, inifile.driver, inifile.con)
		util.QuerySaveExcel(inifile.name, db, inifile.query, inifile.output)
	}
}

func main() {
	//syntaxe
	// d, --driver d={sql driver}
	// c, --con    c={connection string}
	// q, --query  q={sql query}
	// l, --log    l={log file}
	// o, --output o={output file name
	// i, --ini    i={initialisation file}
	//     [goarchive]
	// 		driver = {sql driver}
	//      con = { connection string }
	// 		query = {sql query}
	// 		log = {log file}
	// 		output = {output file name
	inifile := load_parameter()
	if inifile != nil {
		file, err := os.OpenFile(inifile.log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err, "Error opening log file")
		}
		defer file.Close()
		mw := io.MultiWriter(os.Stdout, file)
		log.SetOutput(mw)

		log.Println("START processing")
		doit(inifile)
		log.Println("END processing")
	}
}
