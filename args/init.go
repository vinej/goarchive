package args

import (
	"log"
	"os"
	"strings"

	"gopkg.in/ini.v1"
	"jyv.com/goarchive/message"
)

type IniFile struct {
	Name             string
	Json             string
	Driver           string
	ConnectionString string
	Query            string
	Log              string
	Output           string
}

const INI_GOARCHIVE = "goarchive"
const INI_NAME = "name"
const INI_DRIVER = "driver"
const INI_CONNECTIONSTRING = "connectionstring"
const INI_QUERY = "query"
const INI_OUTPUT = "output"
const INI_LOG = "log"
const INI_DEFAULTNAME = "master"
const INI_DEFAULT_LOG = "goarchive.log"
const INI_DEFAULT_EXCEL = "goarchive.xlsx"

func load_ini_file(filename string) *IniFile {
	cfg, err := ini.Load(filename)
	if err != nil {
		// failed to read ini file
		log.Printf(message.GetMessage("0001"), filename)
		log.Fatal(err)
	}
	inifile := new(IniFile)
	inifile.Name = cfg.Section(INI_GOARCHIVE).Key(INI_NAME).String()
	inifile.Driver = cfg.Section(INI_GOARCHIVE).Key(INI_DRIVER).String()
	inifile.ConnectionString = cfg.Section(INI_GOARCHIVE).Key(INI_CONNECTIONSTRING).String()
	inifile.Query = cfg.Section(INI_GOARCHIVE).Key(INI_QUERY).String()
	inifile.Output = cfg.Section(INI_GOARCHIVE).Key(INI_OUTPUT).String()
	inifile.Log = cfg.Section(INI_GOARCHIVE).Key(INI_LOG).String()
	return inifile
}

func validate_inifile(inifile *IniFile) {
	if len(inifile.Name) == 0 {
		inifile.Name = INI_DEFAULTNAME
	}
	if len(inifile.Log) == 0 {
		inifile.Log = INI_DEFAULT_LOG
	}
	if len(inifile.Output) == 0 {
		inifile.Output = INI_DEFAULT_EXCEL
	}

	if inifile.Json == "" {
		if len(inifile.Driver) == 0 {
			// driver is mandatory
			log.Panic(message.GetMessage("0002"))
		}
		if len(inifile.ConnectionString) == 0 {
			// con is mandatory
			log.Panic(message.GetMessage("0003"))
		}
		if len(inifile.Query) == 0 {
			// query is mandatory
			log.Panic(message.GetMessage("0004"))
		}
	}
}

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
func LoadParameterFromArg() *IniFile {
	// if first parameter starts init -i or -ini, load ini file and forget about other parameter
	var inifile *IniFile
	if len(os.Args) > 1 {
		v := strings.SplitN(os.Args[1], "=", 2)
		if len(v) < 2 {
			// syntaxe error
			log.Panic(message.GetMessage("0005"))
		}
		argname := v[0]
		arginfo := v[1]
		if argname == "i" || argname == "--ini" {
			inifile = load_ini_file(arginfo)
		} else if argname == "j" || argname == "--json" {
			inifile = new(IniFile)
			inifile.Json = v[1]
		} else {
			inifile = new(IniFile)
			for i := 1; i < len(os.Args); i++ {
				v := strings.SplitN(os.Args[i], "=", 2)
				if len(v) < 2 {
					log.Panic(message.GetMessage("0005"))
				}
				argname := v[0]
				arginfo := v[1]

				switch argname {
				case "n":
					inifile.Name = arginfo
				case "--name":
					inifile.Name = arginfo
				case "d":
					inifile.Driver = arginfo
				case "--driver":
					inifile.Driver = arginfo
				case "o":
					inifile.Output = arginfo
				case "--output":
					inifile.Output = arginfo
				case "l":
					inifile.Log = arginfo
				case "--log":
					inifile.Log = arginfo
				case "q":
					inifile.Query = arginfo
				case "query":
					inifile.Query = arginfo
				case "c":
					inifile.ConnectionString = arginfo
				case "--con":
					inifile.ConnectionString = arginfo
				default:
					// unknown parameter
					log.Panicf(message.GetMessage("0006"), argname)
				}
			}
		}
	} else {
		// syntaxe error
		log.Panic(message.GetMessage("0005"))
	}
	validate_inifile(inifile)
	return inifile
}
