package connection

import (
	"database/sql"
	"log"

	"jyv.com/goarchive/message"
	"jyv.com/goarchive/util"
)

type Connection struct {
	Name             string
	Driver           string
	ConnectionString string
}

func GetDB(con *Connection) (db *sql.DB, err error) {
	return sql.Open(con.Driver, con.ConnectionString)
}

func GetConnection(conlist []Connection, name string) *Connection {
	if name == "" {
		return nil
	}

	for _, c := range conlist {
		if c.Name == name {
			return &c
		}
	}
	// connection not found
	log.Fatalf(message.GetMessage("0010"), name)
	return nil
}

func ValidateConnectionUniqueNames(connections []Connection) {
	names := make([]string, 0)
	isFirst := true
	for i, c := range connections {
		if util.Contains(names, c.Name) && !isFirst {
			// already exist
			log.Fatalf(message.GetMessage("0011"), c.Name, i+1)
		} else {
			names = append(names, c.Name)
		}
		isFirst = false
	}
}

func ValidateConnection(con Connection, position int) {
	if con.Name == "" {
		log.Fatalf(message.GetMessage("0012"), position)
	}
	if con.Driver == "" {
		log.Fatalln(message.GetMessage("0013"), position)
	}
	if con.Driver != "sqlserver" {
		// driver not supported
		log.Printf(message.GetMessage("0014"), con.Driver, position)
		log.Fatalf(message.GetMessage("0015"))
	}
	if con.ConnectionString == "" {
		log.Fatalln(message.GetMessage("0016"), position)
	}
}
