package connection

/*
	this package encapsulate the DB connexion
*/

import (
	"database/sql"
	"log"

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
	log.Fatalln("Connection <" + name + "> not found")
	return nil
}

func ValidateConnectionUniqueNames(connections []Connection) {
	names := make([]string, 0)
	isFirst := true
	for i, c := range connections {
		if util.Contains(names, c.Name) && !isFirst {
			log.Fatalln("Connection error in the json file: the <Connection:", c.Name, "> of <Connection:", i+1, "> already exists")
		} else {
			names = append(names, c.Name)
		}
		isFirst = false
	}
}

func ValidateConnection(con Connection, position int) {
	if con.Name == "" {
		log.Fatalln("Connection Error in the json file: <Connections #", position, "> does not contains the field : <Name>")
	}
	if con.Driver == "" {
		log.Fatalln("Connection Error in the json file: <Connections #", position, "> does not contains the field : <Driver>")
	}
	if con.Driver != "sqlserver" {
		log.Printf("Connection Error in the json file: <Connections #%d>, the driver <%s> is not supported", position, con.Driver)
		log.Fatalf("Connection Error in the json file: <Connections #%d>, supported drivers are <sqlserver>", position)
	}
	if con.ConnectionString == "" {
		log.Fatalln("Connection Error in the json file: <Connections #", position, "> does not contains the field : <ConnectionString>")
	}
}
