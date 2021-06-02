package connection

/*
	this package encapsulate the DB connexion
*/

import (
	"database/sql"
	"errors"
	"log"
	"sync"
)

type Connection struct {
	Name             string
	Driver           string
	ConnectionString string
}

var lock = &sync.Mutex{}

// declare a empty map of connection
// go manage a connection pool, so you don<t need to close sql.DB
// all connection will be close when go will leave
var mapcon map[string]*sql.DB = make(map[string]*sql.DB)

func GetDB(name string) (db *sql.DB, err error) {
	db, found := mapcon[name]
	if found {
		return db, nil
	} else {
		return nil, errors.New("connection name does not exist")
	}
}

func CreateOrGetDB(name string, driverName string, dataSourceName string) (db *sql.DB, err error) {
	db, found := mapcon[name]
	if !found {
		lock.Lock()
		defer lock.Unlock()
		// check again to be sure that no goroutine gets here before us
		db, found = mapcon[name]
		if !found {
			log.Println("Connection: Creating Single Instance Now")
			db, err = sql.Open(driverName, dataSourceName)
			if err != nil {
				return nil, err
			} else {
				mapcon[name] = db
			}
		} else {
			log.Println("Connection: Single Instance already created-1")
		}
	} else {
		log.Println("Connection: Single Instance already created-2")
	}
	return mapcon[name], nil
}

func ValidateConnection(con Connection, position int) {
	if con.Name == "" {
		log.Fatalln("Connection Error: Json file for 'Connections' does not contains the field : <Name> ,check for a typo at position", position, "of <Connections> list")
	}
	if con.Driver == "" {
		log.Fatalln("Connection Error: Json file for 'Connections' does not contains the field : <Driver> ,check for a typo at position", position, "of <Connections> list")
	}
	if con.Driver != "sqlserver" {
		log.Printf("Connection Error: Json file for 'Connections' : The driver <%s> is not supported, check for a typo at position %d of <Connections> list", con.Driver, position)
		log.Fatalf("Connection Error: Json file for 'Connections' : Supported drivers are <sqlserver>")
	}
	if con.ConnectionString == "" {
		log.Fatalln("Connection Error: Json file for 'Connections' does not contains the field :  <ConnectionString>   ,check for a typo at position", position, "of <Connections> list")
	}
}

func CreateAll(con []Connection) {
	for _, c := range con {
		_, err := CreateOrGetDB(c.Name, c.Driver, c.ConnectionString)
		if err != nil {
			log.Fatal(err)
		}
	}
}
