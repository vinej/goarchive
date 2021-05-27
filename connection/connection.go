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
			log.Println("Creting Single Instance Now")
			db, err = sql.Open(driverName, dataSourceName)
			if err != nil {
				//	con.db.SetConnMaxIdleTime(time.Duration(maxIdleTime))
				//	con.db.SetMaxIdleConns(maxIdleConns)
				//	con.db.SetMaxOpenConns(maxOpenConns)
				log.Println(err)
				return nil, err
			} else {
				mapcon[name] = db
			}
		} else {
			log.Println("Single Instance already created-1")
		}
	} else {
		log.Println("Single Instance already created-2")
	}
	return mapcon[name], nil
}
