package main

import (
	"database/sql"
	"log"
	"time"

	"github.com/lib/pq"
)

var counts int64

// const dbTimeout = time.Second * 3

func openDB(dsn string) (*sql.DB, error) {
	pgUrl, err := pq.ParseURL(dsn)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("pgx", pgUrl)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (app *application) connectToSourceDB() (*sql.DB, error) {
	for {
		connection, err := openDB(app.SourceDSN)
		if err != nil {
			log.Println(err)
			log.Println("Source Postgres not yet ready ...")
			counts++
		} else {
			log.Println("Connected to Source Postgres!")
			return connection, nil
		}

		if counts > 10 {
			log.Println(err)
			return nil, err
		}

		log.Println("Backing off for two seconds....")
		time.Sleep(2 * time.Second)
		continue
	}
}

func (app *application) connectToTargetDB() (*sql.DB, error) {
	for {
		connection, err := openDB(app.TargetDSN)
		if err != nil {
			log.Println(err)
			log.Println("Target Postgres not yet ready ...")
			counts++
		} else {
			log.Println("Connected to Target Postgres!")
			return connection, nil
		}

		if counts > 10 {
			log.Println(err)
			return nil, err
		}

		log.Println("Backing off for two seconds....")
		time.Sleep(2 * time.Second)
		continue
	}
}
