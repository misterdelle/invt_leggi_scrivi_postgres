package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/jackc/pgconn"
	_ "github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/joho/godotenv"
	"github.com/misterdelle/invt_leggi_scrivi_postgres/pkg/data"
	"github.com/misterdelle/invt_leggi_scrivi_postgres/pkg/db"
	"github.com/misterdelle/invt_leggi_scrivi_postgres/pkg/repository"
)

type application struct {
	SourceDSN  string
	TargetDSN  string
	SourceDB   repository.DatabaseRepository
	TargetDB   repository.DatabaseRepository
	WebPort    string
	StartTimer bool
}

var (
	app application
)

func init() {
	e := godotenv.Load()
	if e != nil {
		log.Println(e)
	}

	app.SourceDSN = os.Getenv("SOURCE_DSN")
	app.TargetDSN = os.Getenv("TARGET_DSN")
	app.StartTimer, _ = strconv.ParseBool(os.Getenv("START_TIMER"))
	app.WebPort = "8080"
}

func main() {
	connSourceRDBMS, err := app.connectToSourceDB()
	if err != nil {
		log.Fatal(fmt.Sprintf("Error %s", err))
	}
	defer connSourceRDBMS.Close()

	app.SourceDB = &db.PostgresDBRepo{DB: connSourceRDBMS}

	connTargetRDBMS, err := app.connectToTargetDB()
	if err != nil {
		log.Fatal(fmt.Sprintf("Error %s", err))
	}
	defer connTargetRDBMS.Close()

	app.TargetDB = &db.PostgresDBRepo{DB: connTargetRDBMS}

	ticker := time.NewTicker(1 * time.Minute)
	done := make(chan bool)

	if app.StartTimer {

		go func() {
			for {
				select {
				case <-done:
					return
				case t := <-ticker.C:
					log.Println(fmt.Sprintf("Tick at %s", t))
					app.doAll()

				}
			}
		}()
	}

	//
	// listen for web connections
	//
	app.serve()
}

func (app *application) serve() {
	//
	// start http server
	//
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", app.WebPort),
		Handler: app.routes(),
	}

	log.Println(fmt.Sprintf("The WEB server is listening on: 0.0.0.0:%s", app.WebPort))
	err := srv.ListenAndServe()
	if err != nil {
		panic(err)
	}
}

func (app *application) doAll() {
	lut, _ := RetryWithBackoff(app.TargetDB.GetMaxLastStationData, 5, 2*time.Second)
	lastUpdateTime := lut.(time.Time)
	log.Println(lastUpdateTime)

	sd, _ := RetryWithBackoff(app.SourceDB.GetStationData, 5, 2*time.Second, lastUpdateTime)
	stationData := sd.([]*data.Station)
	// log.Println(stationData)

	numRowsInserted := 0
	for v := range stationData {
		data := stationData[v]
		rc, _ := RetryWithBackoff(app.TargetDB.InsertStationData, 5, 2*time.Second, data)
		numRowsInserted += rc.(int)
	}
	log.Printf("Inserted %v rows\n", numRowsInserted)

}
