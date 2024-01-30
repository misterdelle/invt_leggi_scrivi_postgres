package main

import (
	"log"
	"net/http"
)

func (app *application) DoAllHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("/DoAll started")

	var resp string
	w.Header().Set("Content-Type", "text/plain")

	app.doAll()

	resp = "OK"
	w.WriteHeader(http.StatusOK)

	w.Write([]byte(resp))

	log.Printf("/DoAll ended")
}
