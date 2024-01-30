package main

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func (app *application) routes() http.Handler {
	// create router
	mux := chi.NewRouter()

	// set up middleware
	mux.Use(middleware.Recoverer)

	// define application routes
	mux.Get("/", app.DoAllHandler)

	return mux
}
