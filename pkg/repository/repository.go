package repository

import (
	"database/sql"
)

type DatabaseRepository interface {
	Connection() *sql.DB
	GetMaxLastStationData(args ...interface{}) (interface{}, error)
	GetStationData(args ...interface{}) (interface{}, error)
	InsertStationData(args ...interface{}) (interface{}, error)
}
