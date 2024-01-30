package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/misterdelle/invt_leggi_scrivi_postgres/pkg/data"
)

type PostgresDBRepo struct {
	DB *sql.DB
}

// GetMaxLastStationData() gets the last Station data from the database
func (m *PostgresDBRepo) GetMaxLastStationData(args ...interface{}) (interface{}, error) {
	var lastUpdateTime time.Time

	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	stmt := `select last_update_ts
	           from "Station".Station
			  where last_update_ts = (select max(last_update_ts) from "Station".Station)
			  limit 100`

	rows, err := m.DB.QueryContext(ctx, stmt)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error reading stationData: %s", rows.Err()))
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(
			&lastUpdateTime,
		)
		if err != nil {
			log.Println("Error scanning", err)
			return nil, err
		}
	}

	return lastUpdateTime, nil
}

// GetStationData() gets the Station data from the database starting from a timestamp
func (m *PostgresDBRepo) GetStationData(args ...interface{}) (interface{}, error) {
	lastUpdateTime := args[0].(time.Time)
	lut := lastUpdateTime.Format("2006-01-02 15:04:05")

	var rc []*data.Station

	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	stmt := `select last_update_ts, total_production, feed_in, battery_charge, self_used, total_consumption, power_purchased, battery_discharge, production, battery_soc
	           from "Station".Station
			  where last_update_ts > $1
			  order by last_update_ts`

	rows, err := m.DB.QueryContext(ctx, stmt, lut)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error reading stationData: %s", rows.Err()))
	}
	defer rows.Close()

	for rows.Next() {
		var stationData data.Station

		err := rows.Scan(
			&stationData.LastUpdateTime,
			&stationData.TotalProduction,
			&stationData.FeedIn,
			&stationData.BatteryCharge,
			&stationData.SelfUsed,
			&stationData.TotalConsumption,
			&stationData.PowerPurchased,
			&stationData.BatteryDischarge,
			&stationData.Production,
			&stationData.BatterySOC,
		)
		if err != nil {
			log.Println("Error scanning", err)
			return nil, err
		}

		rc = append(rc, &stationData)

	}

	return rc, nil
}

// InsertStationData inserts Station data into the database
func (m *PostgresDBRepo) InsertStationData(args ...interface{}) (interface{}, error) {
	stationData := args[0].(*data.Station)

	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	// TotalProduction  int
	// FeedIn           int
	// BatteryCharge    int
	// SelfUsed         int
	// TotalConsumption int
	// PowerPurchased   int
	// BatteryDischarge int
	// Production       int
	// BatterySOC       int

	stmt := `insert into "Station".Station (last_update_ts, total_production, feed_in, battery_charge, self_used, total_consumption, power_purchased, battery_discharge, production, battery_soc)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	row := m.DB.QueryRowContext(ctx, stmt,
		stationData.LastUpdateTime,
		stationData.TotalProduction,
		stationData.FeedIn,
		stationData.BatteryCharge,
		stationData.SelfUsed,
		stationData.TotalConsumption,
		stationData.PowerPurchased,
		stationData.BatteryDischarge,
		stationData.Production,
		stationData.BatterySOC,
	)

	if row.Err() != nil {
		return nil, errors.New(fmt.Sprintf("error inserting stationData: %s", row.Err()))
	}

	return 1, nil
}
