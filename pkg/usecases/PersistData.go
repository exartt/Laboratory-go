package usecases

import (
	"Laboratory-go/pkg/entities"
	"Laboratory-go/repository"
	"database/sql"
	"errors"
	"log"
)

const (
	InsertSql = "INSERT INTO g_data" +
		"(m_thread," +
		"m_memory," +
		"m_execution_r," +
		"m_execution_w," +
		"m_speed_up," +
		"m_efficiency," +
		"m_execution_time," +
		"m_overhead," +
		"m_iddle_thread," +
		"m_full_execution_time," +
		"m_is_valid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
	InsertSqlOt       = "INSERT INTO g_data_ot (m_thread, m_memory, m_execution_r, m_execution_w, m_execution_time, m_iddle_thread, m_full_execution_time, m_is_valid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
	InsertRecordParam = "INSERT INTO record_params (r_sequential_time, r_max_threads, r_lang) VALUES ($1, $2, $3)"
)

func Insert(data entities.DataCollected) {
	db := repository.Connect()
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	var sqlStr string
	if data.IsSingleThread {
		sqlStr = InsertSqlOt
		_, err := db.Exec(sqlStr, UsedThread, data.Memory, data.ExeR, data.ExeW, data.ExecutionTime, data.IdleThreadTimeMedian, data.FullExecutionTime, data.IsValid)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		sqlStr = InsertSql
		_, err := db.Exec(sqlStr, UsedThread, data.Memory, data.ExeR, data.ExeW, data.Speedup, data.Efficiency, data.ExecutionTime, data.OverHead, data.IdleThreadTimeMedian, data.FullExecutionTime, data.IsValid)
		if err != nil {
			log.Fatal(err)
		}
	}

}

func GetAverageExecutionTime() float64 {
	db := repository.Connect()
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	var average float64
	row := db.QueryRow("SELECT AVG(m_execution_time) FROM g_data_ot")
	err := row.Scan(&average)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.Fatal(err)
	}
	return average
}

func InsertData(sequentialTime float64, usedThread int) {
	db := repository.Connect()
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	_, err := db.Exec(InsertRecordParam, sequentialTime, usedThread, 3)
	if err != nil {
		log.Fatal(err)
	}
}

func DeleteFromGData() {
	db := repository.Connect()
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	_, err := db.Exec("DELETE FROM g_data")
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.Fatal(err)
	}
}

func DeleteFromGDataOT() {
	db := repository.Connect()
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	_, err := db.Exec("DELETE FROM g_data_ot")
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.Fatal(err)
	}
}
