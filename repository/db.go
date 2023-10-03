package repository

import (
	"database/sql"
	"fmt"
	"log"
)

const (
	DBHost     = "localhost"
	DBPort     = 5432
	DBUser     = "postgres"
	DBName     = "postgres"
	DBPassword = ""
)

func Connect() *sql.DB {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s sslmode=disable password=%s", DBHost, DBPort, DBName, DBUser, DBPassword)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
