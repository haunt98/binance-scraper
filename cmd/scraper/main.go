package main

import (
	"database/sql"
	"log"
	"path/filepath"

	"github.com/haunt98/binance-scraper/internal/cli"
	_ "github.com/mattn/go-sqlite3"
)

const dataFilename = "data.sqlite3"

func main() {
	db, err := sql.Open("sqlite3", getDataFilePath())
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	app, err := cli.NewApp(db)
	if err != nil {
		log.Fatalln(err)
	}

	app.Run()
}

// Should be ./data.sqlite3
func getDataFilePath() string {
	return filepath.Join(".", dataFilename)
}
