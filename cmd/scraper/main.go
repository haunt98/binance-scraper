package main

import (
	"database/sql"
	"errors"
	"log"
	"os"
	"path/filepath"

	"github.com/haunt98/binance-scraper/internal/cli"
	_ "github.com/mattn/go-sqlite3"
)

const dataFilename = "data.sqlite3"

func main() {
	shouldInitDatabase := false
	if _, err := os.Stat(getDataFilePath()); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			shouldInitDatabase = true
		} else {
			log.Fatalln(err)
		}
	}

	db, err := sql.Open("sqlite3", getDataFilePath())
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	app, err := cli.NewApp(db, shouldInitDatabase)
	if err != nil {
		log.Fatalln(err)
	}

	app.Run()
}

// Should be ./data.sqlite3
func getDataFilePath() string {
	return filepath.Join(".", dataFilename)
}
