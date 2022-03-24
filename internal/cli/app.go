package cli

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"time"

	binanceapi "github.com/haunt98/binance-api-go"
	"github.com/haunt98/binance-scraper/internal/binance"
	"github.com/make-go-great/color-go"
	"github.com/urfave/cli/v2"
)

const (
	Name  = "binance-scraper"
	usage = "get data from Binance"

	commandAdd      = "add"
	commandValidate = "validate"
	commandExport   = "export"

	usageAdd      = "add data"
	usageValidate = "validate data"
	usageExport   = "export data"
)

type App struct {
	cliApp *cli.App
}

func NewApp(db *sql.DB, shouldInitDatabase bool) (*App, error) {
	repo, err := binance.NewRepository(context.Background(), db, shouldInitDatabase)
	if err != nil {
		return nil, fmt.Errorf("failed to new repository: %w", err)
	}

	binanceAPIService := binanceapi.NewService(&http.Client{
		Timeout: time.Second * 5,
	})

	service := binance.NewService(binanceAPIService, repo)

	handler := binance.NewHandler(service)

	a := &action{
		handler: handler,
	}

	cliApp := &cli.App{
		Name:   Name,
		Usage:  usage,
		Action: a.RunHelp,
		Commands: []*cli.Command{
			{
				Name:   commandAdd,
				Usage:  usageAdd,
				Action: a.RunAdd,
			},
			{
				Name:   commandValidate,
				Usage:  usageValidate,
				Action: a.RunValidate,
			},
			{
				Name:   commandExport,
				Usage:  usageExport,
				Action: a.RunExport,
			},
		},
	}

	return &App{
		cliApp: cliApp,
	}, nil
}

func (a *App) Run() {
	if err := a.cliApp.Run(os.Args); err != nil {
		color.PrintAppError(Name, err.Error())
	}
}
