package cli

import (
	"github.com/haunt98/binance-scraper/internal/binance"
	"github.com/urfave/cli/v2"
)

type action struct {
	handler binance.Handler
}

func (a *action) RunHelp(c *cli.Context) error {
	return cli.ShowAppHelp(c)
}

func (a *action) RunAddBTCUSDT_15m(c *cli.Context) error {
	return a.handler.AddMultiBTCUSDT_15m(c.Context)
}
