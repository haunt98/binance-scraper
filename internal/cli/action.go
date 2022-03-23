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

func (a *action) RunAdd(c *cli.Context) error {
	return a.handler.Add(c.Context)
}

func (a *action) RunValidate(c *cli.Context) error {
	return a.handler.Validate(c.Context)
}
