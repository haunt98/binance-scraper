package binance

import (
	"context"
	"fmt"

	"github.com/make-go-great/ioe-go"
	"github.com/spf13/cast"
)

type Handler interface {
	AddMultiBTCUSDT_15m(ctx context.Context) error
}

type handler struct {
	service Service
}

func NewHandler(service Service) Handler {
	return &handler{
		service: service,
	}
}

func (h *handler) AddMultiBTCUSDT_15m(ctx context.Context) error {
	fmt.Printf("Input startTimeMs: ")
	startTimeMs := cast.ToInt64(ioe.ReadInput())

	fmt.Printf("Input endTimeMs: ")
	endTimeMs := cast.ToInt64(ioe.ReadInput())

	return h.service.AddMultiBTCUSDT_15m(ctx, startTimeMs, endTimeMs)
}
