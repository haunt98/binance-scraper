package binance

import (
	"context"
	"fmt"

	"github.com/make-go-great/ioe-go"
	"github.com/spf13/cast"
)

type Handler interface {
	Add(ctx context.Context) error
	Validate(ctx context.Context) error
}

type handler struct {
	service Service
}

func NewHandler(service Service) Handler {
	return &handler{
		service: service,
	}
}

func (h *handler) Add(ctx context.Context) error {
	fmt.Printf("Input startTimeMs: ")
	startTimeMs := cast.ToInt64(ioe.ReadInput())

	fmt.Printf("Input endTimeMs: ")
	endTimeMs := cast.ToInt64(ioe.ReadInput())

	if err := h.service.AddMultiBTCUSDT_15m(ctx, startTimeMs, endTimeMs); err != nil {
		return err
	}

	return nil
}

func (h *handler) Validate(ctx context.Context) error {
	if err := h.service.ValidateBTCUSDT_15m(ctx); err != nil {
		return err
	}

	return nil
}
