package binance

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/make-go-great/ioe-go"
	"github.com/spf13/cast"
)

const nowTime = "now"

type Handler interface {
	Add(ctx context.Context) error
	AddTillNow(ctx context.Context) error
	Validate(ctx context.Context) error
	Export(ctx context.Context) error
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

	fmt.Printf("Input endTimeMs, or now: ")
	endTimeMsStr := ioe.ReadInput()
	var endTimeMs int64
	if strings.EqualFold(endTimeMsStr, nowTime) {
		endTimeMs = time.Now().UnixMilli()
	} else {
		endTimeMs = cast.ToInt64(endTimeMsStr)
	}

	if err := h.service.AddMultiBTCUSDT_15m(ctx, startTimeMs, endTimeMs); err != nil {
		return err
	}

	return nil
}

func (h *handler) AddTillNow(ctx context.Context) error {
	if err := h.service.AddTillNowMultiBTCUSDT_15m(ctx); err != nil {
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

func (h *handler) Export(ctx context.Context) error {
	if err := h.service.ExportCSVBTCUSDT_15m(ctx, "btcusdt_15m.csv"); err != nil {
		return err
	}

	return nil
}
