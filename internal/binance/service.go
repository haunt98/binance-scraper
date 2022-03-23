package binance

import (
	"context"
	"errors"
	"fmt"
	"time"

	binanceapi "github.com/haunt98/binance-api-go"
)

const (
	symbolBTCUSDT = "BTCUSDT"
	interval15m   = "15m"
	defaultLimit  = 1000

	// Binance can block if you spam request
	defaultSleep = time.Second
)

type Service interface {
	AddMultiBTCUSDT_15m(ctx context.Context, startTimeMs, endTimeMs int64) error
}

type service struct {
	binanceAPIService binanceapi.Service
	repo              Repository
}

func NewService(
	binanceAPIService binanceapi.Service,
	repo Repository,
) Service {
	return &service{
		binanceAPIService: binanceAPIService,
		repo:              repo,
	}
}

func (s *service) AddMultiBTCUSDT_15m(ctx context.Context, startTimeMs, endTimeMs int64) error {
	if startTimeMs > endTimeMs {
		return errors.New("invalid time")
	}

	duration15m, err := time.ParseDuration(interval15m)
	if err != nil {
		return fmt.Errorf("failed to parse duration: %w", err)
	}
	durationMs := duration15m.Milliseconds() * defaultLimit

	for startTimeMs < endTimeMs {
		currentEndTimeMs := startTimeMs + durationMs
		if currentEndTimeMs > endTimeMs {
			currentEndTimeMs = endTimeMs
		}

		getCandlestickRsp, err := s.binanceAPIService.GetCandlestick(ctx, binanceapi.GetCandlestickRequest{
			Symbol:      symbolBTCUSDT,
			Interval:    interval15m,
			StartTimeMs: startTimeMs,
			EndTimeMs:   currentEndTimeMs,
			Limit:       defaultLimit,
		})
		if err != nil {
			return fmt.Errorf("failed to get candlestick: %w", err)
		}

		for _, candlestick := range getCandlestickRsp.Candlesticks {
			if err := validateCandlestick(candlestick); err != nil {
				return err
			}

			// If exist, skip
			if _, err := s.repo.GetSingleBTCUSDT_15m(ctx, candlestick.OpenTimeMs); err == nil {
				continue
			}

			if err := s.repo.InsertBTCUSDT_15m(ctx, candlestick); err != nil {
				return fmt.Errorf("failed to insert BTCUSDT_15m: %w", err)
			}
		}

		startTimeMs += durationMs

		// TODO: better handle sleep
		time.Sleep(defaultSleep)
	}

	return nil
}

func validateCandlestick(candlestick binanceapi.Candlestick) error {
	if candlestick.OpenTimeMs == 0 {
		return errors.New("invalid open time")
	}

	if candlestick.Open == "" {
		return errors.New("invalid open")
	}

	if candlestick.High == "" {
		return errors.New("invalid high")
	}

	if candlestick.Low == "" {
		return errors.New("invalid low")
	}

	if candlestick.Close == "" {
		return errors.New("invalid close")
	}

	if candlestick.Volume == "" {
		return errors.New("invalid volume")
	}

	return nil
}
