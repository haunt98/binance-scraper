package binance

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	binanceapi "github.com/haunt98/binance-api-go"
)

const (
	symbolBTCUSDT = "BTCUSDT"
	interval15m   = "15m"
	defaultLimit  = 1000

	// Binance can block if you spam request
	defaultSleep = time.Second

	// Custom
	timeLayout = "2006-01-02 15:04:05"
)

type Service interface {
	AddMultiBTCUSDT_15m(ctx context.Context, startTimeMs, endTimeMs int64) error
	ValidateBTCUSDT_15m(ctx context.Context) error
	ExportCSVBTCUSDT_15m(ctx context.Context, filename string) error
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
			return fmt.Errorf("failed to get single candlestick: %w", err)
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

func (s *service) ValidateBTCUSDT_15m(ctx context.Context) error {
	candlesticks, err := s.repo.GetAllBTCUSDT_15m(ctx)
	if err != nil {
		return fmt.Errorf("failed to get all BTCUSDT_15m: %w", err)
	}

	duration15m, err := time.ParseDuration(interval15m)
	if err != nil {
		return fmt.Errorf("failed to parse duration: %w", err)
	}
	duration15mInMs := duration15m.Milliseconds()

	for i := 0; i < len(candlesticks)-1; i++ {
		if candlesticks[i].OpenTimeMs+duration15mInMs != candlesticks[i+1].OpenTimeMs {
			log.Printf("ValidateBTCUSDT_15m: exist open time %d but missing open time %d", candlesticks[i].OpenTimeMs, candlesticks[i].OpenTimeMs+duration15mInMs)
		}
	}

	return nil
}

func (s *service) ExportCSVBTCUSDT_15m(ctx context.Context, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	csvWriter := csv.NewWriter(file)

	if err := csvWriter.Write([]string{"open_time", "open", "high", "low", "close", "volume"}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	candlesticks, err := s.repo.GetAllBTCUSDT_15m(ctx)
	if err != nil {
		return fmt.Errorf("failed to get all BTCUSDT_15m: %w", err)
	}

	for _, candlestick := range candlesticks {
		if err := csvWriter.Write([]string{
			timeUTCFromMillisecond(candlestick.OpenTimeMs).Format(timeLayout),
			candlestick.Open,
			candlestick.High,
			candlestick.Low,
			candlestick.Close,
			candlestick.Volume,
		}); err != nil {
			return fmt.Errorf("failed to write candlestick: %w", err)
		}
	}

	csvWriter.Flush()
	file.Close()

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

func timeUTCFromMillisecond(ms int64) time.Time {
	return time.Unix(0, ms*int64(time.Millisecond)).UTC()
}
