package binance

import (
	"context"
	"database/sql"
	"fmt"

	binanceapi "github.com/haunt98/binance-api-go"
)

const (
	preparedInsertBTCUSDT_15m = "InsertBTCUSDT_15m"
	preparedGetBTCUSDT_15m    = "GetBTCUSDT_15m"

	stmtInitBTCUSDT_15m = `
CREATE TABLE BTCUSDT_15m
(
    open_time_ms INTEGER PRIMARY KEY,
    open      TEXT,
    high      TEXT,
    low       TEXT,
    close     TEXT,
    volume    TEXT
)
`
	stmtGetBTCUSDT_15m = `
SELECT open_time_ms, open, high, low, close, volume
FROM BTCUSDT_15m
WHERE open_time_ms = ?
`
	stmtInsertBTCUSDT_15m = `
INSERT INTO BTCUSDT_15m (open_time_ms, open, high, low, close, volume)
VALUES (?, ?, ?, ?, ?, ?)
`
)

type Repository interface {
	GetBTCUSDT_15m(ctx context.Context, openTimeMs int64) (binanceapi.Candlestick, error)
	InsertBTCUSDT_15m(ctx context.Context, candlestick binanceapi.Candlestick) error
}

type repo struct {
	db *sql.DB

	// Prepared statements
	// https://go.dev/doc/database/prepared-statements
	preparedStmts map[string]*sql.Stmt
}

func NewRepository(ctx context.Context, db *sql.DB, shouldInitDatabase bool) (Repository, error) {
	if shouldInitDatabase {
		if _, err := db.ExecContext(ctx, stmtInitBTCUSDT_15m); err != nil {
			return nil, fmt.Errorf("database failed to exec: %w", err)
		}
	}

	var err error
	preparedStmts := make(map[string]*sql.Stmt)
	preparedStmts[preparedGetBTCUSDT_15m], err = db.PrepareContext(ctx, stmtGetBTCUSDT_15m)
	if err != nil {
		return nil, fmt.Errorf("database failed to prepare context: %w", err)
	}

	preparedStmts[preparedInsertBTCUSDT_15m], err = db.PrepareContext(ctx, stmtInsertBTCUSDT_15m)
	if err != nil {
		return nil, fmt.Errorf("database failed to prepare context: %w", err)
	}

	return &repo{
		db:            db,
		preparedStmts: preparedStmts,
	}, nil
}

func (r *repo) GetBTCUSDT_15m(ctx context.Context, openTimeMs int64) (binanceapi.Candlestick, error) {
	candlestick := binanceapi.Candlestick{}

	row := r.preparedStmts[preparedGetBTCUSDT_15m].QueryRowContext(ctx, openTimeMs)
	if err := row.Scan(
		&candlestick.OpenTimeMs,
		&candlestick.Open,
		&candlestick.High,
		&candlestick.Low,
		&candlestick.Close,
		&candlestick.Volume,
	); err != nil {
		return binanceapi.Candlestick{}, fmt.Errorf("database failed to scan row: %w", err)
	}

	return candlestick, nil
}

func (r *repo) InsertBTCUSDT_15m(ctx context.Context, candlestick binanceapi.Candlestick) error {
	if _, err := r.preparedStmts[preparedInsertBTCUSDT_15m].ExecContext(ctx,
		candlestick.OpenTimeMs,
		candlestick.Open,
		candlestick.High,
		candlestick.Low,
		candlestick.Close,
		candlestick.Volume,
	); err != nil {
		return fmt.Errorf("database failed to exec: %w", err)
	}

	return nil
}
