package binance

import (
	"context"
	"database/sql"
	"fmt"

	binanceapi "github.com/haunt98/binance-api-go"
)

const (
	preparedInsertBTCUSDT_15m    = "InsertBTCUSDT_15m"
	preparedGetSingleBTCUSDT_15m = "GetSingleBTCUSDT_15m"

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
	stmtGetAllBTCUSDT_15m = `
SELECT open_time_ms, open, high, low, close, volume
FROM BTCUSDT_15m
ORDER BY open_time_ms ASC;
`
	stmtGetSingleBTCUSDT_15m = `
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
	GetAllBTCUSDT_15m(ctx context.Context) ([]binanceapi.Candlestick, error)
	GetSingleBTCUSDT_15m(ctx context.Context, openTimeMs int64) (binanceapi.Candlestick, error)
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
	preparedStmts[preparedGetSingleBTCUSDT_15m], err = db.PrepareContext(ctx, stmtGetSingleBTCUSDT_15m)
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

func (r *repo) GetAllBTCUSDT_15m(ctx context.Context) ([]binanceapi.Candlestick, error) {
	candlesticks := []binanceapi.Candlestick{}

	rows, err := r.db.QueryContext(ctx, stmtGetAllBTCUSDT_15m)
	if err != nil {
		return nil, fmt.Errorf("database failed to query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		candlestick := binanceapi.Candlestick{}
		if err := rows.Scan(
			&candlestick.OpenTimeMs,
			&candlestick.Open,
			&candlestick.High,
			&candlestick.Low,
			&candlestick.Close,
			&candlestick.Volume,
		); err != nil {
			return nil, fmt.Errorf("database failed to scan row: %w", err)
		}

		candlesticks = append(candlesticks, candlestick)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("database failed to scan rows: %w", err)
	}

	return candlesticks, nil
}

func (r *repo) GetSingleBTCUSDT_15m(ctx context.Context, openTimeMs int64) (binanceapi.Candlestick, error) {
	candlestick := binanceapi.Candlestick{}

	row := r.preparedStmts[preparedGetSingleBTCUSDT_15m].QueryRowContext(ctx, openTimeMs)
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
