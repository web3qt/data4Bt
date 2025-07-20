package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
)

// Repository ClickHouse存储库
type Repository struct {
	conn   driver.Conn
	config config.ClickHouseConfig
	logger zerolog.Logger
}

// NewRepository 创建新的ClickHouse存储库
func NewRepository(cfg config.ClickHouseConfig) (*Repository, error) {
	// 构建连接选项
	options := &clickhouse.Options{
		Addr: cfg.Hosts,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     cfg.DialTimeout,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		Settings:        make(clickhouse.Settings),
	}
	
	// 设置压缩
	if cfg.Compression != "" && cfg.Compression != "none" {
		switch strings.ToLower(cfg.Compression) {
		case "lz4":
			options.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
		case "zstd":
			options.Compression = &clickhouse.Compression{Method: clickhouse.CompressionZSTD}
		}
	}
	
	// 设置数据库设置
	for key, value := range cfg.Settings {
		options.Settings[key] = value
	}
	
	// 建立连接
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	
	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}
	
	repository := &Repository{
		conn:   conn,
		config: cfg,
		logger: logger.GetLogger("clickhouse_repository"),
	}
	
	repository.logger.Info().
		Strs("hosts", cfg.Hosts).
		Str("database", cfg.Database).
		Msg("Connected to ClickHouse")
	
	return repository, nil
}

// Save 批量保存K线数据
func (r *Repository) Save(ctx context.Context, klines []domain.KLine) error {
	if len(klines) == 0 {
		return nil
	}
	
	start := time.Now()
	defer func() {
		logger.LogPerformance("clickhouse_repository", "save", time.Since(start), map[string]interface{}{
			"batch_size": len(klines),
			"symbol":     klines[0].Symbol,
		})
	}()
	
	// 准备批量插入
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO klines_1m (
			symbol, open_time, close_time, open_price, high_price, low_price, 
			close_price, volume, quote_asset_volume, number_of_trades, 
			taker_buy_base_volume, taker_buy_quote_volume, interval, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	
	// 添加数据到批次
	for _, kline := range klines {
		err := batch.Append(
			kline.Symbol,
			kline.OpenTime,
			kline.CloseTime,
			kline.OpenPrice,
			kline.HighPrice,
			kline.LowPrice,
			kline.ClosePrice,
			kline.Volume,
			kline.QuoteAssetVolume,
			kline.NumberOfTrades,
			kline.TakerBuyBaseVolume,
			kline.TakerBuyQuoteVolume,
			kline.Interval,
			kline.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}
	
	// 执行批量插入
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}
	
	r.logger.Debug().
		Int("count", len(klines)).
		Str("symbol", klines[0].Symbol).
		Msg("Batch inserted successfully")
	
	return nil
}

// GetLastDate 获取指定交易对的最后日期
func (r *Repository) GetLastDate(ctx context.Context, symbol string) (time.Time, error) {
	// 首先检查是否有数据
	var count uint64
	countQuery := `SELECT count(*) FROM klines_1m WHERE symbol = ?`
	row := r.conn.QueryRow(ctx, countQuery, symbol)
	if err := row.Scan(&count); err != nil {
		return time.Time{}, fmt.Errorf("failed to count records: %w", err)
	}
	
	// 如果没有数据，返回零值
	if count == 0 {
		return time.Time{}, nil
	}
	
	// 有数据时才查询最大日期
	var lastDate time.Time
	query := `
		SELECT max(toDate(open_time)) as last_date 
		FROM klines_1m 
		WHERE symbol = ?
	`
	
	row = r.conn.QueryRow(ctx, query, symbol)
	if err := row.Scan(&lastDate); err != nil {
		return time.Time{}, fmt.Errorf("failed to get last date: %w", err)
	}
	
	return lastDate, nil
}

// GetFirstDate 获取指定交易对的最早日期
func (r *Repository) GetFirstDate(ctx context.Context, symbol string) (time.Time, error) {
	var firstDate time.Time
	
	query := `
		SELECT min(toDate(open_time)) as first_date 
		FROM klines_1m 
		WHERE symbol = ?
	`
	
	row := r.conn.QueryRow(ctx, query, symbol)
	if err := row.Scan(&firstDate); err != nil {
		if err.Error() == "sql: no rows in result set" {
			// 没有数据，返回零值
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("failed to get first date: %w", err)
	}
	
	return firstDate, nil
}

// CreateTables 创建数据表
func (r *Repository) CreateTables(ctx context.Context) error {
	r.logger.Info().Msg("Creating ClickHouse tables")
	
	// 创建数据库（如果不存在）
	if err := r.createDatabase(ctx); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	
	// 创建1分钟K线表
	if err := r.createKlineTable(ctx); err != nil {
		return fmt.Errorf("failed to create kline table: %w", err)
	}
	
	r.logger.Info().Msg("Tables created successfully")
	return nil
}

// CreateMaterializedViews 创建物化视图
func (r *Repository) CreateMaterializedViews(ctx context.Context, intervals []string) error {
	r.logger.Info().Strs("intervals", intervals).Msg("Creating materialized views")
	
	for _, interval := range intervals {
		if err := r.createMaterializedView(ctx, interval); err != nil {
			return fmt.Errorf("failed to create materialized view for %s: %w", interval, err)
		}
	}
	
	r.logger.Info().Msg("Materialized views created successfully")
	return nil
}

// RefreshMaterializedViews 刷新物化视图
func (r *Repository) RefreshMaterializedViews(ctx context.Context) error {
	// ClickHouse的物化视图是自动更新的，这里可以执行一些优化操作
	intervals := []string{"5m", "15m", "1h", "4h", "1d"}
	
	for _, interval := range intervals {
		tableName := fmt.Sprintf("klines_%s", interval)
		
		// 优化表
		query := fmt.Sprintf("OPTIMIZE TABLE %s FINAL", tableName)
		if err := r.conn.Exec(ctx, query); err != nil {
			r.logger.Warn().Err(err).Str("table", tableName).Msg("Failed to optimize table")
		}
	}
	
	return nil
}

// ValidateData 验证数据完整性
func (r *Repository) ValidateData(ctx context.Context, symbol string, date time.Time) (*domain.ValidationResult, error) {
	dateStr := date.Format("2006-01-02")
	
	// 查询指定日期的数据统计
	query := `
		SELECT 
			count(*) as total_rows,
			countIf(open_price > 0 AND high_price > 0 AND low_price > 0 AND close_price > 0) as valid_price_rows,
			countIf(volume >= 0) as valid_volume_rows,
			countIf(open_time < close_time) as valid_time_rows
		FROM klines_1m 
		WHERE symbol = ? AND toDate(open_time) = ?
	`
	
	var totalRows, validPriceRows, validVolumeRows, validTimeRows uint64
	row := r.conn.QueryRow(ctx, query, symbol, dateStr)
	if err := row.Scan(&totalRows, &validPriceRows, &validVolumeRows, &validTimeRows); err != nil {
		return nil, fmt.Errorf("failed to validate data: %w", err)
	}
	
	// 计算有效行数（所有条件都满足）
	validRows := validPriceRows
	if validVolumeRows < validRows {
		validRows = validVolumeRows
	}
	if validTimeRows < validRows {
		validRows = validTimeRows
	}
	
	invalidRows := totalRows - validRows
	
	result := &domain.ValidationResult{
		Valid:       invalidRows == 0,
		TotalRows:   int(totalRows),
		ValidRows:   int(validRows),
		InvalidRows: int(invalidRows),
		Errors:      []string{},
		Warnings:    []string{},
	}
	
	// 添加警告信息
	if validPriceRows < totalRows {
		result.Warnings = append(result.Warnings, fmt.Sprintf("%d rows have invalid prices", totalRows-validPriceRows))
	}
	if validVolumeRows < totalRows {
		result.Warnings = append(result.Warnings, fmt.Sprintf("%d rows have invalid volume", totalRows-validVolumeRows))
	}
	if validTimeRows < totalRows {
		result.Warnings = append(result.Warnings, fmt.Sprintf("%d rows have invalid time", totalRows-validTimeRows))
	}
	
	// 检查数据完整性（1分钟数据应该有1440条记录）
	expectedRows := 1440 // 24 * 60
	if int(totalRows) < expectedRows {
		result.Warnings = append(result.Warnings, fmt.Sprintf("Expected %d rows but got %d", expectedRows, totalRows))
	}
	
	return result, nil
}

// ClearAllData 清空所有数据表
func (r *Repository) ClearAllData(ctx context.Context) error {
	r.logger.Info().Msg("Clearing all data from database")
	
	// 清空主表
	if err := r.conn.Exec(ctx, "TRUNCATE TABLE IF EXISTS klines_1m"); err != nil {
		r.logger.Error().Err(err).Msg("Failed to truncate klines_1m table")
		return fmt.Errorf("failed to truncate klines_1m: %w", err)
	}
	
	// 清空物化视图对应的表
	intervals := []string{"5m", "15m", "1h", "4h", "1d"}
	for _, interval := range intervals {
		tableName := fmt.Sprintf("klines_%s", interval)
		query := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", tableName)
		if err := r.conn.Exec(ctx, query); err != nil {
			r.logger.Warn().Err(err).Str("table", tableName).Msg("Failed to truncate table")
			// 继续处理其他表，不返回错误
		}
	}
	
	r.logger.Info().Msg("All data cleared successfully")
	return nil
}

// Close 关闭连接
func (r *Repository) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// createDatabase 创建数据库
func (r *Repository) createDatabase(ctx context.Context) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", r.config.Database)
	return r.conn.Exec(ctx, query)
}

// createKlineTable 创建K线表
func (r *Repository) createKlineTable(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS klines_1m (
			symbol String,
			open_time DateTime64(3),
			close_time DateTime64(3),
			open_price Float64,
			high_price Float64,
			low_price Float64,
			close_price Float64,
			volume Float64,
			quote_asset_volume Float64,
			number_of_trades Int64,
			taker_buy_base_volume Float64,
			taker_buy_quote_volume Float64,
			interval String,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		PARTITION BY (symbol, toYYYYMM(open_time))
		ORDER BY (symbol, open_time)
		SETTINGS index_granularity = 8192
	`
	
	return r.conn.Exec(ctx, query)
}

// createMaterializedView 创建物化视图
func (r *Repository) createMaterializedView(ctx context.Context, interval string) error {
	tableName := fmt.Sprintf("klines_%s", interval)
	viewName := fmt.Sprintf("klines_%s_mv", interval)
	
	// 首先创建目标表
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			symbol String,
			open_time DateTime64(3),
			close_time DateTime64(3),
			open_price Float64,
			high_price Float64,
			low_price Float64,
			close_price Float64,
			volume Float64,
			quote_asset_volume Float64,
			number_of_trades Int64,
			taker_buy_base_volume Float64,
			taker_buy_quote_volume Float64,
			interval String,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		PARTITION BY (symbol, toYYYYMM(open_time))
		ORDER BY (symbol, open_time)
		SETTINGS index_granularity = 8192
	`, tableName)
	
	if err := r.conn.Exec(ctx, createTableQuery); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}
	
	// 获取时间间隔的分钟数
	intervalMinutes := r.getIntervalMinutes(interval)
	
	// 创建物化视图
	createViewQuery := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS
		SELECT 
			symbol,
			toStartOfInterval(open_time, INTERVAL %d MINUTE) as open_time,
			toStartOfInterval(open_time, INTERVAL %d MINUTE) + INTERVAL %d MINUTE - INTERVAL 1 MILLISECOND as close_time,
			argMin(open_price, open_time) as open_price,
			max(high_price) as high_price,
			min(low_price) as low_price,
			argMax(close_price, open_time) as close_price,
			sum(volume) as volume,
			sum(quote_asset_volume) as quote_asset_volume,
			sum(number_of_trades) as number_of_trades,
			sum(taker_buy_base_volume) as taker_buy_base_volume,
			sum(taker_buy_quote_volume) as taker_buy_quote_volume,
			'%s' as interval,
			now() as created_at
		FROM klines_1m
		GROUP BY symbol, toStartOfInterval(open_time, INTERVAL %d MINUTE)
	`, viewName, tableName, intervalMinutes, intervalMinutes, intervalMinutes, interval, intervalMinutes)
	
	return r.conn.Exec(ctx, createViewQuery)
}

// getIntervalMinutes 获取时间间隔的分钟数
func (r *Repository) getIntervalMinutes(interval string) int {
	switch interval {
	case "5m":
		return 5
	case "15m":
		return 15
	case "1h":
		return 60
	case "4h":
		return 240
	case "1d":
		return 1440
	default:
		return 1
	}
}