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

// GetBatchDateRanges 批量获取多个交易对的时间范围
// 这个方法优化了性能，用单个查询替代多个单独的GetFirstDate和GetLastDate调用
func (r *Repository) GetBatchDateRanges(ctx context.Context, symbols []string) (map[string]*domain.SymbolDateRange, error) {
	if len(symbols) == 0 {
		return make(map[string]*domain.SymbolDateRange), nil
	}
	
	// 创建结果映射
	results := make(map[string]*domain.SymbolDateRange)
	
	// 初始化所有交易对的结果，默认为无数据
	for _, symbol := range symbols {
		results[symbol] = &domain.SymbolDateRange{
			Symbol:  symbol,
			HasData: false,
		}
	}
	
	// 构建批量查询SQL
	// 使用单个查询获取所有交易对的时间范围
	query := `
		SELECT 
			symbol,
			min(toDate(open_time)) as first_date,
			max(toDate(open_time)) as last_date,
			count(*) as record_count
		FROM klines_1m 
		WHERE symbol IN (` + r.buildInClause(len(symbols)) + `)
		GROUP BY symbol
		HAVING record_count > 0
	`
	
	// 准备查询参数
	args := make([]interface{}, len(symbols))
	for i, symbol := range symbols {
		args[i] = symbol
	}
	
	// 执行查询
	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute batch date ranges query: %w", err)
	}
	defer rows.Close()
	
	// 处理查询结果
	for rows.Next() {
		var symbol string
		var firstDate, lastDate time.Time
		var recordCount uint64
		
		if err := rows.Scan(&symbol, &firstDate, &lastDate, &recordCount); err != nil {
			r.logger.Warn().Err(err).Str("symbol", symbol).Msg("Failed to scan date range row")
			continue
		}
		
		// 更新结果
		if result, exists := results[symbol]; exists {
			result.FirstDate = firstDate
			result.LastDate = lastDate
			result.HasData = recordCount > 0
		}
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over date range results: %w", err)
	}
	
	r.logger.Info().
		Int("requested_symbols", len(symbols)).
		Int("symbols_with_data", r.countSymbolsWithData(results)).
		Msg("Batch date ranges query completed")
	
	return results, nil
}

// buildInClause 构建IN子句的占位符
func (r *Repository) buildInClause(count int) string {
	if count == 0 {
		return ""
	}
	
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "?"
	}
	
	return strings.Join(placeholders, ",")
}

// countSymbolsWithData 计算有数据的交易对数量
func (r *Repository) countSymbolsWithData(results map[string]*domain.SymbolDateRange) int {
	count := 0
	for _, result := range results {
		if result.HasData {
			count++
		}
	}
	return count
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
	
	// 创建交易对信息表
	if err := r.createSymbolInfoTable(ctx); err != nil {
		return fmt.Errorf("failed to create symbol info table: %w", err)
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

// createSymbolInfoTable 创建交易对信息表
func (r *Repository) createSymbolInfoTable(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS symbol_infos (
			symbol String,
			status String,
			base_asset String,
			quote_asset String,
			earliest_date Date,
			latest_date Date,
			total_months Int32,
			data_status String,
			created_at DateTime DEFAULT now(),
			updated_at DateTime DEFAULT now()
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY symbol
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
	
	// 创建物化视图 - 修复GROUP BY问题
	createViewQuery := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS
		SELECT 
			symbol,
			interval_start as open_time,
			(interval_start + toIntervalMinute(%d) - toIntervalMillisecond(1)) as close_time,
			any(open_price) as open_price,
			max(high_price) as high_price,
			min(low_price) as low_price,
			anyLast(close_price) as close_price,
			sum(volume) as volume,
			sum(quote_asset_volume) as quote_asset_volume,
			sum(number_of_trades) as number_of_trades,
			sum(taker_buy_base_volume) as taker_buy_base_volume,
			sum(taker_buy_quote_volume) as taker_buy_quote_volume,
			'%s' as interval,
			now() as created_at
		FROM klines_1m
		GROUP BY symbol, toStartOfInterval(open_time, toIntervalMinute(%d)) as interval_start
	`, viewName, tableName, intervalMinutes, interval, intervalMinutes)
	
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

// SaveSymbolInfo 保存交易对信息 (使用UPSERT逻辑)
func (r *Repository) SaveSymbolInfo(ctx context.Context, symbolInfo *domain.SymbolInfo) error {
	// 提取基础资产和报价资产
	baseAsset := symbolInfo.BaseAsset
	quoteAsset := symbolInfo.QuoteAsset
	if baseAsset == "" || quoteAsset == "" {
		// 从symbol中提取，假设USDT结尾
		if strings.HasSuffix(symbolInfo.Symbol, "USDT") {
			baseAsset = strings.TrimSuffix(symbolInfo.Symbol, "USDT")
			quoteAsset = "USDT"
		} else {
			baseAsset = symbolInfo.Symbol
			quoteAsset = "UNKNOWN"
		}
	}
	
	// 检查记录是否已存在
	existingInfo, err := r.GetSymbolInfo(ctx, symbolInfo.Symbol)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("failed to check existing symbol info: %w", err)
	}
	
	if existingInfo != nil {
		// 更新现有记录
		updateQuery := `
			ALTER TABLE symbol_infos UPDATE 
				status = ?,
				base_asset = ?,
				quote_asset = ?,
				earliest_date = ?,
				latest_date = ?,
				total_months = ?,
				data_status = ?,
				updated_at = now()
			WHERE symbol = ?
		`
		return r.conn.Exec(ctx, updateQuery,
			symbolInfo.Status,
			baseAsset,
			quoteAsset,
			symbolInfo.EarliestDate,
			symbolInfo.LatestDate,
			symbolInfo.TotalMonths,
			symbolInfo.DataStatus,
			symbolInfo.Symbol,
		)
	} else {
		// 插入新记录
		insertQuery := `
			INSERT INTO symbol_infos 
			(symbol, status, base_asset, quote_asset, earliest_date, latest_date, total_months, data_status, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, now(), now())
		`
		return r.conn.Exec(ctx, insertQuery,
			symbolInfo.Symbol,
			symbolInfo.Status,
			baseAsset,
			quoteAsset,
			symbolInfo.EarliestDate,
			symbolInfo.LatestDate,
			symbolInfo.TotalMonths,
			symbolInfo.DataStatus,
		)
	}
}

// GetSymbolInfo 获取交易对信息
func (r *Repository) GetSymbolInfo(ctx context.Context, symbol string) (*domain.SymbolInfo, error) {
	query := `
		SELECT symbol, status, base_asset, quote_asset, earliest_date, latest_date, 
		       total_months, data_status, created_at, updated_at
		FROM symbol_infos
		WHERE symbol = ?
	`
	
	var info domain.SymbolInfo
	row := r.conn.QueryRow(ctx, query, symbol)
	
	err := row.Scan(
		&info.Symbol,
		&info.Status,
		&info.BaseAsset,
		&info.QuoteAsset,
		&info.EarliestDate,
		&info.LatestDate,
		&info.TotalMonths,
		&info.DataStatus,
		&info.CreatedAt,
		&info.UpdatedAt,
	)
	
	if err != nil {
		return nil, err
	}
	
	return &info, nil
}

// GetMonthlyDataStats 获取月度数据统计
func (r *Repository) GetMonthlyDataStats(ctx context.Context, symbol string, month time.Time) (int64, time.Time, time.Time, error) {
	// 计算月份的开始和结束时间
	startOfMonth := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	endOfMonth := startOfMonth.AddDate(0, 1, 0).Add(-time.Second)
	
	// 查询该月份的数据统计
	query := `
		SELECT 
			count(*) as total_records,
			min(open_time) as first_record,
			max(open_time) as last_record
		FROM klines_1m 
		WHERE symbol = ? 
		AND open_time >= ? 
		AND open_time <= ?
	`
	
	var totalRecordsUint uint64
	var firstRecord, lastRecord time.Time
	
	row := r.conn.QueryRow(ctx, query, symbol, startOfMonth, endOfMonth)
	err := row.Scan(&totalRecordsUint, &firstRecord, &lastRecord)
	if err != nil {
		return 0, time.Time{}, time.Time{}, fmt.Errorf("failed to get monthly data stats: %w", err)
	}
	
	return int64(totalRecordsUint), firstRecord, lastRecord, nil
}

// CheckMonthlyDataExistence 检查月度数据存在性
func (r *Repository) CheckMonthlyDataExistence(ctx context.Context, symbol string, months []string) (map[string]bool, error) {
	if len(months) == 0 {
		return make(map[string]bool), nil
	}
	
	// 构建查询条件
	var conditions []string
	var args []interface{}
	
	args = append(args, symbol)
	
	for _, month := range months {
		monthTime, err := time.Parse("2006-01", month)
		if err != nil {
			continue
		}
		
		startOfMonth := monthTime
		endOfMonth := startOfMonth.AddDate(0, 1, 0).Add(-time.Second)
		
		conditions = append(conditions, "(open_time >= ? AND open_time <= ?)")
		args = append(args, startOfMonth, endOfMonth)
	}
	
	if len(conditions) == 0 {
		return make(map[string]bool), nil
	}
	
	query := fmt.Sprintf(`
		SELECT 
			formatDateTime(toStartOfMonth(open_time), '%%Y-%%m') as month,
			count(*) > 0 as has_data
		FROM klines_1m 
		WHERE symbol = ? 
		AND (%s)
		GROUP BY toStartOfMonth(open_time)
		ORDER BY month
	`, strings.Join(conditions, " OR "))
	
	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to check monthly data existence: %w", err)
	}
	defer rows.Close()
	
	result := make(map[string]bool)
	
	// 初始化所有月份为false
	for _, month := range months {
		result[month] = false
	}
	
	// 设置有数据的月份为true
	for rows.Next() {
		var month string
		var hasData bool
		
		if err := rows.Scan(&month, &hasData); err != nil {
			return nil, fmt.Errorf("failed to scan month existence: %w", err)
		}
		
		result[month] = hasData
	}
	
	return result, rows.Err()
}

// GetDataCompletenessForSymbol 获取交易对数据完整性统计
func (r *Repository) GetDataCompletenessForSymbol(ctx context.Context, symbol string, startMonth, endMonth string) (*domain.DataCompletenessStats, error) {
	startTime, err := time.Parse("2006-01", startMonth)
	if err != nil {
		return nil, fmt.Errorf("invalid start month format: %w", err)
	}
	
	endTime, err := time.Parse("2006-01", endMonth)
	if err != nil {
		return nil, fmt.Errorf("invalid end month format: %w", err)
	}
	
	// 确保开始时间在结束时间之前
	if startTime.After(endTime) {
		return nil, fmt.Errorf("start month cannot be after end month")
	}
	
	// 查询每月的数据统计
	query := `
		SELECT 
			formatDateTime(toStartOfMonth(open_time), '%Y-%m') as month,
			count(*) as actual_records,
			min(open_time) as first_record,
			max(open_time) as last_record
		FROM klines_1m 
		WHERE symbol = ? 
		AND open_time >= ? 
		AND open_time < ?
		GROUP BY toStartOfMonth(open_time)
		ORDER BY month
	`
	
	endTimeLimit := endTime.AddDate(0, 1, 0) // 添加一个月作为上限
	
	rows, err := r.conn.Query(ctx, query, symbol, startTime, endTimeLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to query data completeness: %w", err)
	}
	defer rows.Close()
	
	monthlyStats := make(map[string]*domain.MonthlyStats)
	var totalActualRecords int64
	var firstRecord, lastRecord time.Time
	
	for rows.Next() {
		var month string
		var actualRecordsUint uint64
		var monthFirstRecord, monthLastRecord time.Time
		
		if err := rows.Scan(&month, &actualRecordsUint, &monthFirstRecord, &monthLastRecord); err != nil {
			return nil, fmt.Errorf("failed to scan completeness data: %w", err)
		}
		
		// 计算该月的预期记录数
		expectedRecords := r.calculateExpectedRecordsForMonth(month)
		actualRecords := int64(actualRecordsUint)
		completenessRatio := 0.0
		if expectedRecords > 0 {
			completenessRatio = float64(actualRecords) / float64(expectedRecords) * 100
		}
		
		monthlyStats[month] = &domain.MonthlyStats{
			Month:           month,
			ExpectedRecords: expectedRecords,
			ActualRecords:   actualRecords,
			CompletenessRatio: completenessRatio,
			FirstRecord:     monthFirstRecord,
			LastRecord:      monthLastRecord,
			HasData:         actualRecords > 0,
		}
		
		totalActualRecords += actualRecords
		
		if firstRecord.IsZero() || monthFirstRecord.Before(firstRecord) {
			firstRecord = monthFirstRecord
		}
		if monthLastRecord.After(lastRecord) {
			lastRecord = monthLastRecord
		}
	}
	
	// 计算总的预期记录数
	var totalExpectedRecords int64
	currentMonth := startTime
	for currentMonth.Before(endTime) || currentMonth.Equal(endTime) {
		monthStr := currentMonth.Format("2006-01")
		expectedRecords := r.calculateExpectedRecordsForMonth(monthStr)
		totalExpectedRecords += expectedRecords
		
		// 如果没有数据，也要添加到统计中
		if _, exists := monthlyStats[monthStr]; !exists {
			monthlyStats[monthStr] = &domain.MonthlyStats{
				Month:           monthStr,
				ExpectedRecords: expectedRecords,
				ActualRecords:   0,
				CompletenessRatio: 0.0,
				HasData:         false,
			}
		}
		
		currentMonth = currentMonth.AddDate(0, 1, 0)
	}
	
	// 计算整体完整性比率
	completenessRatio := 0.0
	if totalExpectedRecords > 0 {
		completenessRatio = float64(totalActualRecords) / float64(totalExpectedRecords) * 100
	}
	
	stats := &domain.DataCompletenessStats{
		Symbol:               symbol,
		TotalExpectedRecords: totalExpectedRecords,
		TotalActualRecords:   totalActualRecords,
		CompletenessRatio:    completenessRatio,
		MonthlyStats:         monthlyStats,
		FirstRecord:          firstRecord,
		LastRecord:           lastRecord,
	}
	
	return stats, rows.Err()
}

// calculateExpectedRecordsForMonth 计算指定月份的预期记录数
func (r *Repository) calculateExpectedRecordsForMonth(month string) int64 {
	monthTime, err := time.Parse("2006-01", month)
	if err != nil {
		return 40320 // 默认值：28天 * 1440分钟
	}
	
	// 计算该月的天数
	year := monthTime.Year()
	monthNum := monthTime.Month()
	
	// 获取下一个月的第一天，然后减去一天得到当前月的最后一天
	nextMonth := time.Date(year, monthNum+1, 1, 0, 0, 0, 0, time.UTC)
	lastDay := nextMonth.Add(-24 * time.Hour).Day()
	
	// 每天1440分钟 (24 * 60)
	return int64(lastDay * 1440)
}

// GetAllSymbolInfos 获取所有交易对信息
func (r *Repository) GetAllSymbolInfos(ctx context.Context) ([]*domain.SymbolInfo, error) {
	query := `
		SELECT symbol, status, base_asset, quote_asset, earliest_date, latest_date, 
		       total_months, data_status, created_at, updated_at
		FROM symbol_infos
		ORDER BY symbol
	`
	
	rows, err := r.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var infos []*domain.SymbolInfo
	for rows.Next() {
		var info domain.SymbolInfo
		err := rows.Scan(
			&info.Symbol,
			&info.Status,
			&info.BaseAsset,
			&info.QuoteAsset,
			&info.EarliestDate,
			&info.LatestDate,
			&info.TotalMonths,
			&info.DataStatus,
			&info.CreatedAt,
			&info.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		infos = append(infos, &info)
	}
	
	return infos, rows.Err()
}

// UpdateSymbolInfo 更新交易对信息
func (r *Repository) UpdateSymbolInfo(ctx context.Context, symbolInfo *domain.SymbolInfo) error {
	query := `
		ALTER TABLE symbol_infos 
		UPDATE status = ?, base_asset = ?, quote_asset = ?, earliest_date = ?, 
		       latest_date = ?, total_months = ?, data_status = ?, updated_at = now()
		WHERE symbol = ?
	`
	
	// 提取基础资产和报价资产
	baseAsset := symbolInfo.BaseAsset
	quoteAsset := symbolInfo.QuoteAsset
	if baseAsset == "" || quoteAsset == "" {
		// 从symbol中提取，假设USDT结尾
		if strings.HasSuffix(symbolInfo.Symbol, "USDT") {
			baseAsset = strings.TrimSuffix(symbolInfo.Symbol, "USDT")
			quoteAsset = "USDT"
		} else {
			baseAsset = symbolInfo.Symbol
			quoteAsset = "UNKNOWN"
		}
	}
	
	return r.conn.Exec(ctx, query,
		symbolInfo.Status,
		baseAsset,
		quoteAsset,
		symbolInfo.EarliestDate,
		symbolInfo.LatestDate,
		symbolInfo.TotalMonths,
		symbolInfo.DataStatus,
		symbolInfo.Symbol,
	)
}

// QueryContext 执行查询并返回结果行
func (r *Repository) QueryContext(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return r.conn.Query(ctx, query, args...)
}