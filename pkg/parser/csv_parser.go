package parser

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
)

// CSVParser CSV解析器
type CSVParser struct {
	config config.ParserConfig
	logger zerolog.Logger
}

// NewCSVParser 创建新的CSV解析器
func NewCSVParser(cfg config.ParserConfig) *CSVParser {
	return &CSVParser{
		config: cfg,
		logger: logger.GetLogger("csv_parser"),
	}
}

// Parse 解析CSV数据
func (p *CSVParser) Parse(ctx context.Context, data []byte, symbol string) ([]domain.KLine, *domain.ValidationResult, error) {
	reader := strings.NewReader(string(data))
	start := time.Now()
	defer func() {
		logger.LogPerformance("csv_parser", "parse", time.Since(start), map[string]interface{}{
			"symbol": symbol,
		})
	}()

	p.logger.Debug().Str("symbol", symbol).Msg("Starting CSV parsing")

	// 创建CSV读取器
	csvReader := csv.NewReader(reader)
	csvReader.ReuseRecord = true // 重用记录以减少内存分配

	var klines []domain.KLine
	validationResult := &domain.ValidationResult{
		Valid:       true,
		TotalRows:   0,
		ValidRows:   0,
		InvalidRows: 0,
		Errors:      []string{},
		Warnings:    []string{},
	}

	lineNumber := 0
	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}

		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read CSV record at line %d: %w", lineNumber+1, err)
		}

		lineNumber++
		validationResult.TotalRows++

		// 解析记录
		kline, err := p.parseRecord(record, symbol, lineNumber)
		if err != nil {
			validationResult.InvalidRows++
			validationResult.Errors = append(validationResult.Errors, 
				fmt.Sprintf("Line %d: %s", lineNumber, err.Error()))
			
			// 如果启用了数据验证且错误过多，则停止解析
			if p.config.ValidateData && len(validationResult.Errors) > 100 {
				validationResult.Valid = false
				return nil, validationResult, fmt.Errorf("too many parsing errors, stopped at line %d", lineNumber)
			}
			continue
		}

		// 验证K线数据
		if err := p.validateKLine(kline); err != nil {
			validationResult.InvalidRows++
			validationResult.Warnings = append(validationResult.Warnings,
				fmt.Sprintf("Line %d: %s", lineNumber, err.Error()))
			continue
		}

		validationResult.ValidRows++
		klines = append(klines, *kline)

		// 检查内存使用
		if len(klines)%10000 == 0 {
			p.logger.Debug().
				Int("parsed_lines", len(klines)).
				Str("symbol", symbol).
				Msg("Parsing progress")
		}
	}

	// 最终验证
	if validationResult.InvalidRows > 0 {
		validationResult.Valid = false
	}

	// 检查数据完整性
	if err := p.validateDataCompleteness(klines, validationResult); err != nil {
		validationResult.Warnings = append(validationResult.Warnings, err.Error())
	}

	p.logger.Info().
		Str("symbol", symbol).
		Int("total_rows", validationResult.TotalRows).
		Int("valid_rows", validationResult.ValidRows).
		Int("invalid_rows", validationResult.InvalidRows).
		Bool("valid", validationResult.Valid).
		Msg("CSV parsing completed")

	return klines, validationResult, nil
}

// parseRecord 解析单条CSV记录
func (p *CSVParser) parseRecord(record []string, symbol string, lineNumber int) (*domain.KLine, error) {
	// 币安K线CSV格式:
	// [0] Open time, [1] Open, [2] High, [3] Low, [4] Close, [5] Volume,
	// [6] Close time, [7] Quote asset volume, [8] Number of trades,
	// [9] Taker buy base asset volume, [10] Taker buy quote asset volume, [11] Ignore

	if len(record) < 12 {
		return nil, fmt.Errorf("invalid record length: expected 12 fields, got %d", len(record))
	}

	// 解析时间戳
	openTime, err := p.parseTimestamp(record[0])
	if err != nil {
		return nil, fmt.Errorf("invalid open time: %w", err)
	}

	closeTime, err := p.parseTimestamp(record[6])
	if err != nil {
		return nil, fmt.Errorf("invalid close time: %w", err)
	}

	// 解析价格数据
	openPrice, err := p.parseFloat(record[1], "open price")
	if err != nil {
		return nil, err
	}

	highPrice, err := p.parseFloat(record[2], "high price")
	if err != nil {
		return nil, err
	}

	lowPrice, err := p.parseFloat(record[3], "low price")
	if err != nil {
		return nil, err
	}

	closePrice, err := p.parseFloat(record[4], "close price")
	if err != nil {
		return nil, err
	}

	// 解析交易量数据
	volume, err := p.parseFloat(record[5], "volume")
	if err != nil {
		return nil, err
	}

	quoteAssetVolume, err := p.parseFloat(record[7], "quote asset volume")
	if err != nil {
		return nil, err
	}

	numberOfTrades, err := p.parseInt(record[8], "number of trades")
	if err != nil {
		return nil, err
	}

	takerBuyBaseVolume, err := p.parseFloat(record[9], "taker buy base volume")
	if err != nil {
		return nil, err
	}

	takerBuyQuoteVolume, err := p.parseFloat(record[10], "taker buy quote volume")
	if err != nil {
		return nil, err
	}

	kline := &domain.KLine{
		Symbol:                symbol,
		OpenTime:              openTime,
		CloseTime:             closeTime,
		OpenPrice:             openPrice,
		HighPrice:             highPrice,
		LowPrice:              lowPrice,
		ClosePrice:            closePrice,
		Volume:                volume,
		QuoteAssetVolume:      quoteAssetVolume,
		NumberOfTrades:        numberOfTrades,
		TakerBuyBaseVolume:    takerBuyBaseVolume,
		TakerBuyQuoteVolume:   takerBuyQuoteVolume,
		Interval:              "1m",
		CreatedAt:             time.Now(),
	}

	return kline, nil
}

// parseTimestamp 解析时间戳
func (p *CSVParser) parseTimestamp(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}

	// 尝试解析毫秒时间戳
	if timestamp, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Unix(timestamp/1000, (timestamp%1000)*1000000), nil
	}

	// 尝试解析ISO格式时间
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, value); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", value)
}

// parseFloat 解析浮点数
func (p *CSVParser) parseFloat(value, fieldName string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty %s", fieldName)
	}

	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", fieldName, err)
	}

	return f, nil
}

// parseInt 解析整数
func (p *CSVParser) parseInt(value, fieldName string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty %s", fieldName)
	}

	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", fieldName, err)
	}

	return i, nil
}

// validateKLine 验证K线数据
func (p *CSVParser) validateKLine(kline *domain.KLine) error {
	if !p.config.ValidateData {
		return nil
	}

	var errors []string

	// 验证价格数据
	if kline.OpenPrice <= 0 {
		errors = append(errors, "open price must be positive")
	}
	if kline.HighPrice <= 0 {
		errors = append(errors, "high price must be positive")
	}
	if kline.LowPrice <= 0 {
		errors = append(errors, "low price must be positive")
	}
	if kline.ClosePrice <= 0 {
		errors = append(errors, "close price must be positive")
	}

	// 验证价格逻辑
	if kline.HighPrice < kline.LowPrice {
		errors = append(errors, "high price cannot be less than low price")
	}
	if kline.HighPrice < kline.OpenPrice {
		errors = append(errors, "high price cannot be less than open price")
	}
	if kline.HighPrice < kline.ClosePrice {
		errors = append(errors, "high price cannot be less than close price")
	}
	if kline.LowPrice > kline.OpenPrice {
		errors = append(errors, "low price cannot be greater than open price")
	}
	if kline.LowPrice > kline.ClosePrice {
		errors = append(errors, "low price cannot be greater than close price")
	}

	// 验证交易量
	if kline.Volume < 0 {
		errors = append(errors, "volume cannot be negative")
	}
	if kline.QuoteAssetVolume < 0 {
		errors = append(errors, "quote asset volume cannot be negative")
	}
	if kline.NumberOfTrades < 0 {
		errors = append(errors, "number of trades cannot be negative")
	}
	if kline.TakerBuyBaseVolume < 0 {
		errors = append(errors, "taker buy base volume cannot be negative")
	}
	if kline.TakerBuyQuoteVolume < 0 {
		errors = append(errors, "taker buy quote volume cannot be negative")
	}

	// 验证时间
	if kline.OpenTime.IsZero() {
		errors = append(errors, "open time cannot be zero")
	}
	if kline.CloseTime.IsZero() {
		errors = append(errors, "close time cannot be zero")
	}
	if !kline.CloseTime.After(kline.OpenTime) {
		errors = append(errors, "close time must be after open time")
	}

	// 验证时间间隔（1分钟数据）
	duration := kline.CloseTime.Sub(kline.OpenTime)
	if duration < 59*time.Second || duration > 60*time.Second {
		errors = append(errors, fmt.Sprintf("invalid time interval: %v (expected ~1 minute)", duration))
	}

	// 验证交易对
	if kline.Symbol == "" {
		errors = append(errors, "symbol cannot be empty")
	}

	if len(errors) > 0 {
		return fmt.Errorf("validation failed: %s", strings.Join(errors, "; "))
	}

	return nil
}

// validateDataCompleteness 验证数据完整性
func (p *CSVParser) validateDataCompleteness(klines []domain.KLine, result *domain.ValidationResult) error {
	if len(klines) == 0 {
		return fmt.Errorf("no valid data found")
	}

	// 检查时间连续性
	if len(klines) > 1 {
		gaps := 0
		for i := 1; i < len(klines); i++ {
			expectedTime := klines[i-1].CloseTime.Add(time.Millisecond)
			if !klines[i].OpenTime.Equal(expectedTime) {
				gaps++
			}
		}

		if gaps > 0 {
			result.Warnings = append(result.Warnings, 
				fmt.Sprintf("Found %d time gaps in the data", gaps))
		}
	}

	// 检查数据量（1分钟数据，一天应该有1440条记录）
	if len(klines) > 0 {
		firstTime := klines[0].OpenTime
		lastTime := klines[len(klines)-1].OpenTime
		duration := lastTime.Sub(firstTime)
		expectedRecords := int(duration.Minutes()) + 1
		actualRecords := len(klines)

		if actualRecords < expectedRecords {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Missing data: expected %d records, got %d", expectedRecords, actualRecords))
		}
	}

	return nil
}

// ParseFromString 从字符串解析CSV数据
func (p *CSVParser) ParseFromString(ctx context.Context, data string, symbol string) ([]domain.KLine, *domain.ValidationResult, error) {
	return p.Parse(ctx, []byte(data), symbol)
}

// ParseFromBuffer 从缓冲区解析CSV数据
func (p *CSVParser) ParseFromBuffer(ctx context.Context, buffer []byte, symbol string) ([]domain.KLine, *domain.ValidationResult, error) {
	return p.Parse(ctx, buffer, symbol)
}

// ValidateCSV 验证CSV数据格式
func (p *CSVParser) ValidateCSV(data []byte) error {
	reader := strings.NewReader(string(data))
	csvReader := csv.NewReader(reader)
	
	// 读取第一行检查格式
	record, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}
	
	// 检查列数
	if len(record) < 12 {
		return fmt.Errorf("invalid CSV format: expected at least 12 columns, got %d", len(record))
	}
	
	return nil
}