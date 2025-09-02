package csvexport

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
	"binance-data-loader/pkg/clickhouse"
)

// ExportParams CSV导出参数
type ExportParams struct {
	Symbol     string     // 交易对，空字符串表示所有交易对
	Interval   string     // 时间间隔: 1m, 5m, 15m, 1h, 4h, 1d
	StartTime  *time.Time // 开始时间，nil表示从最早数据开始
	EndTime    *time.Time // 结束时间，nil表示到最新数据
	OutputPath string     // 输出文件路径，空字符串表示自动生成
}

// CSVExporter CSV导出器
type CSVExporter struct {
	repository *clickhouse.Repository
	logger     zerolog.Logger
}

// NewCSVExporter 创建CSV导出器
func NewCSVExporter(repository *clickhouse.Repository) *CSVExporter {
	return &CSVExporter{
		repository: repository,
		logger:     logger.GetLogger("csv_exporter"),
	}
}

// Export 导出CSV数据
func (e *CSVExporter) Export(ctx context.Context, params ExportParams) error {
	// 验证参数
	if err := e.validateParams(params); err != nil {
		return fmt.Errorf("参数验证失败: %w", err)
	}

	// 获取要导出的交易对列表
	symbols, err := e.getSymbolsToExport(ctx, params.Symbol)
	if err != nil {
		return fmt.Errorf("获取交易对列表失败: %w", err)
	}

	if len(symbols) == 0 {
		return fmt.Errorf("没有找到符合条件的交易对")
	}

	// 生成输出文件路径
	outputPath := e.generateOutputPath(params, symbols)

	// 显示导出信息
	e.printExportInfo(params, symbols, outputPath)

	// 执行导出
	return e.exportToFile(ctx, params, symbols, outputPath)
}

// validateParams 验证导出参数
func (e *CSVExporter) validateParams(params ExportParams) error {
	// 验证时间间隔
	supportedIntervals := map[string]bool{
		"1m": true, "5m": true, "15m": true, 
		"1h": true, "4h": true, "1d": true,
	}
	
	if !supportedIntervals[params.Interval] {
		return fmt.Errorf("不支持的时间间隔: %s，支持的间隔: 1m, 5m, 15m, 1h, 4h, 1d", params.Interval)
	}

	// 验证时间范围
	if params.StartTime != nil && params.EndTime != nil {
		if params.StartTime.After(*params.EndTime) {
			return fmt.Errorf("开始时间不能晚于结束时间")
		}
	}

	return nil
}

// getSymbolsToExport 获取要导出的交易对列表
func (e *CSVExporter) getSymbolsToExport(ctx context.Context, symbol string) ([]string, error) {
	if symbol != "" {
		// 单个交易对
		symbol = strings.ToUpper(strings.TrimSpace(symbol))
		return []string{symbol}, nil
	}

	// 所有交易对
	symbolInfos, err := e.repository.GetAllSymbolInfos(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取交易对信息失败: %w", err)
	}

	symbols := make([]string, len(symbolInfos))
	for i, info := range symbolInfos {
		symbols[i] = info.Symbol
	}

	return symbols, nil
}

// generateOutputPath 生成输出文件路径
func (e *CSVExporter) generateOutputPath(params ExportParams, symbols []string) string {
	if params.OutputPath != "" {
		return params.OutputPath
	}

	// 自动生成文件名
	var symbolPart string
	if len(symbols) == 1 {
		symbolPart = strings.ToLower(symbols[0])
	} else {
		symbolPart = "all_symbols"
	}

	var datePart string
	if params.StartTime != nil && params.EndTime != nil {
		datePart = fmt.Sprintf("_%s_%s", 
			params.StartTime.Format("20060102"), 
			params.EndTime.Format("20060102"))
	}

	return fmt.Sprintf("%s_%s%s.csv", symbolPart, params.Interval, datePart)
}

// printExportInfo 显示导出信息
func (e *CSVExporter) printExportInfo(params ExportParams, symbols []string, outputPath string) {
	fmt.Println("🚀 开始导出CSV数据...")
	
	if len(symbols) == 1 {
		fmt.Printf("📊 交易对: %s", symbols[0])
	} else {
		fmt.Printf("📊 交易对: 全部 (%d个)", len(symbols))
	}
	fmt.Printf(" | 时间间隔: %s", params.Interval)
	
	if params.StartTime != nil && params.EndTime != nil {
		fmt.Printf(" | 时间范围: %s to %s", 
			params.StartTime.Format("2006-01-02"), 
			params.EndTime.Format("2006-01-02"))
	} else {
		fmt.Printf(" | 时间范围: 全部可用数据")
	}
	
	fmt.Printf("\n📁 输出文件: %s\n\n", outputPath)
}

// exportToFile 导出数据到文件
func (e *CSVExporter) exportToFile(ctx context.Context, params ExportParams, symbols []string, outputPath string) error {
	// 创建输出文件
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			e.logger.Warn().Err(err).Msg("关闭输出文件失败")
		}
	}()

	// 创建CSV写入器
	csvWriter := csv.NewWriter(file)
	defer csvWriter.Flush()

	// 写入CSV头部
	if err := e.writeCSVHeader(csvWriter); err != nil {
		return fmt.Errorf("写入CSV头部失败: %w", err)
	}

	// 导出数据
	totalRecords := 0
	startTime := time.Now()
	
	for _, symbol := range symbols {
		records, err := e.exportSymbolData(ctx, csvWriter, symbol, params)
		if err != nil {
			e.logger.Error().Err(err).Str("symbol", symbol).Msg("导出交易对数据失败")
			return fmt.Errorf("导出交易对 %s 数据失败: %w", symbol, err)
		}
		totalRecords += records
		
		// 显示进度
		if len(symbols) > 1 {
			fmt.Printf("✅ %s: %d 条记录\n", symbol, records)
		}
	}

	duration := time.Since(startTime)
	
	// 显示完成信息
	fmt.Printf("\n🎉 导出完成！\n")
	fmt.Printf("📊 总记录数: %s\n", formatNumber(totalRecords))
	fmt.Printf("⏱️  用时: %v\n", duration.Round(time.Millisecond))
	
	// 获取文件大小
	if stat, err := file.Stat(); err == nil {
		fmt.Printf("📁 文件大小: %s\n", formatBytes(stat.Size()))
	}

	return nil
}

// writeCSVHeader 写入CSV头部
func (e *CSVExporter) writeCSVHeader(writer *csv.Writer) error {
	headers := []string{
		"timestamp", "symbol", "open", "high", "low", "close", 
		"volume", "quote_volume", "trades", "taker_buy_base_volume", "taker_buy_quote_volume",
	}
	return writer.Write(headers)
}

// exportSymbolData 导出单个交易对的数据
func (e *CSVExporter) exportSymbolData(ctx context.Context, csvWriter *csv.Writer, symbol string, params ExportParams) (int, error) {
	const batchSize = 10000
	offset := 0
	totalRecords := 0

	for {
		// 检查上下文是否被取消
		select {
		case <-ctx.Done():
			return totalRecords, ctx.Err()
		default:
		}

		// 查询数据批次
		klines, err := e.queryKlinesBatch(ctx, symbol, params, offset, batchSize)
		if err != nil {
			return totalRecords, err
		}

		// 如果没有更多数据，退出循环
		if len(klines) == 0 {
			break
		}

		// 写入CSV数据
		for _, kline := range klines {
			record := []string{
				kline.OpenTime.UTC().Format(time.RFC3339),
				kline.Symbol,
				formatFloat(kline.OpenPrice),
				formatFloat(kline.HighPrice),
				formatFloat(kline.LowPrice),
				formatFloat(kline.ClosePrice),
				formatFloat(kline.Volume),
				formatFloat(kline.QuoteAssetVolume),
				strconv.FormatInt(kline.NumberOfTrades, 10),
				formatFloat(kline.TakerBuyBaseVolume),
				formatFloat(kline.TakerBuyQuoteVolume),
			}
			
			if err := csvWriter.Write(record); err != nil {
				return totalRecords, fmt.Errorf("写入CSV记录失败: %w", err)
			}
		}

		totalRecords += len(klines)
		offset += len(klines)

		// 如果返回的记录数小于批次大小，说明已经是最后一批
		if len(klines) < batchSize {
			break
		}
	}

	return totalRecords, nil
}

// queryKlinesBatch 批量查询K线数据
func (e *CSVExporter) queryKlinesBatch(ctx context.Context, symbol string, params ExportParams, offset, limit int) ([]domain.KLine, error) {
	tableName := e.getTableName(params.Interval)
	
	// 构建查询SQL
	query := fmt.Sprintf(`
		SELECT 
			open_time, symbol, open_price, high_price, low_price, close_price,
			volume, quote_asset_volume, number_of_trades, 
			taker_buy_base_volume, taker_buy_quote_volume, interval, created_at
		FROM %s
		WHERE symbol = ?`, tableName)
	
	var args []interface{}
	args = append(args, symbol)
	
	// 添加时间范围条件
	if params.StartTime != nil {
		query += " AND open_time >= ?"
		args = append(args, *params.StartTime)
	}
	
	if params.EndTime != nil {
		query += " AND open_time < ?"
		args = append(args, *params.EndTime)
	}
	
	query += " ORDER BY open_time LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	// 执行查询
	rows, err := e.repository.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("查询数据失败: %w", err)
	}
	defer rows.Close()

	var klines []domain.KLine
	for rows.Next() {
		var kline domain.KLine
		err := rows.Scan(
			&kline.OpenTime, &kline.Symbol, &kline.OpenPrice, &kline.HighPrice, &kline.LowPrice, &kline.ClosePrice,
			&kline.Volume, &kline.QuoteAssetVolume, &kline.NumberOfTrades,
			&kline.TakerBuyBaseVolume, &kline.TakerBuyQuoteVolume, &kline.Interval, &kline.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描数据失败: %w", err)
		}
		klines = append(klines, kline)
	}

	return klines, rows.Err()
}

// getTableName 根据时间间隔获取表名
func (e *CSVExporter) getTableName(interval string) string {
	if interval == "1m" {
		return "klines_1m"
	}
	return fmt.Sprintf("klines_%s", interval)
}

// formatFloat 格式化浮点数
func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// formatNumber 格式化数字显示
func formatNumber(n int) string {
	if n < 1000 {
		return strconv.Itoa(n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%.1fM", float64(n)/1000000)
}

// formatBytes 格式化字节数显示
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}