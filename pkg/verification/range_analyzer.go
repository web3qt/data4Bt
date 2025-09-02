package verification

import (
	"context"
	"fmt"
	"time"

	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
	"github.com/rs/zerolog"
)

// DataRangeAnalyzer 数据范围分析器接口
type DataRangeAnalyzer interface {
	// AnalyzeSymbolRange 分析交易对数据范围
	AnalyzeSymbolRange(ctx context.Context, symbol string) (*SymbolDataRange, error)
	
	// GenerateMonthList 生成指定范围内的月份列表
	GenerateMonthList(startDate, endDate time.Time) []string
	
	// FilterMonthsByDateRange 根据日期范围过滤月份
	FilterMonthsByDateRange(months []string, startDate, endDate *time.Time) []string
}

// dataRangeAnalyzer 数据范围分析器实现
type dataRangeAnalyzer struct {
	repository domain.KLineRepository
	logger     zerolog.Logger
}

// NewDataRangeAnalyzer 创建数据范围分析器
func NewDataRangeAnalyzer(repository domain.KLineRepository) DataRangeAnalyzer {
	return &dataRangeAnalyzer{
		repository: repository,
		logger:     logger.GetLogger("data_range_analyzer"),
	}
}

// AnalyzeSymbolRange 分析交易对数据范围
func (dra *dataRangeAnalyzer) AnalyzeSymbolRange(ctx context.Context, symbol string) (*SymbolDataRange, error) {
	dra.logger.Debug().Str("symbol", symbol).Msg("开始分析交易对数据范围")
	
	// 获取最早日期
	earliestDate, err := dra.repository.GetFirstDate(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get earliest date for %s: %w", symbol, err)
	}
	
	// 获取最新日期
	latestDate, err := dra.repository.GetLastDate(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest date for %s: %w", symbol, err)
	}
	
	// 检查是否有数据
	hasData := !earliestDate.IsZero() && !latestDate.IsZero()
	if !hasData {
		dra.logger.Warn().Str("symbol", symbol).Msg("交易对没有数据")
		return &SymbolDataRange{
			Symbol:       symbol,
			EarliestDate: time.Time{},
			LatestDate:   time.Time{},
			TotalMonths:  0,
			MonthList:    []string{},
			HasData:      false,
		}, nil
	}
	
	// 生成月份列表
	monthList := dra.GenerateMonthList(earliestDate, latestDate)
	
	dra.logger.Info().
		Str("symbol", symbol).
		Time("earliest_date", earliestDate).
		Time("latest_date", latestDate).
		Int("total_months", len(monthList)).
		Msg("交易对数据范围分析完成")
	
	return &SymbolDataRange{
		Symbol:       symbol,
		EarliestDate: earliestDate,
		LatestDate:   latestDate,
		TotalMonths:  len(monthList),
		MonthList:    monthList,
		HasData:      true,
	}, nil
}

// GenerateMonthList 生成指定范围内的月份列表
func (dra *dataRangeAnalyzer) GenerateMonthList(startDate, endDate time.Time) []string {
	if startDate.IsZero() || endDate.IsZero() {
		return []string{}
	}
	
	// 确保开始日期在结束日期之前
	if startDate.After(endDate) {
		startDate, endDate = endDate, startDate
	}
	
	var months []string
	
	// 从开始月份遍历到结束月份
	current := time.Date(startDate.Year(), startDate.Month(), 1, 0, 0, 0, 0, time.UTC)
	endMonth := time.Date(endDate.Year(), endDate.Month(), 1, 0, 0, 0, 0, time.UTC)
	
	for !current.After(endMonth) {
		months = append(months, current.Format("2006-01"))
		current = current.AddDate(0, 1, 0)
	}
	
	return months
}

// FilterMonthsByDateRange 根据日期范围过滤月份
func (dra *dataRangeAnalyzer) FilterMonthsByDateRange(months []string, startDate, endDate *time.Time) []string {
	if len(months) == 0 {
		return months
	}
	
	// 如果没有指定过滤条件，返回原列表
	if startDate == nil && endDate == nil {
		return months
	}
	
	var filtered []string
	
	for _, month := range months {
		monthTime, err := time.Parse("2006-01", month)
		if err != nil {
			dra.logger.Warn().Str("month", month).Err(err).Msg("无效的月份格式，跳过")
			continue
		}
		
		// 检查开始日期过滤
		if startDate != nil {
			// 月份的第一天
			monthStart := time.Date(monthTime.Year(), monthTime.Month(), 1, 0, 0, 0, 0, time.UTC)
			if monthStart.Before(*startDate) {
				continue
			}
		}
		
		// 检查结束日期过滤
		if endDate != nil {
			// 月份的第一天
			monthStart := time.Date(monthTime.Year(), monthTime.Month(), 1, 0, 0, 0, 0, time.UTC)
			if monthStart.After(*endDate) {
				continue
			}
		}
		
		filtered = append(filtered, month)
	}
	
	dra.logger.Debug().
		Int("original_count", len(months)).
		Int("filtered_count", len(filtered)).
		Msg("月份列表过滤完成")
	
	return filtered
}