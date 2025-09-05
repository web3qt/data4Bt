package verification

import (
	"context"
	"fmt"
	"time"

	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
	"github.com/rs/zerolog"
)

// DatabaseVerificationChecker 数据库验证检查器接口
type DatabaseVerificationChecker interface {
	// VerifySymbolData 验证单个交易对的数据完整性
	VerifySymbolData(ctx context.Context, symbol string, startDate, endDate *time.Time) (*DatabaseVerificationReport, error)
	
	// VerifyBatchSymbols 批量验证多个交易对
	VerifyBatchSymbols(ctx context.Context, symbols []string, startDate, endDate *time.Time) (*BatchDatabaseVerificationReport, error)
}

// databaseVerificationChecker 数据库验证检查器实现
type databaseVerificationChecker struct {
	repository           domain.KLineRepository
	rangeAnalyzer        DataRangeAnalyzer
	completenessAnalyzer CompletenessAnalyzer
	logger               zerolog.Logger
}

// NewDatabaseVerificationChecker 创建数据库验证检查器
func NewDatabaseVerificationChecker(repository domain.KLineRepository) DatabaseVerificationChecker {
	return &databaseVerificationChecker{
		repository:           repository,
		rangeAnalyzer:        NewDataRangeAnalyzer(repository),
		completenessAnalyzer: NewCompletenessAnalyzer(repository),
		logger:               logger.GetLogger("database_verification_checker"),
	}
}

// VerifySymbolData 验证单个交易对的数据完整性
func (dvc *databaseVerificationChecker) VerifySymbolData(ctx context.Context, symbol string, startDate, endDate *time.Time) (*DatabaseVerificationReport, error) {
	startTime := time.Now()
	defer func() {
		logger.LogPerformance("database_verification_checker", "verify_symbol_data", time.Since(startTime), map[string]interface{}{
			"symbol": symbol,
		})
	}()
	
	dvc.logger.Info().
		Str("symbol", symbol).
		Msg("开始验证交易对数据完整性")
	
	// 1. 分析数据范围
	dataRange, err := dvc.rangeAnalyzer.AnalyzeSymbolRange(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze symbol range for %s: %w", symbol, err)
	}
	
	// 如果没有数据，返回空报告
	if !dataRange.HasData {
		dvc.logger.Warn().Str("symbol", symbol).Msg("交易对没有数据")
		return &DatabaseVerificationReport{
			Symbol:                symbol,
			DataRange:            dataRange,
			MonthlyReports:       []*MonthlyCompletenessReport{},
			OverallCompleteness:  0.0,
			MissingMonths:        []string{},
			IncompleteMonths:     []string{},
			QualityScore:         0.0,
			GeneratedAt:          time.Now(),
		}, nil
	}
	
	// 2. 根据用户指定的日期范围过滤月份
	filteredMonths := dvc.rangeAnalyzer.FilterMonthsByDateRange(dataRange.MonthList, startDate, endDate)
	
	dvc.logger.Debug().
		Str("symbol", symbol).
		Int("total_months", len(dataRange.MonthList)).
		Int("filtered_months", len(filteredMonths)).
		Msg("月份过滤完成")
	
	// 3. 分析月度完整性
	monthlyReports, err := dvc.completenessAnalyzer.AnalyzeMonthlyCompleteness(ctx, symbol, filteredMonths)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze monthly completeness for %s: %w", symbol, err)
	}
	
	// 4. 计算总体完整性
	overallCompleteness := dvc.completenessAnalyzer.CalculateOverallCompleteness(monthlyReports)
	
	// 5. 识别缺失和不完整的月份
	missingMonths := dvc.completenessAnalyzer.IdentifyMissingMonths(monthlyReports)
	incompleteMonths := dvc.completenessAnalyzer.IdentifyIncompleteMonths(monthlyReports)
	
	// 6. 计算质量评分（与总体完整性相同）
	qualityScore := overallCompleteness
	
	dvc.logger.Info().
		Str("symbol", symbol).
		Int("total_months", len(filteredMonths)).
		Int("missing_months", len(missingMonths)).
		Int("incomplete_months", len(incompleteMonths)).
		Float64("overall_completeness", overallCompleteness).
		Float64("quality_score", qualityScore).
		Msg("交易对数据完整性验证完成")
	
	return &DatabaseVerificationReport{
		Symbol:                symbol,
		DataRange:            dataRange,
		MonthlyReports:       monthlyReports,
		OverallCompleteness:  overallCompleteness,
		MissingMonths:        missingMonths,
		IncompleteMonths:     incompleteMonths,
		QualityScore:         qualityScore,
		GeneratedAt:          time.Now(),
	}, nil
}

// VerifyBatchSymbols 批量验证多个交易对
func (dvc *databaseVerificationChecker) VerifyBatchSymbols(ctx context.Context, symbols []string, startDate, endDate *time.Time) (*BatchDatabaseVerificationReport, error) {
	startTime := time.Now()
	defer func() {
		logger.LogPerformance("database_verification_checker", "verify_batch_symbols", time.Since(startTime), map[string]interface{}{
			"symbols_count": len(symbols),
		})
	}()
	
	dvc.logger.Info().
		Int("symbols_count", len(symbols)).
		Msg("开始批量验证交易对数据完整性")
	
	// 使用批量分析优化性能 - 单次查询获取所有交易对的数据范围
	dvc.logger.Info().Msg("🔍 批量分析交易对数据范围...")
	dataRanges, err := dvc.rangeAnalyzer.AnalyzeBatchSymbolRanges(ctx, symbols)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze batch symbol ranges: %w", err)
	}
	
	var reports []*DatabaseVerificationReport
	verifiedSymbols := 0
	
	// 分批处理交易对以显示进度并控制内存使用
	batchSize := 50 // 每批处理50个交易对
	totalBatches := (len(symbols) + batchSize - 1) / batchSize
	
	for batchIdx := 0; batchIdx < totalBatches; batchIdx++ {
		start := batchIdx * batchSize
		end := start + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}
		
		batchSymbols := symbols[start:end]
		dvc.logger.Info().
			Int("batch", batchIdx+1).
			Int("total_batches", totalBatches).
			Int("batch_size", len(batchSymbols)).
			Msg("🔄 处理批次")
		
		// 处理当前批次中的每个交易对
		for i, symbol := range batchSymbols {
			currentIndex := start + i + 1
			
			dvc.logger.Debug().
				Str("symbol", symbol).
				Int("current", currentIndex).
				Int("total", len(symbols)).
				Msg("验证交易对")
			
			// 从批量分析结果中获取数据范围
			dataRange, exists := dataRanges[symbol]
			if !exists {
				dvc.logger.Warn().
					Str("symbol", symbol).
					Msg("未找到交易对数据范围信息")
				
				// 创建空数据范围
				dataRange = &SymbolDataRange{
					Symbol:  symbol,
					HasData: false,
				}
			}
			
			var report *DatabaseVerificationReport
			
			// 如果没有数据，创建空报告
			if !dataRange.HasData {
				report = &DatabaseVerificationReport{
					Symbol:                symbol,
					DataRange:            dataRange,
					MonthlyReports:       []*MonthlyCompletenessReport{},
					OverallCompleteness:  0.0,
					MissingMonths:        []string{},
					IncompleteMonths:     []string{},
					QualityScore:         0.0,
					GeneratedAt:          time.Now(),
				}
			} else {
				// 有数据的交易对继续完整性分析
				filteredMonths := dvc.rangeAnalyzer.FilterMonthsByDateRange(dataRange.MonthList, startDate, endDate)
				
				monthlyReports, err := dvc.completenessAnalyzer.AnalyzeMonthlyCompleteness(ctx, symbol, filteredMonths)
				if err != nil {
					dvc.logger.Error().
						Str("symbol", symbol).
						Err(err).
						Msg("分析月度完整性失败")
					
					// 创建错误报告
					report = &DatabaseVerificationReport{
						Symbol:                symbol,
						DataRange:            dataRange,
						MonthlyReports:       []*MonthlyCompletenessReport{},
						OverallCompleteness:  0.0,
						MissingMonths:        []string{},
						IncompleteMonths:     []string{},
						QualityScore:         0.0,
						GeneratedAt:          time.Now(),
					}
				} else {
					// 计算完整性指标
					overallCompleteness := dvc.completenessAnalyzer.CalculateOverallCompleteness(monthlyReports)
					missingMonths := dvc.completenessAnalyzer.IdentifyMissingMonths(monthlyReports)
					incompleteMonths := dvc.completenessAnalyzer.IdentifyIncompleteMonths(monthlyReports)
					qualityScore := overallCompleteness
					
					report = &DatabaseVerificationReport{
						Symbol:                symbol,
						DataRange:            dataRange,
						MonthlyReports:       monthlyReports,
						OverallCompleteness:  overallCompleteness,
						MissingMonths:        missingMonths,
						IncompleteMonths:     incompleteMonths,
						QualityScore:         qualityScore,
						GeneratedAt:          time.Now(),
					}
					
					verifiedSymbols++
				}
			}
			
			reports = append(reports, report)
		}
	}
	
	// 计算批量统计信息
	summary := dvc.calculateBatchSummary(reports)
	averageCompleteness := summary.AverageScore
	
	dvc.logger.Info().
		Int("total_symbols", len(symbols)).
		Int("verified_symbols", verifiedSymbols).
		Float64("average_completeness", averageCompleteness).
		Msg("批量验证完成")
	
	return &BatchDatabaseVerificationReport{
		Reports:             reports,
		TotalSymbols:        len(symbols),
		VerifiedSymbols:     verifiedSymbols,
		AverageCompleteness: averageCompleteness,
		Summary:             summary,
		GeneratedAt:         time.Now(),
	}, nil
}

// calculateBatchSummary 计算批量验证摘要
func (dvc *databaseVerificationChecker) calculateBatchSummary(reports []*DatabaseVerificationReport) *VerificationSummary {
	var totalMonths, completeMonths, partialMonths, missingMonths int
	var totalScore float64
	validReports := 0
	
	for _, report := range reports {
		if report.DataRange.HasData {
			validReports++
			totalScore += report.QualityScore
			
			for _, monthlyReport := range report.MonthlyReports {
				totalMonths++
				switch monthlyReport.Status {
				case CompletenessStatusComplete:
					completeMonths++
				case CompletenessStatusPartial:
					partialMonths++
				case CompletenessStatusMissing:
					missingMonths++
				}
			}
		}
	}
	
	var averageScore float64
	if validReports > 0 {
		averageScore = totalScore / float64(validReports)
	}
	
	qualityGrade := GetQualityGrade(averageScore)
	
	dvc.logger.Debug().
		Int("total_months", totalMonths).
		Int("complete_months", completeMonths).
		Int("partial_months", partialMonths).
		Int("missing_months", missingMonths).
		Float64("average_score", averageScore).
		Str("quality_grade", qualityGrade).
		Msg("批量验证摘要计算完成")
	
	return &VerificationSummary{
		TotalMonths:    totalMonths,
		CompleteMonths: completeMonths,
		PartialMonths:  partialMonths,
		MissingMonths:  missingMonths,
		AverageScore:   averageScore,
		QualityGrade:   qualityGrade,
	}
}