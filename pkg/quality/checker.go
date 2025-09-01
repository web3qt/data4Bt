package quality

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
	"github.com/rs/zerolog"
)

// QualityChecker 数据质量检查器
type QualityChecker struct {
	repository            domain.KLineRepository
	downloader           domain.Downloader
	expectedRecordsCalc  ExpectedRecordsCalculator
	threshold            QualityThreshold
	logger               zerolog.Logger
}

// NewQualityChecker 创建数据质量检查器
func NewQualityChecker(
	repository domain.KLineRepository, 
	downloader domain.Downloader,
) *QualityChecker {
	return &QualityChecker{
		repository:          repository,
		downloader:          downloader,
		expectedRecordsCalc: &DefaultExpectedRecordsCalculator{},
		threshold:          DefaultQualityThreshold,
		logger:             logger.GetLogger("quality_checker"),
	}
}

// CheckSymbolQuality 检查单个交易对的数据质量
func (qc *QualityChecker) CheckSymbolQuality(ctx context.Context, symbol string, startDate, endDate *time.Time) (*DataQualityReport, error) {
	startTime := time.Now()
	defer func() {
		logger.LogPerformance("quality_checker", "check_symbol_quality", time.Since(startTime), map[string]interface{}{
			"symbol": symbol,
		})
	}()

	qc.logger.Info().
		Str("symbol", symbol).
		Msg("开始检查交易对数据质量")

	// 获取该交易对在Binance的可用月份
	availableMonths, err := qc.getAvailableMonthsForSymbol(ctx, symbol, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to get available months for %s: %w", symbol, err)
	}

	if len(availableMonths) == 0 {
		qc.logger.Warn().Str("symbol", symbol).Msg("未找到可用的月份数据")
		return &DataQualityReport{
			Symbol:      symbol,
			OverallScore: 0.0,
			MissingMonths: []string{},
			Statistics:  &QualityStatistics{},
			LastChecked: time.Now(),
		}, nil
	}

	// 检查每个月份的数据状态
	monthlyStatuses := make([]*MonthlyDataStatus, 0, len(availableMonths))
	var missingMonths []string
	var partialMonths []string

	for _, month := range availableMonths {
		monthStr := month.Format("2006-01")
		
		status, err := qc.checkMonthlyData(ctx, symbol, month)
		if err != nil {
			qc.logger.Warn().
				Err(err).
				Str("symbol", symbol).
				Str("month", monthStr).
				Msg("检查月份数据失败")
			continue
		}

		monthlyStatuses = append(monthlyStatuses, status)

		// 分类记录缺失和不完整的月份
		if !status.HasData {
			missingMonths = append(missingMonths, monthStr)
		} else if status.CompletenessRatio < qc.threshold.ExcellentThreshold {
			partialMonths = append(partialMonths, monthStr)
		}
	}

	// 计算整体质量统计
	statistics := qc.calculateQualityStatistics(availableMonths, monthlyStatuses)
	
	// 计算整体评分
	overallScore := qc.calculateOverallScore(monthlyStatuses)

	report := &DataQualityReport{
		Symbol:        symbol,
		OverallScore:  overallScore,
		MonthlyStatus: monthlyStatuses,
		MissingMonths: missingMonths,
		PartialMonths: partialMonths,
		Statistics:    statistics,
		LastChecked:   time.Now(),
	}

	qc.logger.Info().
		Str("symbol", symbol).
		Float64("overall_score", overallScore).
		Int("missing_months", len(missingMonths)).
		Int("partial_months", len(partialMonths)).
		Msg("交易对数据质量检查完成")

	return report, nil
}

// CheckBatchQuality 批量检查多个交易对的数据质量
func (qc *QualityChecker) CheckBatchQuality(ctx context.Context, request *QualityCheckRequest) (*BatchQualityReport, error) {
	startTime := time.Now()
	
	qc.logger.Info().
		Strs("symbols", request.Symbols).
		Str("check_mode", request.CheckMode.String()).
		Msg("开始批量数据质量检查")

	var reports []*DataQualityReport
	checkedSymbols := 0

	// 逐个检查每个交易对
	for i, symbol := range request.Symbols {
		// 检查上下文是否被取消
		select {
		case <-ctx.Done():
			qc.logger.Info().
				Int("checked", checkedSymbols).
				Int("total", len(request.Symbols)).
				Msg("批量质量检查被取消")
			return nil, ctx.Err()
		default:
		}

		qc.logger.Debug().
			Int("current", i+1).
			Int("total", len(request.Symbols)).
			Str("symbol", symbol).
			Msg("检查交易对质量")

		report, err := qc.CheckSymbolQuality(ctx, symbol, request.StartDate, request.EndDate)
		if err != nil {
			qc.logger.Warn().
				Err(err).
				Str("symbol", symbol).
				Msg("检查交易对质量失败")
			continue
		}

		reports = append(reports, report)
		checkedSymbols++
	}

	// 生成汇总信息
	summary := qc.generateQualitySummary(reports)

	batchReport := &BatchQualityReport{
		TotalSymbols:   len(request.Symbols),
		CheckedSymbols: checkedSymbols,
		Reports:        reports,
		Summary:        summary,
		GeneratedAt:    time.Now(),
		CheckDuration:  time.Since(startTime),
		CheckMode:      request.CheckMode,
	}

	qc.logger.Info().
		Int("total_symbols", len(request.Symbols)).
		Int("checked_symbols", checkedSymbols).
		Float64("average_score", summary.AverageScore).
		Dur("duration", time.Since(startTime)).
		Msg("批量数据质量检查完成")

	return batchReport, nil
}

// getAvailableMonthsForSymbol 获取交易对的可用月份
func (qc *QualityChecker) getAvailableMonthsForSymbol(ctx context.Context, symbol string, startDate, endDate *time.Time) ([]time.Time, error) {
	// 从下载器获取Binance上可用的月份
	availableDates, err := qc.downloader.GetAvailableDates(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get available dates from downloader: %w", err)
	}

	var filteredMonths []time.Time
	
	for _, date := range availableDates {
		// 应用时间范围过滤
		if startDate != nil && date.Before(*startDate) {
			continue
		}
		if endDate != nil && date.After(*endDate) {
			continue
		}
		
		// 跳过当前月份（数据可能不完整）
		currentMonth := time.Now().UTC().Truncate(24 * time.Hour)
		currentMonth = time.Date(currentMonth.Year(), currentMonth.Month(), 1, 0, 0, 0, 0, time.UTC)
		if !date.Before(currentMonth) {
			continue
		}
		
		filteredMonths = append(filteredMonths, date)
	}

	// 按时间排序
	sort.Slice(filteredMonths, func(i, j int) bool {
		return filteredMonths[i].Before(filteredMonths[j])
	})

	return filteredMonths, nil
}

// checkMonthlyData 检查单个月份的数据状态
func (qc *QualityChecker) checkMonthlyData(ctx context.Context, symbol string, month time.Time) (*MonthlyDataStatus, error) {
	monthStr := month.Format("2006-01")
	
	// 计算该月的预期记录数
	expectedRecords := qc.expectedRecordsCalc.CalculateExpectedRecords(monthStr)
	
	// 查询该月的实际数据统计
	actualRecords, firstRecord, lastRecord, err := qc.getMonthlyDataStats(ctx, symbol, month)
	if err != nil {
		return nil, fmt.Errorf("failed to get monthly data stats: %w", err)
	}

	hasData := actualRecords > 0
	completenessRatio := 0.0
	if expectedRecords > 0 {
		completenessRatio = float64(actualRecords) / float64(expectedRecords) * 100
	}

	// 检查数据质量问题
	var qualityIssues []string
	if !hasData {
		qualityIssues = append(qualityIssues, IssueNoData.Description())
	} else if actualRecords < expectedRecords {
		qualityIssues = append(qualityIssues, fmt.Sprintf("%s (缺失%d条记录)", 
			IssueIncompleteData.Description(), expectedRecords-actualRecords))
	}

	status := &MonthlyDataStatus{
		Month:              monthStr,
		HasData:            hasData,
		ExpectedRecords:    expectedRecords,
		ActualRecords:      actualRecords,
		CompletenessRatio:  completenessRatio,
		DataQualityIssues:  qualityIssues,
		FirstRecord:        firstRecord,
		LastRecord:         lastRecord,
	}

	return status, nil
}

// getMonthlyDataStats 获取月度数据统计
func (qc *QualityChecker) getMonthlyDataStats(ctx context.Context, symbol string, month time.Time) (int64, time.Time, time.Time, error) {
	return qc.repository.GetMonthlyDataStats(ctx, symbol, month)
}

// calculateQualityStatistics 计算质量统计信息
func (qc *QualityChecker) calculateQualityStatistics(availableMonths []time.Time, monthlyStatuses []*MonthlyDataStatus) *QualityStatistics {
	stats := &QualityStatistics{
		TotalMonths: len(availableMonths),
	}

	if len(availableMonths) > 0 {
		stats.AvailableFrom = availableMonths[0].Format("2006-01")
		stats.AvailableTo = availableMonths[len(availableMonths)-1].Format("2006-01")
		stats.CoverageMonths = len(availableMonths)
	}

	var totalRecords, expectedRecords int64
	completeMonths := 0
	partialMonths := 0
	missingMonths := 0

	for _, status := range monthlyStatuses {
		totalRecords += status.ActualRecords
		expectedRecords += status.ExpectedRecords

		if !status.HasData {
			missingMonths++
		} else if status.CompletenessRatio >= qc.threshold.ExcellentThreshold {
			completeMonths++
		} else {
			partialMonths++
		}
	}

	stats.CompleteMonths = completeMonths
	stats.PartialMonths = partialMonths
	stats.MissingMonths = missingMonths
	stats.TotalRecords = totalRecords
	stats.ExpectedRecords = expectedRecords

	if expectedRecords > 0 {
		stats.CompletenessRatio = float64(totalRecords) / float64(expectedRecords) * 100
	}

	return stats
}

// calculateOverallScore 计算整体评分
func (qc *QualityChecker) calculateOverallScore(monthlyStatuses []*MonthlyDataStatus) float64 {
	if len(monthlyStatuses) == 0 {
		return 0.0
	}

	var totalScore float64
	for _, status := range monthlyStatuses {
		totalScore += status.CompletenessRatio
	}

	return totalScore / float64(len(monthlyStatuses))
}

// generateQualitySummary 生成质量汇总信息
func (qc *QualityChecker) generateQualitySummary(reports []*DataQualityReport) *QualitySummary {
	summary := &QualitySummary{}
	
	if len(reports) == 0 {
		return summary
	}

	var totalScore float64
	for _, report := range reports {
		totalScore += report.OverallScore
		summary.TotalMissingMonths += len(report.MissingMonths)
		summary.TotalPartialMonths += len(report.PartialMonths)

		level := GetQualityLevel(report.OverallScore)
		switch level {
		case QualityLevelExcellent:
			summary.ExcellentCount++
		case QualityLevelGood:
			summary.GoodCount++
		case QualityLevelAcceptable:
			summary.AcceptableCount++
		case QualityLevelPoor:
			summary.PoorCount++
		}
	}

	summary.AverageScore = totalScore / float64(len(reports))
	return summary
}

// GetAllSymbols 获取所有可用的交易对列表
func (qc *QualityChecker) GetAllSymbols(ctx context.Context) ([]string, error) {
	symbols, err := qc.downloader.GetSymbols(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get symbols from downloader: %w", err)
	}

	// 过滤和排序
	var filteredSymbols []string
	for _, symbol := range symbols {
		// 只保留USDT交易对
		if strings.HasSuffix(symbol, "USDT") {
			filteredSymbols = append(filteredSymbols, symbol)
		}
	}

	sort.Strings(filteredSymbols)
	return filteredSymbols, nil
}

// ValidateRequest 验证质量检查请求
func (qc *QualityChecker) ValidateRequest(request *QualityCheckRequest) error {
	if len(request.Symbols) == 0 {
		return fmt.Errorf("symbols list cannot be empty")
	}

	if request.StartDate != nil && request.EndDate != nil {
		if request.StartDate.After(*request.EndDate) {
			return fmt.Errorf("start date cannot be after end date")
		}
	}

	return nil
}