package verification

import (
	"context"
	"fmt"
	"time"

	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
	"binance-data-loader/pkg/quality"
	"github.com/rs/zerolog"
)

// CompletenessAnalyzer 完整性分析器接口
type CompletenessAnalyzer interface {
	// AnalyzeMonthlyCompleteness 分析月度完整性
	AnalyzeMonthlyCompleteness(ctx context.Context, symbol string, months []string) ([]*MonthlyCompletenessReport, error)
	
	// CalculateOverallCompleteness 计算总体完整性
	CalculateOverallCompleteness(monthlyReports []*MonthlyCompletenessReport) float64
	
	// IdentifyMissingMonths 识别缺失月份
	IdentifyMissingMonths(monthlyReports []*MonthlyCompletenessReport) []string
	
	// IdentifyIncompleteMonths 识别不完整月份
	IdentifyIncompleteMonths(monthlyReports []*MonthlyCompletenessReport) []string
}

// completenessAnalyzer 完整性分析器实现
type completenessAnalyzer struct {
	repository          domain.KLineRepository
	expectedRecordsCalc quality.ExpectedRecordsCalculator
	logger              zerolog.Logger
}

// NewCompletenessAnalyzer 创建完整性分析器
func NewCompletenessAnalyzer(repository domain.KLineRepository) CompletenessAnalyzer {
	return &completenessAnalyzer{
		repository:          repository,
		expectedRecordsCalc: &quality.DefaultExpectedRecordsCalculator{},
		logger:              logger.GetLogger("completeness_analyzer"),
	}
}

// AnalyzeMonthlyCompleteness 分析月度完整性
func (ca *completenessAnalyzer) AnalyzeMonthlyCompleteness(ctx context.Context, symbol string, months []string) ([]*MonthlyCompletenessReport, error) {
	ca.logger.Info().
		Str("symbol", symbol).
		Int("months_count", len(months)).
		Msg("开始分析月度完整性")
	
	var reports []*MonthlyCompletenessReport
	
	for i, month := range months {
		ca.logger.Debug().
			Str("symbol", symbol).
			Str("month", month).
			Int("current", i+1).
			Int("total", len(months)).
			Msg("分析月份完整性")
		
		report, err := ca.analyzeMonthCompleteness(ctx, symbol, month)
		if err != nil {
			ca.logger.Error().
				Str("symbol", symbol).
				Str("month", month).
				Err(err).
				Msg("分析月份完整性失败")
			// 创建一个错误报告而不是完全失败
			report = &MonthlyCompletenessReport{
				Month:             month,
				ExpectedRecords:   0,
				ActualRecords:     0,
				CompletenessRatio: 0.0,
				Status:            CompletenessStatusUnknown,
				HasData:           false,
			}
		}
		
		reports = append(reports, report)
	}
	
	ca.logger.Info().
		Str("symbol", symbol).
		Int("analyzed_months", len(reports)).
		Msg("月度完整性分析完成")
	
	return reports, nil
}

// analyzeMonthCompleteness 分析单个月份的完整性
func (ca *completenessAnalyzer) analyzeMonthCompleteness(ctx context.Context, symbol string, month string) (*MonthlyCompletenessReport, error) {
	// 解析月份
	monthTime, err := time.Parse("2006-01", month)
	if err != nil {
		return nil, fmt.Errorf("invalid month format %s: %w", month, err)
	}
	
	// 计算该月的预期记录数
	expectedRecords := ca.expectedRecordsCalc.CalculateExpectedRecords(month)
	
	// 查询该月的实际数据统计
	actualRecords, firstRecord, lastRecord, err := ca.repository.GetMonthlyDataStats(ctx, symbol, monthTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get monthly data stats for %s %s: %w", symbol, month, err)
	}
	
	// 检查是否有数据
	hasData := actualRecords > 0
	
	// 计算完整性比例
	var completenessRatio float64
	if expectedRecords > 0 && hasData {
		completenessRatio = float64(actualRecords) / float64(expectedRecords) * 100.0
		// 确保比例不超过100%
		if completenessRatio > 100.0 {
			completenessRatio = 100.0
		}
	} else {
		completenessRatio = 0.0
	}
	
	// 确定完整性状态
	status := GetCompletenessStatus(completenessRatio, hasData)
	
	ca.logger.Debug().
		Str("symbol", symbol).
		Str("month", month).
		Int64("expected_records", expectedRecords).
		Int64("actual_records", actualRecords).
		Float64("completeness_ratio", completenessRatio).
		Str("status", string(status)).
		Bool("has_data", hasData).
		Msg("月份完整性分析完成")
	
	return &MonthlyCompletenessReport{
		Month:             month,
		ExpectedRecords:   expectedRecords,
		ActualRecords:     actualRecords,
		CompletenessRatio: completenessRatio,
		Status:            status,
		FirstRecord:       firstRecord,
		LastRecord:        lastRecord,
		HasData:           hasData,
	}, nil
}

// CalculateOverallCompleteness 计算总体完整性
func (ca *completenessAnalyzer) CalculateOverallCompleteness(monthlyReports []*MonthlyCompletenessReport) float64 {
	if len(monthlyReports) == 0 {
		return 0.0
	}
	
	var totalExpected, totalActual int64
	validMonths := 0
	
	for _, report := range monthlyReports {
		if report.Status != CompletenessStatusUnknown {
			totalExpected += report.ExpectedRecords
			totalActual += report.ActualRecords
			validMonths++
		}
	}
	
	if totalExpected == 0 || validMonths == 0 {
		return 0.0
	}
	
	overallCompleteness := float64(totalActual) / float64(totalExpected) * 100.0
	
	// 确保比例不超过100%
	if overallCompleteness > 100.0 {
		overallCompleteness = 100.0
	}
	
	ca.logger.Debug().
		Int64("total_expected", totalExpected).
		Int64("total_actual", totalActual).
		Int("valid_months", validMonths).
		Float64("overall_completeness", overallCompleteness).
		Msg("总体完整性计算完成")
	
	return overallCompleteness
}

// IdentifyMissingMonths 识别缺失月份
func (ca *completenessAnalyzer) IdentifyMissingMonths(monthlyReports []*MonthlyCompletenessReport) []string {
	var missingMonths []string
	
	for _, report := range monthlyReports {
		if report.Status == CompletenessStatusMissing {
			missingMonths = append(missingMonths, report.Month)
		}
	}
	
	ca.logger.Debug().
		Int("missing_months_count", len(missingMonths)).
		Strs("missing_months", missingMonths).
		Msg("缺失月份识别完成")
	
	return missingMonths
}

// IdentifyIncompleteMonths 识别不完整月份
func (ca *completenessAnalyzer) IdentifyIncompleteMonths(monthlyReports []*MonthlyCompletenessReport) []string {
	var incompleteMonths []string
	
	for _, report := range monthlyReports {
		if report.Status == CompletenessStatusPartial {
			incompleteMonths = append(incompleteMonths, report.Month)
		}
	}
	
	ca.logger.Debug().
		Int("incomplete_months_count", len(incompleteMonths)).
		Strs("incomplete_months", incompleteMonths).
		Msg("不完整月份识别完成")
	
	return incompleteMonths
}