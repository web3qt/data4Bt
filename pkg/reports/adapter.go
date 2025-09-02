package reports

import (
	"fmt"
	"time"

	"binance-data-loader/internal/domain"
	"binance-data-loader/pkg/verification"
)

// VerificationResultAdapter 验证结果适配器
type VerificationResultAdapter struct{}

// NewVerificationResultAdapter 创建新的适配器
func NewVerificationResultAdapter() *VerificationResultAdapter {
	return &VerificationResultAdapter{}
}

// ConvertBatchReport 将批量验证报告转换为报告生成器需要的格式
func (vra *VerificationResultAdapter) ConvertBatchReport(
	batchReport *verification.BatchDatabaseVerificationReport,
) map[string]*domain.DataCompletenessStats {
	
	results := make(map[string]*domain.DataCompletenessStats)
	
	for _, report := range batchReport.Reports {
		stats := vra.convertSingleReport(report)
		results[report.Symbol] = stats
	}
	
	return results
}

// convertSingleReport 转换单个验证报告
func (vra *VerificationResultAdapter) convertSingleReport(
	report *verification.DatabaseVerificationReport,
) *domain.DataCompletenessStats {
	
	// 转换月度统计数据
	monthlyStats := make(map[string]*domain.MonthlyStats)
	
	var totalExpected, totalActual int64
	var firstRecord, lastRecord time.Time
	
	for _, monthlyReport := range report.MonthlyReports {
		monthlyStats[monthlyReport.Month] = &domain.MonthlyStats{
			Month:             monthlyReport.Month,
			ExpectedRecords:   monthlyReport.ExpectedRecords,
			ActualRecords:     monthlyReport.ActualRecords,
			CompletenessRatio: monthlyReport.CompletenessRatio,
			FirstRecord:       monthlyReport.FirstRecord,
			LastRecord:        monthlyReport.LastRecord,
			HasData:           monthlyReport.HasData,
		}
		
		totalExpected += monthlyReport.ExpectedRecords
		totalActual += monthlyReport.ActualRecords
		
		if monthlyReport.HasData {
			if firstRecord.IsZero() || monthlyReport.FirstRecord.Before(firstRecord) {
				firstRecord = monthlyReport.FirstRecord
			}
			if monthlyReport.LastRecord.After(lastRecord) {
				lastRecord = monthlyReport.LastRecord
			}
		}
	}
	
	return &domain.DataCompletenessStats{
		Symbol:               report.Symbol,
		TotalExpectedRecords: totalExpected,
		TotalActualRecords:   totalActual,
		CompletenessRatio:    report.OverallCompleteness,
		MonthlyStats:         monthlyStats,
		FirstRecord:          firstRecord,
		LastRecord:           lastRecord,
	}
}

// ExtractExecutionTime 从批量报告中提取执行时间（如果有记录的话）
func (vra *VerificationResultAdapter) ExtractExecutionTime(
	batchReport *verification.BatchDatabaseVerificationReport,
	startTime time.Time,
) time.Duration {
	
	// 如果批量报告有生成时间，使用它来计算执行时间
	if !batchReport.GeneratedAt.IsZero() {
		return batchReport.GeneratedAt.Sub(startTime)
	}
	
	// 否则使用当前时间
	return time.Since(startTime)
}

// GenerateReportSummary 基于批量验证报告生成摘要信息
func (vra *VerificationResultAdapter) GenerateReportSummary(
	batchReport *verification.BatchDatabaseVerificationReport,
	executionTime time.Duration,
) string {
	
	// 分类统计
	var criticalCount, attentionCount, goodCount, excellentCount int
	
	for _, report := range batchReport.Reports {
		completeness := report.OverallCompleteness
		if completeness < 60.0 {
			criticalCount++
		} else if completeness < 80.0 {
			attentionCount++
		} else if completeness < 95.0 {
			goodCount++
		} else {
			excellentCount++
		}
	}
	
	return fmt.Sprintf(
		"验证了 %d 个交易对，耗时 %s\n🔴 %d个严重问题，🟡 %d个需关注，🟢 %d个良好，✅ %d个优秀",
		batchReport.TotalSymbols,
		FormatDurationSimple(executionTime),
		criticalCount,
		attentionCount,
		goodCount,
		excellentCount,
	)
}

// AnalyzeReportIssues 分析报告中的问题并返回关键信息
func (vra *VerificationResultAdapter) AnalyzeReportIssues(
	batchReport *verification.BatchDatabaseVerificationReport,
) (criticalSymbols, attentionSymbols []string) {
	
	for _, report := range batchReport.Reports {
		completeness := report.OverallCompleteness
		if completeness < 60.0 {
			criticalSymbols = append(criticalSymbols, 
				fmt.Sprintf("%s(%.1f%%)", report.Symbol, completeness))
		} else if completeness < 80.0 {
			attentionSymbols = append(attentionSymbols, 
				fmt.Sprintf("%s(%.1f%%)", report.Symbol, completeness))
		}
	}
	
	return criticalSymbols, attentionSymbols
}