package verification

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// DatabaseReporter 数据库验证报告器
type DatabaseReporter struct{}

// NewDatabaseReporter 创建数据库验证报告器
func NewDatabaseReporter() *DatabaseReporter {
	return &DatabaseReporter{}
}

// WriteJSONReport 写入JSON格式报告
func (dr *DatabaseReporter) WriteJSONReport(w io.Writer, report *BatchDatabaseVerificationReport) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

// GenerateBatchConsoleReport 生成批量验证的控制台报告
func (dr *DatabaseReporter) GenerateBatchConsoleReport(report *BatchDatabaseVerificationReport) string {
	var sb strings.Builder
	
	// 报告头部
	sb.WriteString("=== 批量数据完整性验证报告 ===\n")
	sb.WriteString(fmt.Sprintf("总交易对: %d, 已验证: %d\n", report.TotalSymbols, report.VerifiedSymbols))
	sb.WriteString(fmt.Sprintf("验证耗时: %.2fms\n", float64(time.Since(report.GeneratedAt).Nanoseconds())/1e6))
	sb.WriteString(fmt.Sprintf("生成时间: %s\n", report.GeneratedAt.Format("2006-01-02 15:04:05")))
	sb.WriteString("\n")
	
	// 完整性等级分布
	if report.Summary != nil {
		sb.WriteString("完整性等级分布:\n")
		sb.WriteString(fmt.Sprintf("  完整 (95%%+): %d 个月份\n", report.Summary.CompleteMonths))
		sb.WriteString(fmt.Sprintf("  部分 (0-95%%): %d 个月份\n", report.Summary.PartialMonths))
		sb.WriteString(fmt.Sprintf("  缺失 (0%%): %d 个月份\n", report.Summary.MissingMonths))
		sb.WriteString(fmt.Sprintf("平均完整性: %.2f%%\n", report.AverageCompleteness))
		sb.WriteString(fmt.Sprintf("总月份数: %d\n", report.Summary.TotalMonths))
		sb.WriteString("\n")
	}
	
	// 各交易对详细状况
	sb.WriteString("各交易对完整性状况:\n")
	sb.WriteString(strings.Repeat("=", 80) + "\n")
	
	for _, symbolReport := range report.Reports {
		dr.writeSymbolReport(&sb, symbolReport)
	}
	
	return sb.String()
}

// GenerateSymbolConsoleReport 生成单个交易对的控制台报告（便于流式输出）
func (dr *DatabaseReporter) GenerateSymbolConsoleReport(report *DatabaseVerificationReport) string {
    var sb strings.Builder
    dr.writeSymbolReport(&sb, report)
    return sb.String()
}

// writeSymbolReport 写入单个交易对的报告
func (dr *DatabaseReporter) writeSymbolReport(sb *strings.Builder, report *DatabaseVerificationReport) {
	// 确定状态图标和颜色
	var statusIcon string
	if report.QualityScore >= 95.0 {
		statusIcon = "🟢"
	} else if report.QualityScore >= 85.0 {
		statusIcon = "🟡"
	} else if report.QualityScore >= 70.0 {
		statusIcon = "🟠"
	} else {
		statusIcon = "🔴"
	}
	
	qualityGrade := GetQualityGrade(report.QualityScore)
	
	sb.WriteString(fmt.Sprintf("%s %s: %.2f%% (%s)\n", 
		statusIcon, report.Symbol, report.QualityScore, qualityGrade))
	
	// 数据范围信息
	if report.DataRange.HasData {
		sb.WriteString(fmt.Sprintf("  数据范围: %s - %s (%d月份)\n", 
			report.DataRange.EarliestDate.Format("2006-01"), 
			report.DataRange.LatestDate.Format("2006-01"), 
			report.DataRange.TotalMonths))
		
		// 缺失月份信息
		if len(report.MissingMonths) > 0 {
			sb.WriteString(fmt.Sprintf("  缺失月份: %s\n", strings.Join(report.MissingMonths, ", ")))
		}
		
		// 不完整月份信息
		if len(report.IncompleteMonths) > 0 {
			sb.WriteString(fmt.Sprintf("  不完整月份: %s\n", strings.Join(report.IncompleteMonths, ", ")))
		}
		
		// 月度统计摘要
		if len(report.MonthlyReports) > 0 {
			completeCount := 0
			partialCount := 0
			missingCount := 0
			
			for _, monthly := range report.MonthlyReports {
				switch monthly.Status {
				case CompletenessStatusComplete:
					completeCount++
				case CompletenessStatusPartial:
					partialCount++
				case CompletenessStatusMissing:
					missingCount++
				}
			}
			
			sb.WriteString(fmt.Sprintf("  月度统计: 完整 %d, 部分 %d, 缺失 %d\n", 
				completeCount, partialCount, missingCount))
		}
	} else {
		sb.WriteString("  数据范围: 无数据\n")
	}
	
	sb.WriteString("\n")
}

// GenerateCSVReport 生成CSV格式报告
func (dr *DatabaseReporter) GenerateCSVReport(reports []*DatabaseVerificationReport) string {
	var sb strings.Builder
	
	// CSV头部
	sb.WriteString("Symbol,HasData,EarliestDate,LatestDate,TotalMonths,OverallCompleteness,QualityScore,MissingMonths,IncompleteMonths\n")
	
	// 数据行
	for _, report := range reports {
		earliestDate := ""
		latestDate := ""
		if report.DataRange.HasData {
			earliestDate = report.DataRange.EarliestDate.Format("2006-01-02")
			latestDate = report.DataRange.LatestDate.Format("2006-01-02")
		}
		
		missingMonths := strings.Join(report.MissingMonths, ";")
		incompleteMonths := strings.Join(report.IncompleteMonths, ";")
		
		sb.WriteString(fmt.Sprintf("%s,%t,%s,%s,%d,%.2f,%.2f,\"%s\",\"%s\"\n",
			report.Symbol,
			report.DataRange.HasData,
			earliestDate,
			latestDate,
			report.DataRange.TotalMonths,
			report.OverallCompleteness,
			report.QualityScore,
			missingMonths,
			incompleteMonths))
	}
	
	return sb.String()
}

// GenerateMarkdownReport 生成Markdown格式报告
func (dr *DatabaseReporter) GenerateMarkdownReport(report *BatchDatabaseVerificationReport) string {
	var sb strings.Builder
	
	// 标题
	sb.WriteString("# 数据完整性验证报告\n\n")
	
	// 概览
	sb.WriteString("## 验证概览\n\n")
	sb.WriteString(fmt.Sprintf("- **总交易对**: %d\n", report.TotalSymbols))
	sb.WriteString(fmt.Sprintf("- **已验证**: %d\n", report.VerifiedSymbols))
	sb.WriteString(fmt.Sprintf("- **平均完整性**: %.2f%%\n", report.AverageCompleteness))
	sb.WriteString(fmt.Sprintf("- **生成时间**: %s\n\n", report.GeneratedAt.Format("2006-01-02 15:04:05")))
	
	// 统计摘要
	if report.Summary != nil {
		sb.WriteString("## 完整性统计\n\n")
		sb.WriteString("| 状态 | 月份数 | 百分比 |\n")
		sb.WriteString("|------|--------|--------|\n")
		
		total := report.Summary.TotalMonths
		if total > 0 {
			sb.WriteString(fmt.Sprintf("| 完整 (95%%+) | %d | %.1f%% |\n", 
				report.Summary.CompleteMonths, 
				float64(report.Summary.CompleteMonths)/float64(total)*100))
			sb.WriteString(fmt.Sprintf("| 部分 (0-95%%) | %d | %.1f%% |\n", 
				report.Summary.PartialMonths, 
				float64(report.Summary.PartialMonths)/float64(total)*100))
			sb.WriteString(fmt.Sprintf("| 缺失 (0%%) | %d | %.1f%% |\n", 
				report.Summary.MissingMonths, 
				float64(report.Summary.MissingMonths)/float64(total)*100))
		}
		sb.WriteString("\n")
	}
	
	// 详细结果
	sb.WriteString("## 详细结果\n\n")
	sb.WriteString("| 交易对 | 完整性 | 数据范围 | 缺失月份 | 不完整月份 |\n")
	sb.WriteString("|--------|--------|----------|----------|------------|\n")
	
	for _, symbolReport := range report.Reports {
		dataRange := "无数据"
		if symbolReport.DataRange.HasData {
			dataRange = fmt.Sprintf("%s ~ %s", 
				symbolReport.DataRange.EarliestDate.Format("2006-01"), 
				symbolReport.DataRange.LatestDate.Format("2006-01"))
		}
		
		missingMonths := strings.Join(symbolReport.MissingMonths, ", ")
		if missingMonths == "" {
			missingMonths = "无"
		}
		
		incompleteMonths := strings.Join(symbolReport.IncompleteMonths, ", ")
		if incompleteMonths == "" {
			incompleteMonths = "无"
		}
		
		sb.WriteString(fmt.Sprintf("| %s | %.2f%% | %s | %s | %s |\n",
			symbolReport.Symbol,
			symbolReport.QualityScore,
			dataRange,
			missingMonths,
			incompleteMonths))
	}
	
	return sb.String()
}

// GenerateSummaryTable 生成摘要表格
func (dr *DatabaseReporter) GenerateSummaryTable(reports []*DatabaseVerificationReport) string {
	var sb strings.Builder
	
	// 表格头部
	sb.WriteString(fmt.Sprintf("%-15s %-12s %-20s %-10s %-15s\n", 
		"交易对", "完整性", "数据范围", "总月份", "状态"))
	sb.WriteString(strings.Repeat("-", 80) + "\n")
	
	// 表格数据
	for _, report := range reports {
		dataRange := "无数据"
		if report.DataRange.HasData {
			dataRange = fmt.Sprintf("%s~%s", 
				report.DataRange.EarliestDate.Format("06-01"), 
				report.DataRange.LatestDate.Format("06-01"))
		}
		
		qualityGrade := GetQualityGrade(report.QualityScore)
		
		sb.WriteString(fmt.Sprintf("%-15s %-11.2f%% %-20s %-10d %-15s\n",
			report.Symbol,
			report.QualityScore,
			dataRange,
			report.DataRange.TotalMonths,
			qualityGrade))
	}
	
	return sb.String()
}
