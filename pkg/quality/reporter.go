package quality

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Reporter 数据质量报告生成器
type Reporter struct {
	threshold QualityThreshold
}

// NewReporter 创建报告生成器
func NewReporter() *Reporter {
	return &Reporter{
		threshold: DefaultQualityThreshold,
	}
}

// GenerateConsoleReport 生成控制台报告
func (r *Reporter) GenerateConsoleReport(report *DataQualityReport) string {
	var sb strings.Builder
	
	// 报告头部
	sb.WriteString(fmt.Sprintf("=== 数据质量检查报告 ===\n"))
	sb.WriteString(fmt.Sprintf("代币: %s\n", report.Symbol))
	sb.WriteString(fmt.Sprintf("整体完整性评分: %.2f%%\n", report.OverallScore))
	sb.WriteString(fmt.Sprintf("质量等级: %s\n", GetQualityLevel(report.OverallScore).Description()))
	
	if report.Statistics != nil {
		sb.WriteString(fmt.Sprintf("数据覆盖期间: %s 到 %s\n", 
			report.Statistics.AvailableFrom, report.Statistics.AvailableTo))
		sb.WriteString(fmt.Sprintf("总月份: %d, 完整: %d, 部分: %d, 缺失: %d\n",
			report.Statistics.TotalMonths,
			report.Statistics.CompleteMonths,
			report.Statistics.PartialMonths,
			report.Statistics.MissingMonths))
	}
	
	sb.WriteString(fmt.Sprintf("检查时间: %s\n\n", 
		report.LastChecked.Format("2006-01-02 15:04:05")))

	// 月度数据状态
	if len(report.MonthlyStatus) > 0 {
		sb.WriteString("月度数据状态:\n")
		for _, status := range report.MonthlyStatus {
			icon := r.getStatusIcon(status)
			sb.WriteString(fmt.Sprintf("%s %s: ", icon, status.Month))
			
			if !status.HasData {
				sb.WriteString("缺失\n")
			} else if len(status.DataQualityIssues) == 0 && status.CompletenessRatio >= r.threshold.ExcellentThreshold {
				sb.WriteString(fmt.Sprintf("完整 (%d/%d 记录)\n", 
					status.ActualRecords, status.ExpectedRecords))
			} else {
				sb.WriteString(fmt.Sprintf("不完整 (%d/%d 记录, %.1f%%)\n", 
					status.ActualRecords, status.ExpectedRecords, status.CompletenessRatio))
				if len(status.DataQualityIssues) > 0 {
					sb.WriteString(fmt.Sprintf("    问题: %s\n", 
						strings.Join(status.DataQualityIssues, ", ")))
				}
			}
		}
	}

	// 缺失月份汇总
	if len(report.MissingMonths) > 0 {
		sb.WriteString(fmt.Sprintf("\n缺失月份: %s\n", strings.Join(report.MissingMonths, ", ")))
	}

	// 部分缺失月份汇总
	if len(report.PartialMonths) > 0 {
		sb.WriteString(fmt.Sprintf("不完整月份: %s\n", strings.Join(report.PartialMonths, ", ")))
	}

	// 数据质量问题汇总
	if len(report.MissingMonths) > 0 || len(report.PartialMonths) > 0 {
		sb.WriteString(fmt.Sprintf("数据质量问题: 发现%d个月份数据缺失，%d个月份数据不完整\n", 
			len(report.MissingMonths), len(report.PartialMonths)))
	} else {
		sb.WriteString("数据质量: 所有月份数据完整\n")
	}

	return sb.String()
}

// GenerateBatchConsoleReport 生成批量检查的控制台报告
func (r *Reporter) GenerateBatchConsoleReport(batchReport *BatchQualityReport) string {
	var sb strings.Builder

	// 批量报告头部
	sb.WriteString(fmt.Sprintf("=== 批量数据质量检查报告 ===\n"))
	sb.WriteString(fmt.Sprintf("检查模式: %s\n", batchReport.CheckMode.String()))
	sb.WriteString(fmt.Sprintf("总交易对: %d, 已检查: %d\n", 
		batchReport.TotalSymbols, batchReport.CheckedSymbols))
	sb.WriteString(fmt.Sprintf("检查耗时: %v\n", batchReport.CheckDuration))
	sb.WriteString(fmt.Sprintf("生成时间: %s\n\n", 
		batchReport.GeneratedAt.Format("2006-01-02 15:04:05")))

	// 质量分布汇总
	if batchReport.Summary != nil {
		sb.WriteString("质量等级分布:\n")
		sb.WriteString(fmt.Sprintf("  %s: %d 个交易对\n", 
			QualityLevelExcellent.Description(), batchReport.Summary.ExcellentCount))
		sb.WriteString(fmt.Sprintf("  %s: %d 个交易对\n", 
			QualityLevelGood.Description(), batchReport.Summary.GoodCount))
		sb.WriteString(fmt.Sprintf("  %s: %d 个交易对\n", 
			QualityLevelAcceptable.Description(), batchReport.Summary.AcceptableCount))
		sb.WriteString(fmt.Sprintf("  %s: %d 个交易对\n", 
			QualityLevelPoor.Description(), batchReport.Summary.PoorCount))
		sb.WriteString(fmt.Sprintf("平均质量评分: %.2f%%\n", batchReport.Summary.AverageScore))
		sb.WriteString(fmt.Sprintf("总缺失月份: %d, 总不完整月份: %d\n\n", 
			batchReport.Summary.TotalMissingMonths, batchReport.Summary.TotalPartialMonths))
	}

	// 各交易对详细情况
	sb.WriteString("各交易对质量状况:\n")
	sb.WriteString(strings.Repeat("=", 80) + "\n")

	for i, report := range batchReport.Reports {
		if i > 0 {
			sb.WriteString(strings.Repeat("-", 40) + "\n")
		}
		
		level := GetQualityLevel(report.OverallScore)
		icon := r.getQualityLevelIcon(level)
		
		sb.WriteString(fmt.Sprintf("%s %s: %.2f%% (%s)\n", 
			icon, report.Symbol, report.OverallScore, level.Description()))
		
		if len(report.MissingMonths) > 0 {
			sb.WriteString(fmt.Sprintf("  缺失月份(%d): %s\n", 
				len(report.MissingMonths), 
				r.formatMonthList(report.MissingMonths, 8)))
		}
		
		if len(report.PartialMonths) > 0 {
			sb.WriteString(fmt.Sprintf("  不完整月份(%d): %s\n", 
				len(report.PartialMonths), 
				r.formatMonthList(report.PartialMonths, 8)))
		}
		
		if report.Statistics != nil {
			sb.WriteString(fmt.Sprintf("  数据范围: %s - %s (%d月份)\n",
				report.Statistics.AvailableFrom,
				report.Statistics.AvailableTo,
				report.Statistics.TotalMonths))
		}
	}

	return sb.String()
}

// WriteJSONReport 写入JSON格式报告
func (r *Reporter) WriteJSONReport(writer io.Writer, report interface{}) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

// GenerateSummaryTable 生成质量汇总表格
func (r *Reporter) GenerateSummaryTable(reports []*DataQualityReport) string {
	if len(reports) == 0 {
		return "无数据"
	}

	var sb strings.Builder
	
	// 表头
	sb.WriteString(fmt.Sprintf("%-12s %-8s %-6s %-6s %-6s %-15s\n", 
		"交易对", "评分", "缺失", "不完整", "总月", "数据范围"))
	sb.WriteString(strings.Repeat("-", 70) + "\n")
	
	// 数据行
	for _, report := range reports {
		level := GetQualityLevel(report.OverallScore)
		icon := r.getQualityLevelIcon(level)
		
		dataRange := "-"
		if report.Statistics != nil && report.Statistics.TotalMonths > 0 {
			dataRange = fmt.Sprintf("%s~%s", 
				report.Statistics.AvailableFrom[2:], // 去掉年份前两位
				report.Statistics.AvailableTo[2:])
		}
		
		sb.WriteString(fmt.Sprintf("%-12s %s%-6.1f %6d %6d %6d %-15s\n",
			report.Symbol,
			icon,
			report.OverallScore,
			len(report.MissingMonths),
			len(report.PartialMonths),
			report.Statistics.TotalMonths,
			dataRange))
	}
	
	return sb.String()
}

// getStatusIcon 获取状态图标
func (r *Reporter) getStatusIcon(status *MonthlyDataStatus) string {
	if !status.HasData {
		return "❌"
	} else if len(status.DataQualityIssues) == 0 && status.CompletenessRatio >= r.threshold.ExcellentThreshold {
		return "✅"
	} else {
		return "⚠️ "
	}
}

// getQualityLevelIcon 获取质量等级图标
func (r *Reporter) getQualityLevelIcon(level QualityLevel) string {
	switch level {
	case QualityLevelExcellent:
		return "✅"
	case QualityLevelGood:
		return "🟢"
	case QualityLevelAcceptable:
		return "🟡"
	case QualityLevelPoor:
		return "🔴"
	default:
		return "❓"
	}
}

// formatMonthList 格式化月份列表，限制每行显示的月份数量
func (r *Reporter) formatMonthList(months []string, maxPerLine int) string {
	if len(months) == 0 {
		return ""
	}
	
	if len(months) <= maxPerLine {
		return strings.Join(months, ", ")
	}
	
	var parts []string
	for i := 0; i < len(months); i += maxPerLine {
		end := i + maxPerLine
		if end > len(months) {
			end = len(months)
		}
		parts = append(parts, strings.Join(months[i:end], ", "))
	}
	
	return strings.Join(parts, ",\n    ")
}

// GenerateMarkdownReport 生成Markdown格式报告
func (r *Reporter) GenerateMarkdownReport(batchReport *BatchQualityReport) string {
	var sb strings.Builder
	
	// 标题
	sb.WriteString("# 数据质量检查报告\n\n")
	sb.WriteString(fmt.Sprintf("**生成时间**: %s  \n", 
		batchReport.GeneratedAt.Format("2006-01-02 15:04:05")))
	sb.WriteString(fmt.Sprintf("**检查耗时**: %v  \n", batchReport.CheckDuration))
	sb.WriteString(fmt.Sprintf("**总交易对**: %d，**已检查**: %d\n\n", 
		batchReport.TotalSymbols, batchReport.CheckedSymbols))
	
	// 质量等级分布
	if batchReport.Summary != nil {
		sb.WriteString("## 质量等级分布\n\n")
		sb.WriteString("| 等级 | 数量 | 占比 |\n")
		sb.WriteString("|------|------|------|\n")
		
		total := float64(batchReport.CheckedSymbols)
		if total > 0 {
			sb.WriteString(fmt.Sprintf("| %s | %d | %.1f%% |\n", 
				QualityLevelExcellent.Description(), batchReport.Summary.ExcellentCount,
				float64(batchReport.Summary.ExcellentCount)/total*100))
			sb.WriteString(fmt.Sprintf("| %s | %d | %.1f%% |\n", 
				QualityLevelGood.Description(), batchReport.Summary.GoodCount,
				float64(batchReport.Summary.GoodCount)/total*100))
			sb.WriteString(fmt.Sprintf("| %s | %d | %.1f%% |\n", 
				QualityLevelAcceptable.Description(), batchReport.Summary.AcceptableCount,
				float64(batchReport.Summary.AcceptableCount)/total*100))
			sb.WriteString(fmt.Sprintf("| %s | %d | %.1f%% |\n", 
				QualityLevelPoor.Description(), batchReport.Summary.PoorCount,
				float64(batchReport.Summary.PoorCount)/total*100))
		}
		
		sb.WriteString(fmt.Sprintf("\n**平均质量评分**: %.2f%%\n\n", batchReport.Summary.AverageScore))
	}
	
	// 详细交易对列表
	sb.WriteString("## 交易对详细信息\n\n")
	sb.WriteString("| 交易对 | 评分 | 等级 | 缺失月份 | 不完整月份 | 数据范围 |\n")
	sb.WriteString("|--------|------|------|----------|------------|----------|\n")
	
	for _, report := range batchReport.Reports {
		level := GetQualityLevel(report.OverallScore)
		dataRange := "-"
		if report.Statistics != nil && report.Statistics.TotalMonths > 0 {
			dataRange = fmt.Sprintf("%s ~ %s", 
				report.Statistics.AvailableFrom, report.Statistics.AvailableTo)
		}
		
		missingCount := len(report.MissingMonths)
		partialCount := len(report.PartialMonths)
		
		sb.WriteString(fmt.Sprintf("| %s | %.2f%% | %s | %d | %d | %s |\n",
			report.Symbol, report.OverallScore, level.Description(),
			missingCount, partialCount, dataRange))
	}
	
	return sb.String()
}

// GenerateCSVReport 生成CSV格式报告
func (r *Reporter) GenerateCSVReport(reports []*DataQualityReport) string {
	var sb strings.Builder
	
	// CSV头部
	sb.WriteString("Symbol,Score,Level,MissingMonths,PartialMonths,TotalMonths,AvailableFrom,AvailableTo,TotalRecords,ExpectedRecords,CompletenessRatio\n")
	
	// 数据行
	for _, report := range reports {
		level := GetQualityLevel(report.OverallScore)
		
		// 处理统计信息
		availableFrom := ""
		availableTo := ""
		totalMonths := 0
		totalRecords := int64(0)
		expectedRecords := int64(0)
		completenessRatio := 0.0
		
		if report.Statistics != nil {
			availableFrom = report.Statistics.AvailableFrom
			availableTo = report.Statistics.AvailableTo
			totalMonths = report.Statistics.TotalMonths
			totalRecords = report.Statistics.TotalRecords
			expectedRecords = report.Statistics.ExpectedRecords
			completenessRatio = report.Statistics.CompletenessRatio
		}
		
		sb.WriteString(fmt.Sprintf("%s,%.2f,%s,%d,%d,%d,%s,%s,%d,%d,%.2f\n",
			report.Symbol,
			report.OverallScore,
			level.String(),
			len(report.MissingMonths),
			len(report.PartialMonths),
			totalMonths,
			availableFrom,
			availableTo,
			totalRecords,
			expectedRecords,
			completenessRatio))
	}
	
	return sb.String()
}