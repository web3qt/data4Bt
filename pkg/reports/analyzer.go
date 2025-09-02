package reports

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"binance-data-loader/internal/domain"
)

// VerificationAnalyzer 验证结果分析器
type VerificationAnalyzer struct {
	thresholds QualityThresholds
}

// NewVerificationAnalyzer 创建新的验证分析器
func NewVerificationAnalyzer() *VerificationAnalyzer {
	return &VerificationAnalyzer{
		thresholds: DefaultQualityThresholds,
	}
}

// AnalyzeResults 分析验证结果并生成报告
func (va *VerificationAnalyzer) AnalyzeResults(
	verificationResults map[string]*domain.DataCompletenessStats,
	executionTime time.Duration,
) *VerificationSummaryReport {
	
	startTime := time.Now()
	totalSymbols := len(verificationResults)
	
	// 分类问题
	var criticalIssues []SymbolIssue
	var needsAttention []SymbolIssue
	var goodCondition []SymbolIssue
	var excellent []SymbolIssue
	
	// 收集统计数据
	var totalCompleteness float64
	var completenessValues []float64
	totalMonthsAnalyzed := 0
	totalCompletedMonths := 0
	totalPartialMonths := 0
	totalMissingMonths := 0
	
	earliestDate := ""
	latestDate := ""
	
	// 分析每个交易对
	for symbol, stats := range verificationResults {
		issue := va.analyzeSymbol(symbol, stats)
		
		// 分类
		level := va.thresholds.GetQualityLevel(issue.CompletenessRatio)
		switch level {
		case "excellent":
			excellent = append(excellent, issue)
		case "good":
			goodCondition = append(goodCondition, issue)
		case "acceptable":
			needsAttention = append(needsAttention, issue)
		case "poor":
			criticalIssues = append(criticalIssues, issue)
		}
		
		// 收集统计数据
		totalCompleteness += issue.CompletenessRatio
		completenessValues = append(completenessValues, issue.CompletenessRatio)
		totalMonthsAnalyzed += issue.TotalMonths
		totalCompletedMonths += issue.CompleteMonths
		totalPartialMonths += issue.PartialMonths
		totalMissingMonths += len(issue.MissingMonths)
		
		// 更新时间范围
		if earliestDate == "" || (issue.DataRange != "" && strings.Split(issue.DataRange, " - ")[0] < earliestDate) {
			if issue.DataRange != "" {
				earliestDate = strings.Split(issue.DataRange, " - ")[0]
			}
		}
		if latestDate == "" || (issue.DataRange != "" && strings.Split(issue.DataRange, " - ")[1] > latestDate) {
			if issue.DataRange != "" {
				latestDate = strings.Split(issue.DataRange, " - ")[1]
			}
		}
	}
	
	// 排序（按完整性从低到高）
	sort.Slice(criticalIssues, func(i, j int) bool {
		return criticalIssues[i].CompletenessRatio < criticalIssues[j].CompletenessRatio
	})
	sort.Slice(needsAttention, func(i, j int) bool {
		return needsAttention[i].CompletenessRatio < needsAttention[j].CompletenessRatio
	})
	sort.Slice(goodCondition, func(i, j int) bool {
		return goodCondition[i].CompletenessRatio < goodCondition[j].CompletenessRatio
	})
	sort.Slice(excellent, func(i, j int) bool {
		return excellent[i].CompletenessRatio < excellent[j].CompletenessRatio
	})
	
	// 计算统计信息
	averageCompleteness := 0.0
	if totalSymbols > 0 {
		averageCompleteness = totalCompleteness / float64(totalSymbols)
	}
	
	medianCompleteness := va.calculateMedian(completenessValues)
	
	// 计算覆盖年数
	coverageYears := 0
	if earliestDate != "" && latestDate != "" {
		startYear := va.extractYear(earliestDate)
		endYear := va.extractYear(latestDate)
		if startYear > 0 && endYear > 0 {
			coverageYears = endYear - startYear + 1
		}
	}
	
	// 分析问题模式
	issuePatterns := va.analyzeIssuePatterns(verificationResults)
	
	// 生成报告
	report := &VerificationSummaryReport{
		GeneratedAt:     startTime,
		TotalSymbols:    totalSymbols,
		VerifiedSymbols: totalSymbols,
		ExecutionTime:   executionTime,
		
		CriticalIssues:  criticalIssues,
		NeedsAttention:  needsAttention,
		GoodCondition:   goodCondition,
		Excellent:       excellent,
		
		Statistics: ReportStatistics{
			AverageCompleteness:  averageCompleteness,
			MedianCompleteness:   medianCompleteness,
			CriticalCount:        len(criticalIssues),
			NeedsAttentionCount:  len(needsAttention),
			GoodConditionCount:   len(goodCondition),
			ExcellentCount:       len(excellent),
			TotalMonthsAnalyzed:  totalMonthsAnalyzed,
			CompletedMonths:      totalCompletedMonths,
			PartialMonths:        totalPartialMonths,
			MissingMonths:        totalMissingMonths,
			EarliestDataDate:     earliestDate,
			LatestDataDate:       latestDate,
			CoverageYears:        coverageYears,
			CommonIssuePatterns:  issuePatterns,
		},
		
		ReportConfig: ReportConfig{
			Format:         "markdown",
			IncludeDetails: true,
			SortBy:         "completeness",
		},
	}
	
	return report
}

// analyzeSymbol 分析单个交易对
func (va *VerificationAnalyzer) analyzeSymbol(symbol string, stats *domain.DataCompletenessStats) SymbolIssue {
	completeness := stats.CompletenessRatio
	level := va.thresholds.GetQualityLevel(completeness)
	priority := va.thresholds.GetPriority(completeness)
	
	// 计算月份统计
	totalMonths := len(stats.MonthlyStats)
	completeMonths := 0
	partialMonths := 0
	var missingMonths []string
	var partialMonthsList []string
	
	for month, monthStats := range stats.MonthlyStats {
		if !monthStats.HasData {
			missingMonths = append(missingMonths, month)
		} else if monthStats.CompletenessRatio < va.thresholds.ExcellentMin {
			partialMonths++
			if monthStats.CompletenessRatio < va.thresholds.AcceptableMin {
				partialMonthsList = append(partialMonthsList, fmt.Sprintf("%s (%.1f%%)", month, monthStats.CompletenessRatio))
			}
		} else {
			completeMonths++
		}
	}
	
	// 排序月份列表
	sort.Strings(missingMonths)
	sort.Strings(partialMonthsList)
	
	// 生成数据范围
	dataRange := ""
	if !stats.FirstRecord.IsZero() && !stats.LastRecord.IsZero() {
		dataRange = fmt.Sprintf("%s - %s", 
			stats.FirstRecord.Format("2006-01"), 
			stats.LastRecord.Format("2006-01"))
	}
	
	// 生成建议
	recommendations := va.generateRecommendations(level, len(missingMonths), len(partialMonthsList))
	
	return SymbolIssue{
		Symbol:              symbol,
		CompletenessRatio:   completeness,
		DataRange:           dataRange,
		TotalMonths:         totalMonths,
		CompleteMonths:      completeMonths,
		PartialMonths:       partialMonths,
		MissingMonths:       missingMonths,
		PartialMonthsList:   partialMonthsList,
		Recommendations:     recommendations,
		Priority:            priority,
		QualityLevel:        level,
	}
}

// generateRecommendations 生成修复建议
func (va *VerificationAnalyzer) generateRecommendations(level string, missingCount, partialCount int) []string {
	var recommendations []string
	
	switch level {
	case "poor":
		recommendations = append(recommendations, "🚨 高优先级：立即检查数据下载和导入流程")
		if missingCount > 0 {
			recommendations = append(recommendations, fmt.Sprintf("📥 重新下载 %d 个缺失月份的数据", missingCount))
		}
		if partialCount > 0 {
			recommendations = append(recommendations, fmt.Sprintf("🔄 修复 %d 个不完整月份的数据", partialCount))
		}
		recommendations = append(recommendations, "🔍 检查网络连接和币安数据源可用性")
		
	case "acceptable":
		recommendations = append(recommendations, "⚠️ 中优先级：建议在合适时机进行数据修复")
		if partialCount > 0 {
			recommendations = append(recommendations, "🔄 补全不完整的月份数据")
		}
		recommendations = append(recommendations, "📊 定期监控数据质量变化")
		
	case "good":
		recommendations = append(recommendations, "✅ 数据质量良好，建议定期维护")
		if partialCount > 0 {
			recommendations = append(recommendations, "🔧 可在低峰时段优化不完整的月份")
		}
		
	case "excellent":
		recommendations = append(recommendations, "🎉 数据完整性优秀，继续保持")
		recommendations = append(recommendations, "📈 可作为其他交易对的参考标准")
	}
	
	return recommendations
}

// calculateMedian 计算中位数
func (va *VerificationAnalyzer) calculateMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	
	sort.Float64s(values)
	n := len(values)
	
	if n%2 == 0 {
		return (values[n/2-1] + values[n/2]) / 2
	}
	return values[n/2]
}

// extractYear 从日期字符串提取年份
func (va *VerificationAnalyzer) extractYear(dateStr string) int {
	if len(dateStr) >= 4 {
		year := 0
		fmt.Sscanf(dateStr[:4], "%d", &year)
		return year
	}
	return 0
}

// analyzeIssuePatterns 分析问题模式
func (va *VerificationAnalyzer) analyzeIssuePatterns(results map[string]*domain.DataCompletenessStats) []IssuePattern {
	var patterns []IssuePattern
	
	// 分析缺失数据的时间模式
	missingMonthsCount := make(map[string]int)
	var lowCompletenessSymbols []string
	
	for symbol, stats := range results {
		if stats.CompletenessRatio < 50.0 {
			lowCompletenessSymbols = append(lowCompletenessSymbols, symbol)
		}
		
		for month, monthStats := range stats.MonthlyStats {
			if !monthStats.HasData {
				missingMonthsCount[month]++
			}
		}
	}
	
	// 识别常见的缺失月份
	for month, count := range missingMonthsCount {
		if count >= 5 { // 如果至少5个交易对在同一月份缺失数据
			patterns = append(patterns, IssuePattern{
				Pattern:     "common_missing_month",
				Description: fmt.Sprintf("%s 月份多个交易对缺失数据", month),
				Frequency:   count,
			})
		}
	}
	
	// 识别整体质量过低的模式
	if len(lowCompletenessSymbols) > 0 {
		patterns = append(patterns, IssuePattern{
			Pattern:         "low_completeness_batch",
			Description:     "批量交易对完整性过低（<50%）",
			AffectedSymbols: lowCompletenessSymbols,
			Frequency:       len(lowCompletenessSymbols),
		})
	}
	
	return patterns
}