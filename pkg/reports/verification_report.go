package reports

import (
	"time"
)

// VerificationSummaryReport 验证汇总报告
type VerificationSummaryReport struct {
	GeneratedAt      time.Time              `json:"generated_at"`
	TotalSymbols     int                    `json:"total_symbols"`
	VerifiedSymbols  int                    `json:"verified_symbols"`
	ExecutionTime    time.Duration          `json:"execution_time"`
	
	// 按问题严重程度分类
	CriticalIssues   []SymbolIssue          `json:"critical_issues"`    // < 60%
	NeedsAttention   []SymbolIssue          `json:"needs_attention"`    // 60-80%
	GoodCondition    []SymbolIssue          `json:"good_condition"`     // 80-95%
	Excellent        []SymbolIssue          `json:"excellent"`          // >= 95%
	
	// 统计信息
	Statistics       ReportStatistics       `json:"statistics"`
	
	// 报告配置
	ReportConfig     ReportConfig           `json:"report_config"`
}

// SymbolIssue 交易对问题详情
type SymbolIssue struct {
	Symbol              string    `json:"symbol"`
	CompletenessRatio   float64   `json:"completeness_ratio"`
	DataRange           string    `json:"data_range"`
	TotalMonths         int       `json:"total_months"`
	CompleteMonths      int       `json:"complete_months"`
	PartialMonths       int       `json:"partial_months"`
	MissingMonths       []string  `json:"missing_months"`
	PartialMonthsList   []string  `json:"partial_months_list"`
	Recommendations     []string  `json:"recommendations"`
	Priority            string    `json:"priority"`           // high, medium, low
	QualityLevel        string    `json:"quality_level"`      // excellent, good, acceptable, poor
}

// ReportStatistics 报告统计信息
type ReportStatistics struct {
	// 总体统计
	AverageCompleteness  float64            `json:"average_completeness"`
	MedianCompleteness   float64            `json:"median_completeness"`
	
	// 问题分布统计
	CriticalCount        int                `json:"critical_count"`
	NeedsAttentionCount  int                `json:"needs_attention_count"`
	GoodConditionCount   int                `json:"good_condition_count"`
	ExcellentCount       int                `json:"excellent_count"`
	
	// 月份统计
	TotalMonthsAnalyzed  int                `json:"total_months_analyzed"`
	CompletedMonths      int                `json:"completed_months"`
	PartialMonths        int                `json:"partial_months"`
	MissingMonths        int                `json:"missing_months"`
	
	// 时间范围分析
	EarliestDataDate     string             `json:"earliest_data_date"`
	LatestDataDate       string             `json:"latest_data_date"`
	CoverageYears        int                `json:"coverage_years"`
	
	// 问题模式分析
	CommonIssuePatterns  []IssuePattern     `json:"common_issue_patterns"`
}

// IssuePattern 问题模式
type IssuePattern struct {
	Pattern      string   `json:"pattern"`
	Description  string   `json:"description"`
	AffectedSymbols []string `json:"affected_symbols"`
	Frequency    int      `json:"frequency"`
}

// ReportConfig 报告配置
type ReportConfig struct {
	OutputPath          string    `json:"output_path"`
	Format              string    `json:"format"`            // markdown, html, json
	IncludeDetails      bool      `json:"include_details"`
	SortBy              string    `json:"sort_by"`           // completeness, symbol, priority
	FilterMinCompleteness float64 `json:"filter_min_completeness"`
	MaxIssuesPerCategory int     `json:"max_issues_per_category"`
}

// QualityThresholds 质量阈值配置
type QualityThresholds struct {
	ExcellentMin    float64  `json:"excellent_min"`     // >= 95%
	GoodMin         float64  `json:"good_min"`          // >= 80%
	AcceptableMin   float64  `json:"acceptable_min"`    // >= 60%
	// 低于 AcceptableMin 的为 Poor
}

// DefaultQualityThresholds 默认质量阈值
var DefaultQualityThresholds = QualityThresholds{
	ExcellentMin:  95.0,
	GoodMin:      80.0,
	AcceptableMin: 60.0,
}

// GetQualityLevel 获取质量等级
func (t QualityThresholds) GetQualityLevel(completeness float64) string {
	if completeness >= t.ExcellentMin {
		return "excellent"
	} else if completeness >= t.GoodMin {
		return "good"
	} else if completeness >= t.AcceptableMin {
		return "acceptable"
	}
	return "poor"
}

// GetPriority 获取优先级
func (t QualityThresholds) GetPriority(completeness float64) string {
	if completeness < t.AcceptableMin {
		return "high"
	} else if completeness < t.GoodMin {
		return "medium"
	}
	return "low"
}

// GetQualityEmoji 获取质量等级emoji
func GetQualityEmoji(level string) string {
	switch level {
	case "excellent":
		return "✅"
	case "good":
		return "🟢"
	case "acceptable":
		return "🟡"
	case "poor":
		return "🔴"
	default:
		return "❓"
	}
}

// GetPriorityEmoji 获取优先级emoji
func GetPriorityEmoji(priority string) string {
	switch priority {
	case "high":
		return "🚨"
	case "medium":
		return "⚠️"
	case "low":
		return "ℹ️"
	default:
		return "📋"
	}
}