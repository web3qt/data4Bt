package reports

import (
	"fmt"
	"path/filepath"
	"time"

	"binance-data-loader/internal/domain"
)

// ReportGenerator 报告生成器接口
type ReportGenerator interface {
	GenerateVerificationReport(
		results map[string]*domain.DataCompletenessStats,
		executionTime time.Duration,
		outputPath string,
	) (string, error)
}

// DefaultReportGenerator 默认报告生成器
type DefaultReportGenerator struct {
	analyzer  *VerificationAnalyzer
	generator *MarkdownGenerator
}

// NewReportGenerator 创建新的报告生成器
func NewReportGenerator() *DefaultReportGenerator {
	return &DefaultReportGenerator{
		analyzer:  NewVerificationAnalyzer(),
		generator: NewMarkdownGenerator(),
	}
}

// GenerateVerificationReport 生成验证报告
func (rg *DefaultReportGenerator) GenerateVerificationReport(
	results map[string]*domain.DataCompletenessStats,
	executionTime time.Duration,
	outputPath string,
) (string, error) {
	
	// 如果没有指定输出路径，生成默认路径
	if outputPath == "" {
		timestamp := time.Now().Format("20060102-150405")
		outputPath = fmt.Sprintf("reports/data-completeness-report-%s.md", timestamp)
	}

	// 分析验证结果
	report := rg.analyzer.AnalyzeResults(results, executionTime)

	// 生成Markdown报告
	if err := rg.generator.GenerateReport(report, outputPath); err != nil {
		return "", fmt.Errorf("failed to generate markdown report: %w", err)
	}

	return outputPath, nil
}

// GenerateDefaultReportPath 生成默认报告路径
func GenerateDefaultReportPath() string {
	timestamp := time.Now().Format("20060102-150405")
	return filepath.Join("reports", fmt.Sprintf("data-completeness-report-%s.md", timestamp))
}

// ValidateOutputPath 验证输出路径
func ValidateOutputPath(path string) error {
	if path == "" {
		return fmt.Errorf("output path cannot be empty")
	}

	// 确保是.md文件
	if filepath.Ext(path) != ".md" {
		return fmt.Errorf("output file must have .md extension")
	}

	return nil
}

// FormatReportSummary 格式化报告摘要信息
func FormatReportSummary(report *VerificationSummaryReport) string {
	return fmt.Sprintf(
		"验证了 %d 个交易对，耗时 %s\n🔴 %d个严重问题，🟡 %d个需关注，🟢 %d个良好，✅ %d个优秀",
		report.TotalSymbols,
		FormatDurationSimple(report.ExecutionTime),
		report.Statistics.CriticalCount,
		report.Statistics.NeedsAttentionCount,
		report.Statistics.GoodConditionCount,
		report.Statistics.ExcellentCount,
	)
}

// FormatDurationSimple 简单格式化时间间隔
func FormatDurationSimple(duration time.Duration) string {
	if duration < time.Minute {
		return fmt.Sprintf("%.1fs", duration.Seconds())
	} else if duration < time.Hour {
		minutes := int(duration.Minutes())
		seconds := int(duration.Seconds()) % 60
		if seconds > 0 {
			return fmt.Sprintf("%dm%ds", minutes, seconds)
		}
		return fmt.Sprintf("%dm", minutes)
	}
	hours := int(duration.Hours())
	minutes := int(duration.Minutes()) % 60
	return fmt.Sprintf("%dh%dm", hours, minutes)
}