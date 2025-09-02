package reports

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// MarkdownGenerator Markdown报告生成器
type MarkdownGenerator struct{}

// NewMarkdownGenerator 创建新的Markdown生成器
func NewMarkdownGenerator() *MarkdownGenerator {
	return &MarkdownGenerator{}
}

// GenerateReport 生成Markdown格式的验证报告
func (mg *MarkdownGenerator) GenerateReport(report *VerificationSummaryReport, outputPath string) error {
	// 确保输出目录存在
	outputDir := filepath.Dir(outputPath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// 生成Markdown内容
	content := mg.buildMarkdownContent(report)

	// 写入文件
	if err := os.WriteFile(outputPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write report file: %w", err)
	}

	return nil
}

// buildMarkdownContent 构建Markdown内容
func (mg *MarkdownGenerator) buildMarkdownContent(report *VerificationSummaryReport) string {
	var content strings.Builder

	// 标题和基本信息
	mg.writeHeader(&content, report)
	
	// 执行摘要
	mg.writeExecutiveSummary(&content, report)
	
	// 详细统计信息
	mg.writeDetailedStatistics(&content, report)
	
	// 问题分类详情
	mg.writeIssueCategories(&content, report)
	
	// 问题模式分析
	mg.writeIssuePatterns(&content, report)
	
	// 修复建议
	mg.writeActionItems(&content, report)
	
	// 附录
	mg.writeAppendix(&content, report)

	return content.String()
}

// writeHeader 写入报告头部
func (mg *MarkdownGenerator) writeHeader(content *strings.Builder, report *VerificationSummaryReport) {
	content.WriteString("# 📊 数据完整性验证报告\n\n")
	content.WriteString(fmt.Sprintf("**生成时间**: %s  \n", report.GeneratedAt.Format("2006-01-02 15:04:05")))
	content.WriteString(fmt.Sprintf("**验证范围**: 所有交易对 (%d个)  \n", report.TotalSymbols))
	content.WriteString(fmt.Sprintf("**执行耗时**: %s  \n", mg.formatDuration(report.ExecutionTime)))
	content.WriteString(fmt.Sprintf("**平均完整性**: %.2f%%  \n\n", report.Statistics.AverageCompleteness))

	// 生成目录
	content.WriteString("## 📋 目录\n\n")
	content.WriteString("- [执行摘要](#-执行摘要)\n")
	content.WriteString("- [详细统计](#-详细统计)\n")
	content.WriteString("- [问题分类](#-问题分类)\n")
	if len(report.Statistics.CommonIssuePatterns) > 0 {
		content.WriteString("- [问题模式分析](#-问题模式分析)\n")
	}
	content.WriteString("- [修复建议](#-修复建议)\n")
	content.WriteString("- [附录](#-附录)\n\n")
	content.WriteString("---\n\n")
}

// writeExecutiveSummary 写入执行摘要
func (mg *MarkdownGenerator) writeExecutiveSummary(content *strings.Builder, report *VerificationSummaryReport) {
	content.WriteString("## 📈 执行摘要\n\n")
	
	stats := report.Statistics
	
	// 关键指标表格
	content.WriteString("### 关键指标\n\n")
	content.WriteString("| 指标 | 数值 | 百分比 |\n")
	content.WriteString("|------|------|--------|\n")
	content.WriteString(fmt.Sprintf("| 🔴 **严重问题** | %d个交易对 | %.1f%% |\n", 
		stats.CriticalCount, 
		float64(stats.CriticalCount)/float64(report.TotalSymbols)*100))
	content.WriteString(fmt.Sprintf("| 🟡 **需要关注** | %d个交易对 | %.1f%% |\n", 
		stats.NeedsAttentionCount, 
		float64(stats.NeedsAttentionCount)/float64(report.TotalSymbols)*100))
	content.WriteString(fmt.Sprintf("| 🟢 **状况良好** | %d个交易对 | %.1f%% |\n", 
		stats.GoodConditionCount, 
		float64(stats.GoodConditionCount)/float64(report.TotalSymbols)*100))
	content.WriteString(fmt.Sprintf("| ✅ **数据完整** | %d个交易对 | %.1f%% |\n", 
		stats.ExcellentCount, 
		float64(stats.ExcellentCount)/float64(report.TotalSymbols)*100))
	content.WriteString("\n")

	// 质量分布可视化
	content.WriteString("### 质量分布可视化\n\n")
	content.WriteString("```\n")
	mg.writeQualityDistributionChart(content, report)
	content.WriteString("```\n\n")

	// 重点关注项
	content.WriteString("### 🎯 重点关注\n\n")
	if stats.CriticalCount > 0 {
		content.WriteString(fmt.Sprintf("- 🚨 **%d个交易对**需要立即处理（完整性 < 60%%）\n", stats.CriticalCount))
	}
	if stats.NeedsAttentionCount > 0 {
		content.WriteString(fmt.Sprintf("- ⚠️  **%d个交易对**建议近期修复（完整性 60-80%%）\n", stats.NeedsAttentionCount))
	}
	if stats.ExcellentCount > 0 {
		content.WriteString(fmt.Sprintf("- 🎉 **%d个交易对**数据完整性优秀（≥95%%）\n", stats.ExcellentCount))
	}
	content.WriteString("\n---\n\n")
}

// writeQualityDistributionChart 写入质量分布图
func (mg *MarkdownGenerator) writeQualityDistributionChart(content *strings.Builder, report *VerificationSummaryReport) {
	stats := report.Statistics
	total := float64(report.TotalSymbols)
	
	// 简单的ASCII图表
	excellent := int(float64(stats.ExcellentCount) / total * 50)
	good := int(float64(stats.GoodConditionCount) / total * 50)
	attention := int(float64(stats.NeedsAttentionCount) / total * 50)
	critical := int(float64(stats.CriticalCount) / total * 50)
	
	content.WriteString("质量分布图 (每个字符代表约2%的交易对):\n\n")
	content.WriteString("✅ 优秀(≥95%):  " + strings.Repeat("█", excellent) + 
		fmt.Sprintf(" %d个 (%.1f%%)\n", stats.ExcellentCount, float64(stats.ExcellentCount)/total*100))
	content.WriteString("🟢 良好(80-95%): " + strings.Repeat("█", good) + 
		fmt.Sprintf(" %d个 (%.1f%%)\n", stats.GoodConditionCount, float64(stats.GoodConditionCount)/total*100))
	content.WriteString("🟡 关注(60-80%): " + strings.Repeat("█", attention) + 
		fmt.Sprintf(" %d个 (%.1f%%)\n", stats.NeedsAttentionCount, float64(stats.NeedsAttentionCount)/total*100))
	content.WriteString("🔴 严重(<60%):   " + strings.Repeat("█", critical) + 
		fmt.Sprintf(" %d个 (%.1f%%)\n", stats.CriticalCount, float64(stats.CriticalCount)/total*100))
}

// writeDetailedStatistics 写入详细统计
func (mg *MarkdownGenerator) writeDetailedStatistics(content *strings.Builder, report *VerificationSummaryReport) {
	content.WriteString("## 📊 详细统计\n\n")
	
	stats := report.Statistics
	
	// 整体数据概况
	content.WriteString("### 整体数据概况\n\n")
	content.WriteString("| 项目 | 数值 |\n")
	content.WriteString("|------|------|\n")
	content.WriteString(fmt.Sprintf("| 总交易对数 | %d |\n", report.TotalSymbols))
	content.WriteString(fmt.Sprintf("| 平均完整性 | %.2f%% |\n", stats.AverageCompleteness))
	content.WriteString(fmt.Sprintf("| 中位数完整性 | %.2f%% |\n", stats.MedianCompleteness))
	content.WriteString(fmt.Sprintf("| 数据覆盖年数 | %d年 |\n", stats.CoverageYears))
	if stats.EarliestDataDate != "" && stats.LatestDataDate != "" {
		content.WriteString(fmt.Sprintf("| 数据时间范围 | %s 至 %s |\n", stats.EarliestDataDate, stats.LatestDataDate))
	}
	content.WriteString("\n")

	// 月份数据统计
	content.WriteString("### 月份数据统计\n\n")
	content.WriteString("| 项目 | 数量 | 占比 |\n")
	content.WriteString("|------|------|------|\n")
	content.WriteString(fmt.Sprintf("| 总分析月份数 | %d | - |\n", stats.TotalMonthsAnalyzed))
	if stats.TotalMonthsAnalyzed > 0 {
		content.WriteString(fmt.Sprintf("| 完整月份 | %d | %.1f%% |\n", 
			stats.CompletedMonths, float64(stats.CompletedMonths)/float64(stats.TotalMonthsAnalyzed)*100))
		content.WriteString(fmt.Sprintf("| 部分月份 | %d | %.1f%% |\n", 
			stats.PartialMonths, float64(stats.PartialMonths)/float64(stats.TotalMonthsAnalyzed)*100))
		content.WriteString(fmt.Sprintf("| 缺失月份 | %d | %.1f%% |\n", 
			stats.MissingMonths, float64(stats.MissingMonths)/float64(stats.TotalMonthsAnalyzed)*100))
	}
	content.WriteString("\n---\n\n")
}

// writeIssueCategories 写入问题分类
func (mg *MarkdownGenerator) writeIssueCategories(content *strings.Builder, report *VerificationSummaryReport) {
	content.WriteString("## 🔍 问题分类\n\n")
	
	// 严重问题
	if len(report.CriticalIssues) > 0 {
		mg.writeIssueCategory(content, "🔴 严重问题 (完整性 < 60%)", report.CriticalIssues, true)
	}
	
	// 需要关注
	if len(report.NeedsAttention) > 0 {
		mg.writeIssueCategory(content, "🟡 需要关注 (完整性 60-80%)", report.NeedsAttention, true)
	}
	
	// 状况良好
	if len(report.GoodCondition) > 0 {
		mg.writeIssueCategory(content, "🟢 状况良好 (完整性 80-95%)", report.GoodCondition, false)
	}
	
	// 数据完整
	if len(report.Excellent) > 0 {
		mg.writeIssueCategory(content, "✅ 数据完整 (完整性 ≥ 95%)", report.Excellent, false)
	}
	
	content.WriteString("---\n\n")
}

// writeIssueCategory 写入具体问题分类
func (mg *MarkdownGenerator) writeIssueCategory(content *strings.Builder, title string, issues []SymbolIssue, showDetails bool) {
	content.WriteString(fmt.Sprintf("### %s\n\n", title))
	content.WriteString(fmt.Sprintf("**数量**: %d个交易对\n\n", len(issues)))
	
	if showDetails {
		// 详细表格
		content.WriteString("| 交易对 | 完整性 | 数据范围 | 缺失月份 | 主要问题 |\n")
		content.WriteString("|--------|--------|----------|----------|----------|\n")
		
		maxShow := 20 // 最多显示20个
		for i, issue := range issues {
			if i >= maxShow {
				content.WriteString(fmt.Sprintf("| ... | ... | ... | ... | 还有%d个交易对 |\n", len(issues)-maxShow))
				break
			}
			
			missingCount := len(issue.MissingMonths)
			missingDesc := ""
			if missingCount > 0 {
				if missingCount <= 3 {
					missingDesc = strings.Join(issue.MissingMonths, ", ")
				} else {
					missingDesc = fmt.Sprintf("%d个月份", missingCount)
				}
			} else {
				missingDesc = "无"
			}
			
			mainIssue := mg.getMainIssueDescription(issue)
			
			content.WriteString(fmt.Sprintf("| %s | %.1f%% | %s | %s | %s |\n",
				issue.Symbol,
				issue.CompletenessRatio,
				issue.DataRange,
				missingDesc,
				mainIssue))
		}
	} else {
		// 简化列表
		content.WriteString("**交易对列表**: ")
		symbolNames := make([]string, len(issues))
		for i, issue := range issues {
			symbolNames[i] = fmt.Sprintf("%s(%.1f%%)", issue.Symbol, issue.CompletenessRatio)
		}
		
		maxShow := 10
		if len(symbolNames) > maxShow {
			content.WriteString(strings.Join(symbolNames[:maxShow], ", "))
			content.WriteString(fmt.Sprintf(" ... 等%d个", len(symbolNames)))
		} else {
			content.WriteString(strings.Join(symbolNames, ", "))
		}
	}
	
	content.WriteString("\n\n")
}

// getMainIssueDescription 获取主要问题描述
func (mg *MarkdownGenerator) getMainIssueDescription(issue SymbolIssue) string {
	if len(issue.MissingMonths) > 5 {
		return "大量月份缺失"
	} else if len(issue.MissingMonths) > 0 {
		return "部分月份缺失"
	} else if len(issue.PartialMonthsList) > 0 {
		return "数据不完整"
	}
	return "质量良好"
}

// writeIssuePatterns 写入问题模式分析
func (mg *MarkdownGenerator) writeIssuePatterns(content *strings.Builder, report *VerificationSummaryReport) {
	if len(report.Statistics.CommonIssuePatterns) == 0 {
		return
	}
	
	content.WriteString("## 🔍 问题模式分析\n\n")
	content.WriteString("通过分析发现以下系统性问题模式：\n\n")
	
	for i, pattern := range report.Statistics.CommonIssuePatterns {
		content.WriteString(fmt.Sprintf("### 模式 %d: %s\n\n", i+1, pattern.Description))
		content.WriteString(fmt.Sprintf("**影响频次**: %d\n", pattern.Frequency))
		
		if len(pattern.AffectedSymbols) > 0 {
			content.WriteString(fmt.Sprintf("**受影响交易对**: %s\n", strings.Join(pattern.AffectedSymbols, ", ")))
		}
		content.WriteString("\n")
	}
	
	content.WriteString("---\n\n")
}

// writeActionItems 写入修复建议
func (mg *MarkdownGenerator) writeActionItems(content *strings.Builder, report *VerificationSummaryReport) {
	content.WriteString("## 🛠️ 修复建议\n\n")
	
	// 优先级建议
	content.WriteString("### 按优先级排序的修复建议\n\n")
	
	if len(report.CriticalIssues) > 0 {
		content.WriteString("#### 🚨 高优先级 (立即处理)\n\n")
		content.WriteString(fmt.Sprintf("需要立即处理 **%d个严重问题**的交易对:\n\n", len(report.CriticalIssues)))
		
		for _, issue := range report.CriticalIssues[:min(5, len(report.CriticalIssues))] {
			content.WriteString(fmt.Sprintf("**%s** (%.1f%%)\n", issue.Symbol, issue.CompletenessRatio))
			for _, rec := range issue.Recommendations {
				content.WriteString(fmt.Sprintf("- %s\n", rec))
			}
			content.WriteString("\n")
		}
		
		if len(report.CriticalIssues) > 5 {
			content.WriteString(fmt.Sprintf("... 还有 %d 个交易对需要处理\n\n", len(report.CriticalIssues)-5))
		}
	}
	
	if len(report.NeedsAttention) > 0 {
		content.WriteString("#### ⚠️ 中优先级 (建议处理)\n\n")
		content.WriteString(fmt.Sprintf("建议在合适时机处理 **%d个需要关注**的交易对\n\n", len(report.NeedsAttention)))
		content.WriteString("**通用建议**:\n")
		content.WriteString("- 🔄 在系统低峰时段补全不完整的数据\n")
		content.WriteString("- 📊 设置监控告警，防止数据质量进一步下降\n")
		content.WriteString("- 🔍 分析数据缺失的根本原因\n\n")
	}
	
	// 系统性建议
	content.WriteString("### 🎯 系统性改进建议\n\n")
	content.WriteString("- **自动化监控**: 建立数据完整性定期检查机制\n")
	content.WriteString("- **告警系统**: 对完整性低于80%的交易对设置告警\n")
	content.WriteString("- **备份策略**: 确保关键交易对数据的多重备份\n")
	content.WriteString("- **质量标准**: 建立交易对数据质量的准入和维护标准\n")
	content.WriteString("- **文档记录**: 记录每次修复操作和效果评估\n\n")
	
	content.WriteString("---\n\n")
}

// writeAppendix 写入附录
func (mg *MarkdownGenerator) writeAppendix(content *strings.Builder, report *VerificationSummaryReport) {
	content.WriteString("## 📎 附录\n\n")
	
	// 质量等级说明
	content.WriteString("### 质量等级说明\n\n")
	content.WriteString("| 等级 | 完整性范围 | 说明 | 建议措施 |\n")
	content.WriteString("|------|------------|------|----------|\n")
	content.WriteString("| ✅ 优秀 | ≥ 95% | 数据完整性极佳 | 继续保持，定期维护 |\n")
	content.WriteString("| 🟢 良好 | 80% - 95% | 数据完整性较好 | 适时优化，监控变化 |\n")
	content.WriteString("| 🟡 关注 | 60% - 80% | 数据存在缺失 | 建议修复，加强监控 |\n")
	content.WriteString("| 🔴 严重 | < 60% | 数据严重不完整 | 立即修复，高优先级 |\n\n")
	
	// 修复命令参考
	content.WriteString("### 修复命令参考\n\n")
	content.WriteString("```bash\n")
	content.WriteString("# 重新验证特定交易对\n")
	content.WriteString("go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT,ETHUSDT\n\n")
	content.WriteString("# 重新下载缺失数据\n")
	content.WriteString("go run cmd/main.go -cmd=run -symbols=BTCUSDT\n\n")
	content.WriteString("# 更新交易对时间范围\n")
	content.WriteString("go run cmd/main.go -cmd=update-ranges -symbols=BTCUSDT\n\n")
	content.WriteString("# 查看交易对状态\n")
	content.WriteString("go run cmd/main.go -cmd=status -detailed\n")
	content.WriteString("```\n\n")
	
	// 生成信息
	content.WriteString("### 生成信息\n\n")
	content.WriteString(fmt.Sprintf("- **生成工具**: Binance Data Loader v1.2.2\n"))
	content.WriteString(fmt.Sprintf("- **报告版本**: 1.0\n"))
	content.WriteString(fmt.Sprintf("- **生成时间**: %s\n", report.GeneratedAt.Format("2006-01-02 15:04:05")))
	content.WriteString(fmt.Sprintf("- **执行耗时**: %s\n", mg.formatDuration(report.ExecutionTime)))
	
	content.WriteString("\n---\n")
	content.WriteString("\n*本报告由数据完整性验证系统自动生成*")
}

// formatDuration 格式化时间间隔
func (mg *MarkdownGenerator) formatDuration(duration time.Duration) string {
	if duration < time.Minute {
		return fmt.Sprintf("%.1fs", duration.Seconds())
	} else if duration < time.Hour {
		minutes := int(duration.Minutes())
		seconds := int(duration.Seconds()) % 60
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}
	hours := int(duration.Hours())
	minutes := int(duration.Minutes()) % 60
	return fmt.Sprintf("%dh%dm", hours, minutes)
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}