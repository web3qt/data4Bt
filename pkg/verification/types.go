package verification

import (
	"time"
)

// CompletenessStatus 完整性状态枚举
type CompletenessStatus string

const (
	CompletenessStatusComplete   CompletenessStatus = "complete"   // 完整
	CompletenessStatusPartial    CompletenessStatus = "partial"    // 部分
	CompletenessStatusMissing    CompletenessStatus = "missing"    // 缺失
	CompletenessStatusUnknown    CompletenessStatus = "unknown"    // 未知
)

// SymbolDataRange 交易对数据范围
type SymbolDataRange struct {
	Symbol       string    `json:"symbol"`
	EarliestDate time.Time `json:"earliest_date"`
	LatestDate   time.Time `json:"latest_date"`
	TotalMonths  int       `json:"total_months"`
	MonthList    []string  `json:"month_list"`
	HasData      bool      `json:"has_data"`
}

// MonthlyCompletenessReport 月度完整性报告
type MonthlyCompletenessReport struct {
	Month             string             `json:"month"`
	ExpectedRecords   int64              `json:"expected_records"`
	ActualRecords     int64              `json:"actual_records"`
	CompletenessRatio float64            `json:"completeness_ratio"`
	Status            CompletenessStatus `json:"status"`
	FirstRecord       time.Time          `json:"first_record,omitempty"`
	LastRecord        time.Time          `json:"last_record,omitempty"`
	HasData           bool               `json:"has_data"`
}

// DatabaseVerificationReport 数据库验证报告
type DatabaseVerificationReport struct {
	Symbol                string                        `json:"symbol"`
	DataRange            *SymbolDataRange              `json:"data_range"`
	MonthlyReports       []*MonthlyCompletenessReport  `json:"monthly_reports"`
	OverallCompleteness  float64                       `json:"overall_completeness"`
	MissingMonths        []string                      `json:"missing_months"`
	IncompleteMonths     []string                      `json:"incomplete_months"`
	QualityScore         float64                       `json:"quality_score"`
	GeneratedAt          time.Time                     `json:"generated_at"`
}

// BatchDatabaseVerificationReport 批量数据库验证报告
type BatchDatabaseVerificationReport struct {
	Reports             []*DatabaseVerificationReport `json:"reports"`
	TotalSymbols        int                           `json:"total_symbols"`
	VerifiedSymbols     int                           `json:"verified_symbols"`
	AverageCompleteness float64                       `json:"average_completeness"`
	Summary             *VerificationSummary          `json:"summary"`
	GeneratedAt         time.Time                     `json:"generated_at"`
}

// VerificationSummary 验证摘要
type VerificationSummary struct {
	TotalMonths       int     `json:"total_months"`
	CompleteMonths    int     `json:"complete_months"`
	PartialMonths     int     `json:"partial_months"`
	MissingMonths     int     `json:"missing_months"`
	AverageScore      float64 `json:"average_score"`
	QualityGrade      string  `json:"quality_grade"`
}

// GetCompletenessStatus 根据完整性比例获取状态
func GetCompletenessStatus(ratio float64, hasData bool) CompletenessStatus {
	if !hasData {
		return CompletenessStatusMissing
	}
	if ratio >= 95.0 {
		return CompletenessStatusComplete
	}
	if ratio > 0.0 {
		return CompletenessStatusPartial
	}
	return CompletenessStatusMissing
}

// GetQualityGrade 根据平均评分获取质量等级
func GetQualityGrade(score float64) string {
	if score >= 95.0 {
		return "优秀 (95%+)"
	}
	if score >= 90.0 {
		return "良好 (90%+)"
	}
	if score >= 80.0 {
		return "可接受 (80%+)"
	}
	return "较差 (<80%)"
}