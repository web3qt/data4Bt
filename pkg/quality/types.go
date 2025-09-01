package quality

import (
	"time"
)

// DataQualityReport 数据质量报告
type DataQualityReport struct {
	Symbol           string                `json:"symbol"`
	OverallScore     float64              `json:"overall_score"`
	MonthlyStatus    []*MonthlyDataStatus `json:"monthly_status"`
	MissingMonths    []string             `json:"missing_months"`
	PartialMonths    []string             `json:"partial_months"`
	Statistics       *QualityStatistics   `json:"statistics"`
	LastChecked      time.Time            `json:"last_checked"`
}

// MonthlyDataStatus 月度数据状态
type MonthlyDataStatus struct {
	Month              string    `json:"month"`
	HasData            bool      `json:"has_data"`
	ExpectedRecords    int64     `json:"expected_records"`
	ActualRecords      int64     `json:"actual_records"`
	CompletenessRatio  float64   `json:"completeness_ratio"`
	DataQualityIssues  []string  `json:"data_quality_issues"`
	FirstRecord        time.Time `json:"first_record,omitempty"`
	LastRecord         time.Time `json:"last_record,omitempty"`
}

// QualityStatistics 质量统计信息
type QualityStatistics struct {
	TotalMonths       int     `json:"total_months"`
	CompleteMonths    int     `json:"complete_months"`
	PartialMonths     int     `json:"partial_months"`
	MissingMonths     int     `json:"missing_months"`
	TotalRecords      int64   `json:"total_records"`
	ExpectedRecords   int64   `json:"expected_records"`
	CompletenessRatio float64 `json:"completeness_ratio"`
	AvailableFrom     string  `json:"available_from"`
	AvailableTo       string  `json:"available_to"`
	CoverageMonths    int     `json:"coverage_months"`
}

// QualityCheckRequest 质量检查请求
type QualityCheckRequest struct {
	Symbols   []string   `json:"symbols"`
	StartDate *time.Time `json:"start_date,omitempty"`
	EndDate   *time.Time `json:"end_date,omitempty"`
	CheckMode CheckMode  `json:"check_mode"`
}

// CheckMode 检查模式
type CheckMode int

const (
	// CheckModeBasic 基础检查 - 只检查数据存在性
	CheckModeBasic CheckMode = iota
	// CheckModeStandard 标准检查 - 检查数据存在性和完整性
	CheckModeStandard
	// CheckModeDetailed 详细检查 - 检查数据存在性、完整性和质量
	CheckModeDetailed
)

func (cm CheckMode) String() string {
	switch cm {
	case CheckModeBasic:
		return "basic"
	case CheckModeStandard:
		return "standard"
	case CheckModeDetailed:
		return "detailed"
	default:
		return "unknown"
	}
}

// DataQualityIssue 数据质量问题类型
type DataQualityIssue string

const (
	IssueNoData          DataQualityIssue = "no_data"
	IssueIncompleteData  DataQualityIssue = "incomplete_data"
	IssueInvalidPrice    DataQualityIssue = "invalid_price"
	IssueInvalidVolume   DataQualityIssue = "invalid_volume"
	IssueInvalidTime     DataQualityIssue = "invalid_time"
	IssueDataGaps        DataQualityIssue = "data_gaps"
	IssueDuplicateData   DataQualityIssue = "duplicate_data"
)

func (dqi DataQualityIssue) String() string {
	return string(dqi)
}

func (dqi DataQualityIssue) Description() string {
	switch dqi {
	case IssueNoData:
		return "月份无数据"
	case IssueIncompleteData:
		return "数据不完整"
	case IssueInvalidPrice:
		return "价格数据异常"
	case IssueInvalidVolume:
		return "交易量数据异常"
	case IssueInvalidTime:
		return "时间数据异常"
	case IssueDataGaps:
		return "数据间隔异常"
	case IssueDuplicateData:
		return "重复数据"
	default:
		return "未知问题"
	}
}

// QualityThreshold 质量阈值配置
type QualityThreshold struct {
	// ExcellentThreshold 优秀阈值 (95%+)
	ExcellentThreshold float64 `json:"excellent_threshold"`
	// GoodThreshold 良好阈值 (90%+)
	GoodThreshold float64 `json:"good_threshold"`
	// AcceptableThreshold 可接受阈值 (80%+)
	AcceptableThreshold float64 `json:"acceptable_threshold"`
	// MinExpectedRecordsPerMonth 每月最少预期记录数
	MinExpectedRecordsPerMonth int64 `json:"min_expected_records_per_month"`
}

// DefaultQualityThreshold 默认质量阈值
var DefaultQualityThreshold = QualityThreshold{
	ExcellentThreshold:         95.0,
	GoodThreshold:             90.0,
	AcceptableThreshold:       80.0,
	MinExpectedRecordsPerMonth: 40000, // 约28天 * 1440分钟/天
}

// QualityLevel 质量等级
type QualityLevel string

const (
	QualityLevelExcellent   QualityLevel = "excellent"
	QualityLevelGood        QualityLevel = "good"
	QualityLevelAcceptable  QualityLevel = "acceptable"
	QualityLevelPoor        QualityLevel = "poor"
)

func (ql QualityLevel) String() string {
	return string(ql)
}

func (ql QualityLevel) Description() string {
	switch ql {
	case QualityLevelExcellent:
		return "优秀 (95%+)"
	case QualityLevelGood:
		return "良好 (90%+)"
	case QualityLevelAcceptable:
		return "可接受 (80%+)"
	case QualityLevelPoor:
		return "较差 (<80%)"
	default:
		return "未知"
	}
}

// GetQualityLevel 根据完整性比率获取质量等级
func GetQualityLevel(completenessRatio float64) QualityLevel {
	threshold := DefaultQualityThreshold
	if completenessRatio >= threshold.ExcellentThreshold {
		return QualityLevelExcellent
	} else if completenessRatio >= threshold.GoodThreshold {
		return QualityLevelGood
	} else if completenessRatio >= threshold.AcceptableThreshold {
		return QualityLevelAcceptable
	} else {
		return QualityLevelPoor
	}
}

// BatchQualityReport 批量质量检查报告
type BatchQualityReport struct {
	TotalSymbols    int                   `json:"total_symbols"`
	CheckedSymbols  int                   `json:"checked_symbols"`
	Reports         []*DataQualityReport  `json:"reports"`
	Summary         *QualitySummary       `json:"summary"`
	GeneratedAt     time.Time             `json:"generated_at"`
	CheckDuration   time.Duration         `json:"check_duration"`
	CheckMode       CheckMode             `json:"check_mode"`
}

// QualitySummary 质量汇总信息
type QualitySummary struct {
	ExcellentCount    int     `json:"excellent_count"`
	GoodCount         int     `json:"good_count"`
	AcceptableCount   int     `json:"acceptable_count"`
	PoorCount         int     `json:"poor_count"`
	AverageScore      float64 `json:"average_score"`
	TotalMissingMonths int    `json:"total_missing_months"`
	TotalPartialMonths int    `json:"total_partial_months"`
}

// MonthRange 月份范围
type MonthRange struct {
	StartMonth string `json:"start_month"`
	EndMonth   string `json:"end_month"`
}

// ExpectedRecordsCalculator 预期记录数计算器接口
type ExpectedRecordsCalculator interface {
	CalculateExpectedRecords(month string) int64
}

// DefaultExpectedRecordsCalculator 默认预期记录数计算器
type DefaultExpectedRecordsCalculator struct{}

// CalculateExpectedRecords 计算指定月份的预期记录数
func (calc *DefaultExpectedRecordsCalculator) CalculateExpectedRecords(month string) int64 {
	// 解析月份
	t, err := time.Parse("2006-01", month)
	if err != nil {
		return DefaultQualityThreshold.MinExpectedRecordsPerMonth
	}
	
	// 计算该月的天数
	year := t.Year()
	monthNum := t.Month()
	
	// 获取下一个月的第一天，然后减去一天得到当前月的最后一天
	nextMonth := time.Date(year, monthNum+1, 1, 0, 0, 0, 0, time.UTC)
	lastDay := nextMonth.Add(-24 * time.Hour).Day()
	
	// 每天1440分钟 (24 * 60)
	return int64(lastDay * 1440)
}