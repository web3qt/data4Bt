package domain

import (
	"context"
	"time"
)

// KLine 表示K线数据结构
type KLine struct {
	Symbol               string    `json:"symbol"`
	OpenTime             time.Time `json:"open_time"`
	CloseTime            time.Time `json:"close_time"`
	OpenPrice            float64   `json:"open_price"`
	HighPrice            float64   `json:"high_price"`
	LowPrice             float64   `json:"low_price"`
	ClosePrice           float64   `json:"close_price"`
	Volume               float64   `json:"volume"`
	QuoteAssetVolume     float64   `json:"quote_asset_volume"`
	NumberOfTrades       int64     `json:"number_of_trades"`
	TakerBuyBaseVolume   float64   `json:"taker_buy_base_volume"`
	TakerBuyQuoteVolume  float64   `json:"taker_buy_quote_volume"`
	Interval             string    `json:"interval"`
	CreatedAt            time.Time `json:"created_at"`
}

// DownloadTask 表示下载任务
type DownloadTask struct {
	Symbol   string    `json:"symbol"`
	Date     time.Time `json:"date"`
	Interval string    `json:"interval"`
	URL      string    `json:"url"`
	Retries  int       `json:"retries"`
}

// ProcessingState 表示处理状态
type ProcessingState struct {
	Symbol           string    `json:"symbol"`
	LastDate         time.Time `json:"last_date"`
	TotalFiles       int       `json:"total_files"`
	Processed        int       `json:"processed"`
	Failed           int       `json:"failed"`
	LastUpdated      time.Time `json:"last_updated"`
	Status           string    `json:"status"`           // pending, running, completed, failed
	WorkerID         int       `json:"worker_id"`        // 执行该任务的worker编号
	StartDate        time.Time `json:"start_date"`       // 开始处理时间
	EndDate          time.Time `json:"end_date"`         // 结束处理时间
	CurrentMonth     string    `json:"current_month"`    // 当前处理的月份
	ErrorMessage     string    `json:"error_message"`    // 最后的错误信息
	ProgressPercent  float64   `json:"progress_percent"` // 进度百分比
}

// SymbolTimeline 表示代币的完整时间线状态
type SymbolTimeline struct {
	Symbol              string    `json:"symbol"`
	HistoricalStartDate time.Time `json:"historical_start_date"` // 币安最早可用数据时间
	CurrentImportDate   time.Time `json:"current_import_date"`   // 当前已导入到的时间
	LatestAvailableDate time.Time `json:"latest_available_date"` // 币安最新可用数据时间
	TotalMonths         int       `json:"total_months"`          // 总月份数
	ImportedMonthsCount int       `json:"imported_months_count"` // 已导入月份数
	FailedMonthsCount   int       `json:"failed_months_count"`   // 失败月份数
	ImportProgress      float64   `json:"import_progress"`       // 导入进度百分比
	Status              string    `json:"status"`               // 状态: discovering, importing, completed, failed
	LastUpdated         time.Time `json:"last_updated"`
	AvailableMonths     []string  `json:"available_months"`      // 所有可用月份列表 (YYYY-MM格式)
	ImportedMonths      []string  `json:"imported_months_list"`  // 已导入月份列表
	FailedMonths        []string  `json:"failed_months_list"`    // 失败月份列表
}

// ValidationResult 表示数据验证结果
type ValidationResult struct {
	Valid        bool     `json:"valid"`
	TotalRows    int      `json:"total_rows"`
	ValidRows    int      `json:"valid_rows"`
	InvalidRows  int      `json:"invalid_rows"`
	Errors       []string `json:"errors"`
	Warnings     []string `json:"warnings"`
}

// ProgressReport 表示进度报告
type ProgressReport struct {
	TotalTasks       int                    `json:"total_tasks"`
	CompletedTasks   int                    `json:"completed_tasks"`
	FailedTasks      int                    `json:"failed_tasks"`
	RunningTasks     int                    `json:"running_tasks"`
	Progress         float64                `json:"progress"`
	StartTime        time.Time              `json:"start_time"`
	EstimatedEnd     time.Time              `json:"estimated_end"`
	CurrentSymbol    string                 `json:"current_symbol"`
	CurrentDate      time.Time              `json:"current_date"`
	WorkerStates     map[int]*WorkerState   `json:"worker_states"`  // 每个worker的状态
	SymbolProgress   map[string]*SymbolProgressInfo `json:"symbol_progress"` // 每个币种的进度
}

// WorkerState 表示worker状态
type WorkerState struct {
	WorkerID      int       `json:"worker_id"`
	Status        string    `json:"status"`        // idle, running, completed, failed
	CurrentSymbol string    `json:"current_symbol"`
	CurrentMonth  string    `json:"current_month"`
	StartTime     time.Time `json:"start_time"`
	LastUpdate    time.Time `json:"last_update"`
	TasksCount    int       `json:"tasks_count"`
	CompletedTasks int      `json:"completed_tasks"`
	FailedTasks   int       `json:"failed_tasks"`
	ErrorMessage  string    `json:"error_message,omitempty"`
}

// SymbolProgressInfo 表示单个币种的进度信息
type SymbolProgressInfo struct {
	Symbol          string    `json:"symbol"`
	TotalMonths     int       `json:"total_months"`
	CompletedMonths int       `json:"completed_months"`
	FailedMonths    int       `json:"failed_months"`
	CurrentMonth    string    `json:"current_month"`
	Progress        float64   `json:"progress"`
	Status          string    `json:"status"`
	LastUpdate      time.Time `json:"last_update"`
	WorkerID        int       `json:"worker_id"`
}

// ConcurrentTask 表示并发任务
type ConcurrentTask struct {
	Symbol      string          `json:"symbol"`
	Tasks       []DownloadTask  `json:"tasks"`
	WorkerID    int             `json:"worker_id"`
	Priority    int             `json:"priority"`    // 任务优先级
	RetryCount  int             `json:"retry_count"` // 重试次数
}

// KLineRepository 定义K线数据存储接口
type KLineRepository interface {
	// Save 批量保存K线数据
	Save(ctx context.Context, klines []KLine) error
	
	// GetLastDate 获取指定交易对的最后日期
	GetLastDate(ctx context.Context, symbol string) (time.Time, error)
	
	// GetFirstDate 获取指定交易对的最早日期
	GetFirstDate(ctx context.Context, symbol string) (time.Time, error)
	
	// CreateTables 创建数据表
	CreateTables(ctx context.Context) error
	
	// CreateMaterializedViews 创建物化视图
	CreateMaterializedViews(ctx context.Context, intervals []string) error
	
	// RefreshMaterializedViews 刷新物化视图
	RefreshMaterializedViews(ctx context.Context) error
	
	// ValidateData 验证数据完整性
	ValidateData(ctx context.Context, symbol string, date time.Time) (*ValidationResult, error)
	
	// ClearAllData 清空所有数据
	ClearAllData(ctx context.Context) error
	
	// Close 关闭连接
	Close() error
}

// Downloader 定义下载器接口
type Downloader interface {
	// Fetch 下载并解压数据
	Fetch(ctx context.Context, task DownloadTask) ([]byte, error)
	
	// GetSymbols 获取所有可用的交易对
	GetSymbols(ctx context.Context) ([]string, error)
	
	// GetAvailableDates 获取指定交易对的可用日期
	GetAvailableDates(ctx context.Context, symbol string) ([]time.Time, error)
	
	// GetSymbolTimeline 获取指定交易对的完整时间线信息
	GetSymbolTimeline(ctx context.Context, symbol string) (*SymbolTimeline, error)
	
	// GetAllSymbolsFromBinance 从币安数据页面获取所有USDT交易对
	GetAllSymbolsFromBinance(ctx context.Context) ([]string, error)
	
	// ValidateURL 验证URL是否有效
	ValidateURL(ctx context.Context, url string) error
}

// StateManager 定义状态管理接口
type StateManager interface {
	// GetState 获取处理状态
	GetState(symbol string) (*ProcessingState, error)
	
	// SaveState 保存处理状态
	SaveState(state *ProcessingState) error
	
	// GetAllStates 获取所有状态
	GetAllStates() (map[string]*ProcessingState, error)
	
	// DeleteState 删除状态
	DeleteState(symbol string) error
	
	// GetTimeline 获取代币时间线状态
	GetTimeline(symbol string) (*SymbolTimeline, error)
	
	// SaveTimeline 保存代币时间线状态
	SaveTimeline(timeline *SymbolTimeline) error
	
	// GetAllTimelines 获取所有代币时间线状态
	GetAllTimelines() (map[string]*SymbolTimeline, error)
	
	// DeleteTimeline 删除代币时间线状态
	DeleteTimeline(symbol string) error
	
	// Backup 备份状态
	Backup() error
	
	// Restore 恢复状态
	Restore(backupPath string) error
	
	// UpdateWorkerState 更新worker状态
	UpdateWorkerState(workerID int, state *WorkerState) error
	
	// GetWorkerState 获取worker状态
	GetWorkerState(workerID int) (*WorkerState, error)
	
	// GetAllWorkerStates 获取所有worker状态
	GetAllWorkerStates() (map[int]*WorkerState, error)
	
	// UpdateSymbolProgress 更新币种进度
	UpdateSymbolProgress(symbol string, progress *SymbolProgressInfo) error
	
	// GetSymbolProgress 获取币种进度
	GetSymbolProgress(symbol string) (*SymbolProgressInfo, error)
	
	// GetAllSymbolProgress 获取所有币种进度
	GetAllSymbolProgress() (map[string]*SymbolProgressInfo, error)
	
	// GetIncompleteSymbols 获取未完成的币种列表（用于断点续传）
	GetIncompleteSymbols() ([]string, error)
}

// Parser 定义解析器接口
type Parser interface {
	// Parse 解析CSV数据为K线数据
	Parse(ctx context.Context, data []byte, symbol string) ([]KLine, *ValidationResult, error)
	
	// ValidateCSV 验证CSV格式
	ValidateCSV(data []byte) error
}

// ProgressReporter 进度报告器接口
type ProgressReporter interface {
	Start(totalTasks int) error
	ReportProgress(progress *ProgressReport)
	GetOverallProgress() map[string]interface{}
	Stop(ctx context.Context) error
}

// Importer 数据导入器接口
type Importer interface {
	ImportData(ctx context.Context, tasks []DownloadTask) error
	// ImportDataConcurrent 并发导入数据
	ImportDataConcurrent(ctx context.Context, concurrentTasks []ConcurrentTask) error
	Close() error
}