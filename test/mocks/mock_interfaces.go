package mocks

import (
	"context"
	"time"

	"binance-data-loader/internal/domain"
)

// MockDownloader 模拟下载器
type MockDownloader struct {
	FetchFunc                   func(ctx context.Context, task domain.DownloadTask) ([]byte, error)
	GetSymbolsFunc              func(ctx context.Context) ([]string, error)
	GetAvailableDatesFunc       func(ctx context.Context, symbol string) ([]time.Time, error)
	GetSymbolTimelineFunc       func(ctx context.Context, symbol string) (*domain.SymbolTimeline, error)
	GetAllSymbolsFromBinanceFunc func(ctx context.Context) ([]string, error)
	ValidateURLFunc             func(ctx context.Context, url string) error
}

func (m *MockDownloader) Fetch(ctx context.Context, task domain.DownloadTask) ([]byte, error) {
	if m.FetchFunc != nil {
		return m.FetchFunc(ctx, task)
	}
	return []byte("mock csv data"), nil
}

func (m *MockDownloader) GetSymbols(ctx context.Context) ([]string, error) {
	if m.GetSymbolsFunc != nil {
		return m.GetSymbolsFunc(ctx)
	}
	return []string{"BTCUSDT", "ETHUSDT"}, nil
}

func (m *MockDownloader) GetAvailableDates(ctx context.Context, symbol string) ([]time.Time, error) {
	if m.GetAvailableDatesFunc != nil {
		return m.GetAvailableDatesFunc(ctx, symbol)
	}
	return []time.Time{
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
	}, nil
}

func (m *MockDownloader) ValidateURL(ctx context.Context, url string) error {
	if m.ValidateURLFunc != nil {
		return m.ValidateURLFunc(ctx, url)
	}
	return nil
}

func (m *MockDownloader) GetSymbolTimeline(ctx context.Context, symbol string) (*domain.SymbolTimeline, error) {
	if m.GetSymbolTimelineFunc != nil {
		return m.GetSymbolTimelineFunc(ctx, symbol)
	}
	return &domain.SymbolTimeline{
		Symbol:      symbol,
		TotalMonths: 12,
		Status:      "completed",
		LastUpdated: time.Now(),
	}, nil
}

func (m *MockDownloader) GetAllSymbolsFromBinance(ctx context.Context) ([]string, error) {
	if m.GetAllSymbolsFromBinanceFunc != nil {
		return m.GetAllSymbolsFromBinanceFunc(ctx)
	}
	return []string{"BTCUSDT", "ETHUSDT"}, nil
}

// MockStateManager 模拟状态管理器
type MockStateManager struct {
	GetStateFunc              func(symbol string) (*domain.ProcessingState, error)
	SaveStateFunc             func(state *domain.ProcessingState) error
	GetAllStatesFunc          func() (map[string]*domain.ProcessingState, error)
	DeleteStateFunc           func(symbol string) error
	GetTimelineFunc           func(symbol string) (*domain.SymbolTimeline, error)
	SaveTimelineFunc          func(timeline *domain.SymbolTimeline) error
	GetAllTimelinesFunc       func() (map[string]*domain.SymbolTimeline, error)
	DeleteTimelineFunc        func(symbol string) error
	BackupFunc                func() error
	RestoreFunc               func(backupPath string) error
	UpdateWorkerStateFunc     func(workerID int, state *domain.WorkerState) error
	GetWorkerStateFunc        func(workerID int) (*domain.WorkerState, error)
	GetAllWorkerStatesFunc    func() (map[int]*domain.WorkerState, error)
	UpdateSymbolProgressFunc  func(symbol string, progress *domain.SymbolProgressInfo) error
	GetSymbolProgressFunc     func(symbol string) (*domain.SymbolProgressInfo, error)
	GetAllSymbolProgressFunc  func() (map[string]*domain.SymbolProgressInfo, error)
	GetIncompleteSymbolsFunc  func() ([]string, error)
}

func (m *MockStateManager) GetState(symbol string) (*domain.ProcessingState, error) {
	if m.GetStateFunc != nil {
		return m.GetStateFunc(symbol)
	}
	return &domain.ProcessingState{
		Symbol:      symbol,
		Status:      "pending",
		LastUpdated: time.Now(),
	}, nil
}

func (m *MockStateManager) SaveState(state *domain.ProcessingState) error {
	if m.SaveStateFunc != nil {
		return m.SaveStateFunc(state)
	}
	return nil
}

func (m *MockStateManager) GetAllStates() (map[string]*domain.ProcessingState, error) {
	if m.GetAllStatesFunc != nil {
		return m.GetAllStatesFunc()
	}
	return make(map[string]*domain.ProcessingState), nil
}

func (m *MockStateManager) DeleteState(symbol string) error {
	if m.DeleteStateFunc != nil {
		return m.DeleteStateFunc(symbol)
	}
	return nil
}

func (m *MockStateManager) Backup() error {
	if m.BackupFunc != nil {
		return m.BackupFunc()
	}
	return nil
}

func (m *MockStateManager) Restore(backupPath string) error {
	if m.RestoreFunc != nil {
		return m.RestoreFunc(backupPath)
	}
	return nil
}

func (m *MockStateManager) UpdateWorkerState(workerID int, state *domain.WorkerState) error {
	if m.UpdateWorkerStateFunc != nil {
		return m.UpdateWorkerStateFunc(workerID, state)
	}
	return nil
}

func (m *MockStateManager) GetWorkerState(workerID int) (*domain.WorkerState, error) {
	if m.GetWorkerStateFunc != nil {
		return m.GetWorkerStateFunc(workerID)
	}
	return &domain.WorkerState{
		WorkerID: workerID,
		Status:   "idle",
	}, nil
}

func (m *MockStateManager) GetAllWorkerStates() (map[int]*domain.WorkerState, error) {
	if m.GetAllWorkerStatesFunc != nil {
		return m.GetAllWorkerStatesFunc()
	}
	return make(map[int]*domain.WorkerState), nil
}

func (m *MockStateManager) UpdateSymbolProgress(symbol string, progress *domain.SymbolProgressInfo) error {
	if m.UpdateSymbolProgressFunc != nil {
		return m.UpdateSymbolProgressFunc(symbol, progress)
	}
	return nil
}

func (m *MockStateManager) GetSymbolProgress(symbol string) (*domain.SymbolProgressInfo, error) {
	if m.GetSymbolProgressFunc != nil {
		return m.GetSymbolProgressFunc(symbol)
	}
	return &domain.SymbolProgressInfo{
		Symbol:   symbol,
		Progress: 50.0,
		Status:   "processing",
	}, nil
}

func (m *MockStateManager) GetAllSymbolProgress() (map[string]*domain.SymbolProgressInfo, error) {
	if m.GetAllSymbolProgressFunc != nil {
		return m.GetAllSymbolProgressFunc()
	}
	return make(map[string]*domain.SymbolProgressInfo), nil
}

func (m *MockStateManager) GetIncompleteSymbols() ([]string, error) {
	if m.GetIncompleteSymbolsFunc != nil {
		return m.GetIncompleteSymbolsFunc()
	}
	return []string{}, nil
}

func (m *MockStateManager) GetTimeline(symbol string) (*domain.SymbolTimeline, error) {
	if m.GetTimelineFunc != nil {
		return m.GetTimelineFunc(symbol)
	}
	return &domain.SymbolTimeline{
		Symbol:      symbol,
		TotalMonths: 12,
		Status:      "completed",
		LastUpdated: time.Now(),
	}, nil
}

func (m *MockStateManager) SaveTimeline(timeline *domain.SymbolTimeline) error {
	if m.SaveTimelineFunc != nil {
		return m.SaveTimelineFunc(timeline)
	}
	return nil
}

func (m *MockStateManager) GetAllTimelines() (map[string]*domain.SymbolTimeline, error) {
	if m.GetAllTimelinesFunc != nil {
		return m.GetAllTimelinesFunc()
	}
	return make(map[string]*domain.SymbolTimeline), nil
}

func (m *MockStateManager) DeleteTimeline(symbol string) error {
	if m.DeleteTimelineFunc != nil {
		return m.DeleteTimelineFunc(symbol)
	}
	return nil
}

// MockImporter 模拟导入器
type MockImporter struct {
	ImportDataFunc           func(ctx context.Context, tasks []domain.DownloadTask) error
	ImportDataConcurrentFunc func(ctx context.Context, concurrentTasks []domain.ConcurrentTask) error
	CloseFunc                func() error
}

func (m *MockImporter) ImportData(ctx context.Context, tasks []domain.DownloadTask) error {
	if m.ImportDataFunc != nil {
		return m.ImportDataFunc(ctx, tasks)
	}
	return nil
}

func (m *MockImporter) ImportDataConcurrent(ctx context.Context, concurrentTasks []domain.ConcurrentTask) error {
	if m.ImportDataConcurrentFunc != nil {
		return m.ImportDataConcurrentFunc(ctx, concurrentTasks)
	}
	return nil
}

func (m *MockImporter) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

// MockProgressReporter 模拟进度报告器
type MockProgressReporter struct {
	StartFunc              func(totalTasks int) error
	ReportProgressFunc     func(progress *domain.ProgressReport)
	GetOverallProgressFunc func() map[string]interface{}
	StopFunc               func(ctx context.Context) error
}

func (m *MockProgressReporter) Start(totalTasks int) error {
	if m.StartFunc != nil {
		return m.StartFunc(totalTasks)
	}
	return nil
}

func (m *MockProgressReporter) ReportProgress(progress *domain.ProgressReport) {
	if m.ReportProgressFunc != nil {
		m.ReportProgressFunc(progress)
	}
}

func (m *MockProgressReporter) Stop(ctx context.Context) error {
	if m.StopFunc != nil {
		return m.StopFunc(ctx)
	}
	return nil
}

func (m *MockProgressReporter) GetOverallProgress() map[string]interface{} {
	if m.GetOverallProgressFunc != nil {
		return m.GetOverallProgressFunc()
	}
	return map[string]interface{}{
		"completed": 50,
		"total":     100,
		"progress":  50.0,
	}
}

// MockRepository 模拟仓库
type MockRepository struct {
	CreateTablesFunc           func(ctx context.Context) error
	SaveFunc                   func(ctx context.Context, klines []domain.KLine) error
	GetLastDateFunc            func(ctx context.Context, symbol string) (time.Time, error)
	GetFirstDateFunc           func(ctx context.Context, symbol string) (time.Time, error)
	ValidateDataFunc           func(ctx context.Context, symbol string, date time.Time) (*domain.ValidationResult, error)
	ClearAllDataFunc           func(ctx context.Context) error
	CreateMaterializedViewsFunc func(ctx context.Context, intervals []string) error
	RefreshMaterializedViewsFunc func(ctx context.Context) error
	CloseFunc                  func() error
}

func (m *MockRepository) CreateTables(ctx context.Context) error {
	if m.CreateTablesFunc != nil {
		return m.CreateTablesFunc(ctx)
	}
	return nil
}

func (m *MockRepository) Save(ctx context.Context, klines []domain.KLine) error {
	if m.SaveFunc != nil {
		return m.SaveFunc(ctx, klines)
	}
	return nil
}

func (m *MockRepository) GetLastDate(ctx context.Context, symbol string) (time.Time, error) {
	if m.GetLastDateFunc != nil {
		return m.GetLastDateFunc(ctx, symbol)
	}
	return time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), nil
}

func (m *MockRepository) GetFirstDate(ctx context.Context, symbol string) (time.Time, error) {
	if m.GetFirstDateFunc != nil {
		return m.GetFirstDateFunc(ctx, symbol)
	}
	return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), nil
}

func (m *MockRepository) ValidateData(ctx context.Context, symbol string, date time.Time) (*domain.ValidationResult, error) {
	if m.ValidateDataFunc != nil {
		return m.ValidateDataFunc(ctx, symbol, date)
	}
	return &domain.ValidationResult{
		Valid:      true,
		TotalRows:  1440,
		ValidRows:  1440,
		InvalidRows: 0,
		Errors:     []string{},
		Warnings:   []string{},
	}, nil
}

func (m *MockRepository) ClearAllData(ctx context.Context) error {
	if m.ClearAllDataFunc != nil {
		return m.ClearAllDataFunc(ctx)
	}
	return nil
}

func (m *MockRepository) CreateMaterializedViews(ctx context.Context, intervals []string) error {
	if m.CreateMaterializedViewsFunc != nil {
		return m.CreateMaterializedViewsFunc(ctx, intervals)
	}
	return nil
}

func (m *MockRepository) RefreshMaterializedViews(ctx context.Context) error {
	if m.RefreshMaterializedViewsFunc != nil {
		return m.RefreshMaterializedViewsFunc(ctx)
	}
	return nil
}

func (m *MockRepository) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}