package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/test/mocks"
	"binance-data-loader/test/testutils"
)

func TestNewScheduler(t *testing.T) {
	cfg := config.SchedulerConfig{
		EndDate:               "2024-01-31",
		BatchDays:             7,
		MaxConcurrentSymbols:  5,
	}
	
	mockDownloader := &mocks.MockDownloader{}
	mockImporter := &mocks.MockImporter{}
	mockStateManager := &mocks.MockStateManager{}
	mockProgressReporter := &mocks.MockProgressReporter{}
	mockRepository := &mocks.MockRepository{}

	scheduler := NewScheduler(
		cfg,
		mockDownloader,
		mockImporter,
		mockStateManager,
		mockProgressReporter,
		mockRepository,
	)
	
	if scheduler == nil {
		t.Fatal("Expected scheduler to be created, got nil")
	}
	
	if scheduler.config.MaxConcurrentSymbols != 5 {
		t.Errorf("Expected MaxConcurrentSymbols 5, got %d", scheduler.config.MaxConcurrentSymbols)
	}
	
	if scheduler.config.BatchDays != 7 {
		t.Errorf("Expected BatchDays 7, got %d", scheduler.config.BatchDays)
	}
}

func TestScheduler_getSymbols(t *testing.T) {
	tests := []struct {
		name           string
		mockFunc       func(ctx context.Context) ([]string, error)
		expectedSymbols []string
		expectError    bool
	}{
		{
			name: "successful_fetch",
			mockFunc: func(ctx context.Context) ([]string, error) {
				return []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"}, nil
			},
			expectedSymbols: []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"},
			expectError:     false,
		},
		{
			name: "fetch_failed_fallback",
			mockFunc: func(ctx context.Context) ([]string, error) {
				return nil, errors.New("network error")
			},
			expectedSymbols: []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"}, // fallback symbols
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDownloader := &mocks.MockDownloader{
				GetSymbolsFunc: tt.mockFunc,
			}
			
			scheduler := &Scheduler{
				downloader: mockDownloader,
			}
			
			ctx := testutils.WithTimeout(t, 5*time.Second)
			symbols, err := scheduler.getSymbols(ctx)
			
			if tt.expectError {
				testutils.AssertError(t, err)
			} else {
				testutils.AssertNoError(t, err)
				
				if len(symbols) != len(tt.expectedSymbols) {
					t.Errorf("Expected %d symbols, got %d", len(tt.expectedSymbols), len(symbols))
				}
			}
		})
	}
}

func TestScheduler_generateTasksWithEndDate(t *testing.T) {
	symbols := []string{"BTCUSDT", "ETHUSDT"}
	endDate := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	
	mockDownloader := &mocks.MockDownloader{
		GetAvailableDatesFunc: func(ctx context.Context, symbol string) ([]time.Time, error) {
			return []time.Time{
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			}, nil
		},
	}
	
	mockStateManager := &mocks.MockStateManager{
		GetStateFunc: func(symbol string) (*domain.ProcessingState, error) {
			return &domain.ProcessingState{
				Symbol:   symbol,
				LastDate: time.Time{}, // 未处理过
				Status:   "pending",
			}, nil
		},
	}
	
	scheduler := &Scheduler{
		downloader:   mockDownloader,
		stateManager: mockStateManager,
	}
	
	ctx := testutils.WithTimeout(t, 5*time.Second)
	tasks, err := scheduler.generateTasksWithEndDate(ctx, symbols, endDate)
	
	testutils.AssertNoError(t, err)
	
	if len(tasks) == 0 {
		t.Error("Expected some tasks to be generated")
	}
	
	// 验证任务按代币排序
	lastSymbol := ""
	for _, task := range tasks {
		if lastSymbol != "" && task.Symbol < lastSymbol {
			t.Error("Tasks should be sorted by symbol")
		}
		lastSymbol = task.Symbol
	}
}

func TestScheduler_generateTasksForSymbolWithEndDate(t *testing.T) {
	symbol := "BTCUSDT"
	endDate := time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC)
	
	mockDownloader := &mocks.MockDownloader{
		GetAvailableDatesFunc: func(ctx context.Context, symbol string) ([]time.Time, error) {
			return []time.Time{
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			}, nil
		},
	}
	
	mockStateManager := &mocks.MockStateManager{
		GetStateFunc: func(symbol string) (*domain.ProcessingState, error) {
			return &domain.ProcessingState{
				Symbol:   symbol,
				LastDate: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), // 已处理到1月15日
				Status:   "processing",
			}, nil
		},
	}
	
	scheduler := &Scheduler{
		downloader:   mockDownloader,
		stateManager: mockStateManager,
	}
	
	ctx := testutils.WithTimeout(t, 5*time.Second)
	tasks, err := scheduler.generateTasksForSymbolWithEndDate(ctx, symbol, endDate)
	
	testutils.AssertNoError(t, err)
	
	// 应该生成所有3个月的任务（1月、2月、3月），因为1月15日的处理不表示整个月已完成
	expectedTasks := 3
	if len(tasks) != expectedTasks {
		t.Errorf("Expected %d tasks, got %d", expectedTasks, len(tasks))
	}
	
	// 验证任务按时间顺序排列
	if len(tasks) >= 2 {
		if !tasks[0].Date.Before(tasks[1].Date) {
			t.Error("Tasks should be sorted chronologically")
		}
	}
}

func TestScheduler_processTasks(t *testing.T) {
	tasks := []domain.DownloadTask{
		{Symbol: "BTCUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Symbol: "BTCUSDT", Date: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)},
		{Symbol: "ETHUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	
	processedTasks := [][]domain.DownloadTask{}
	var mu sync.Mutex
	
	mockImporter := &mocks.MockImporter{
		ImportDataFunc: func(ctx context.Context, tasks []domain.DownloadTask) error {
			mu.Lock()
			processedTasks = append(processedTasks, tasks)
			mu.Unlock()
			return nil
		},
	}
	
	scheduler := &Scheduler{
		importer: mockImporter,
	}
	
	ctx := testutils.WithTimeout(t, 5*time.Second)
	err := scheduler.processTasks(ctx, tasks)
	
	testutils.AssertNoError(t, err)
	
	// 验证每个代币的任务被单独处理
	if len(processedTasks) != 2 { // BTCUSDT和ETHUSDT
		t.Errorf("Expected 2 symbol groups, got %d", len(processedTasks))
	}
	
	// 验证任务按代币分组
	symbolGroups := make(map[string]int)
	for _, group := range processedTasks {
		if len(group) > 0 {
			symbolGroups[group[0].Symbol] = len(group)
		}
	}
	
	if symbolGroups["BTCUSDT"] != 2 {
		t.Errorf("Expected 2 BTCUSDT tasks, got %d", symbolGroups["BTCUSDT"])
	}
	
	if symbolGroups["ETHUSDT"] != 1 {
		t.Errorf("Expected 1 ETHUSDT task, got %d", symbolGroups["ETHUSDT"])
	}
}

func TestScheduler_processTasksConcurrent(t *testing.T) {
	tasks := []domain.DownloadTask{
		{Symbol: "BTCUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Symbol: "ETHUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Symbol: "ADAUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Symbol: "BNBUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Symbol: "XRPUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Symbol: "DOTUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	
	processedCount := 0
	concurrentCount := 0
	maxConcurrent := 0
	var mu sync.Mutex
	
	mockImporter := &mocks.MockImporter{
		ImportDataFunc: func(ctx context.Context, tasks []domain.DownloadTask) error {
			mu.Lock()
			concurrentCount++
			if concurrentCount > maxConcurrent {
				maxConcurrent = concurrentCount
			}
			mu.Unlock()
			
			// 模拟处理时间
			time.Sleep(100 * time.Millisecond)
			
			mu.Lock()
			processedCount++
			concurrentCount--
			mu.Unlock()
			
			return nil
		},
	}
	
	scheduler := &Scheduler{
		importer: mockImporter,
	}
	
	ctx := testutils.WithTimeout(t, 10*time.Second)
	err := scheduler.processTasksConcurrent(ctx, tasks)
	
	testutils.AssertNoError(t, err)
	
	// 验证所有任务都被处理
	if processedCount != 6 { // 6个不同的代币
		t.Errorf("Expected 6 symbols processed, got %d", processedCount)
	}
	
	// 验证并发数不超过5
	if maxConcurrent > 5 {
		t.Errorf("Expected max concurrent <= 5, got %d", maxConcurrent)
	}
}

func TestScheduler_Run(t *testing.T) {
	endDate := "2024-01-31"
	
	mockDownloader := &mocks.MockDownloader{
		GetSymbolsFunc: func(ctx context.Context) ([]string, error) {
			return []string{"BTCUSDT"}, nil
		},
		GetAvailableDatesFunc: func(ctx context.Context, symbol string) ([]time.Time, error) {
			return []time.Time{
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			}, nil
		},
	}
	
	mockStateManager := &mocks.MockStateManager{
		GetStateFunc: func(symbol string) (*domain.ProcessingState, error) {
			return &domain.ProcessingState{
				Symbol:   symbol,
				LastDate: time.Time{},
				Status:   "pending",
			}, nil
		},
	}
	
	mockImporter := &mocks.MockImporter{
		ImportDataFunc: func(ctx context.Context, tasks []domain.DownloadTask) error {
			return nil
		},
	}
	
	mockProgressReporter := &mocks.MockProgressReporter{
		StartFunc: func(totalTasks int) error {
			return nil
		},
	}
	
	mockRepository := &mocks.MockRepository{
		CreateMaterializedViewsFunc: func(ctx context.Context, intervals []string) error {
			return nil
		},
		RefreshMaterializedViewsFunc: func(ctx context.Context) error {
			return nil
		},
	}
	
	scheduler := NewScheduler(
		config.SchedulerConfig{
			EndDate:               endDate,
			BatchDays:             7,
			MaxConcurrentSymbols:  5,
		},
		mockDownloader,
		mockImporter,
		mockStateManager,
		mockProgressReporter,
		mockRepository,
	)
	
	ctx := testutils.WithTimeout(t, 10*time.Second)
	err := scheduler.Run(ctx)
	
	testutils.AssertNoError(t, err)
}

func TestScheduler_RunConcurrent(t *testing.T) {
	mockDownloader := &mocks.MockDownloader{
		GetSymbolsFunc: func(ctx context.Context) ([]string, error) {
			return []string{"BTCUSDT", "ETHUSDT"}, nil
		},
		GetAvailableDatesFunc: func(ctx context.Context, symbol string) ([]time.Time, error) {
			return []time.Time{
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			}, nil
		},
	}
	
	mockStateManager := &mocks.MockStateManager{
		GetStateFunc: func(symbol string) (*domain.ProcessingState, error) {
			return &domain.ProcessingState{
				Symbol:   symbol,
				LastDate: time.Time{},
				Status:   "pending",
			}, nil
		},
	}
	
	processedSymbols := make(map[string]bool)
	var mu sync.Mutex
	
	mockImporter := &mocks.MockImporter{
		ImportDataFunc: func(ctx context.Context, tasks []domain.DownloadTask) error {
			if len(tasks) > 0 {
				mu.Lock()
				processedSymbols[tasks[0].Symbol] = true
				mu.Unlock()
			}
			return nil
		},
	}
	
	mockProgressReporter := &mocks.MockProgressReporter{}
	mockRepository := &mocks.MockRepository{}
	
	scheduler := NewScheduler(
		config.SchedulerConfig{
			EndDate:               "",
			BatchDays:             7,
			MaxConcurrentSymbols:  5,
		},
		mockDownloader,
		mockImporter,
		mockStateManager,
		mockProgressReporter,
		mockRepository,
	)
	
	ctx := testutils.WithTimeout(t, 10*time.Second)
	err := scheduler.RunConcurrent(ctx)
	
	testutils.AssertNoError(t, err)
	
	// 验证两个代币都被处理
	if !processedSymbols["BTCUSDT"] {
		t.Error("Expected BTCUSDT to be processed")
	}
	if !processedSymbols["ETHUSDT"] {
		t.Error("Expected ETHUSDT to be processed")
	}
}

func TestScheduler_UpdateToLatest(t *testing.T) {
	mockDownloader := &mocks.MockDownloader{
		GetSymbolsFunc: func(ctx context.Context) ([]string, error) {
			return []string{"BTCUSDT"}, nil
		},
		GetAvailableDatesFunc: func(ctx context.Context, symbol string) ([]time.Time, error) {
			return []time.Time{
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			}, nil
		},
	}
	
	mockStateManager := &mocks.MockStateManager{
		GetAllStatesFunc: func() (map[string]*domain.ProcessingState, error) {
			return map[string]*domain.ProcessingState{
				"BTCUSDT": {
					Symbol:   "BTCUSDT",
					LastDate: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
					Status:   "completed",
				},
			}, nil
		},
	}
	
	updatedTasks := 0
	mockImporter := &mocks.MockImporter{
		ImportDataFunc: func(ctx context.Context, tasks []domain.DownloadTask) error {
			updatedTasks = len(tasks)
			return nil
		},
	}
	
	mockProgressReporter := &mocks.MockProgressReporter{}
	
	scheduler := NewScheduler(
		config.SchedulerConfig{},
		mockDownloader,
		mockImporter,
		mockStateManager,
		mockProgressReporter,
		nil,
	)
	
	ctx := testutils.WithTimeout(t, 5*time.Second)
	err := scheduler.UpdateToLatest(ctx)
	
	testutils.AssertNoError(t, err)
	
	// 应该有更新任务（2月份数据）
	if updatedTasks == 0 {
		t.Error("Expected some update tasks to be processed")
	}
}

func TestScheduler_ValidateData(t *testing.T) {
	symbols := []string{"BTCUSDT"}
	endDate := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	
	mockRepository := &mocks.MockRepository{
		GetFirstDateFunc: func(ctx context.Context, symbol string) (time.Time, error) {
			return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), nil
		},
		ValidateDataFunc: func(ctx context.Context, symbol string, date time.Time) (*domain.ValidationResult, error) {
			return &domain.ValidationResult{
				Valid:       true,
				TotalRows:   1440,
				ValidRows:   1440,
				InvalidRows: 0,
				Errors:      []string{},
				Warnings:    []string{},
			}, nil
		},
	}
	
	scheduler := &Scheduler{
		repository: mockRepository,
	}
	
	ctx := testutils.WithTimeout(t, 5*time.Second)
	err := scheduler.ValidateData(ctx, symbols, endDate)
	
	testutils.AssertNoError(t, err)
}

func TestScheduler_Stop(t *testing.T) {
	mockProgressReporter := &mocks.MockProgressReporter{
		StopFunc: func(ctx context.Context) error {
			return nil
		},
	}
	
	mockImporter := &mocks.MockImporter{
		CloseFunc: func() error {
			return nil
		},
	}
	
	scheduler := &Scheduler{
		progressReporter: mockProgressReporter,
		importer:         mockImporter,
	}
	
	ctx := testutils.WithTimeout(t, 5*time.Second)
	err := scheduler.Stop(ctx)
	
	testutils.AssertNoError(t, err)
}

func TestScheduler_GetProgress(t *testing.T) {
	mockProgressReporter := &mocks.MockProgressReporter{
		GetOverallProgressFunc: func() map[string]interface{} {
			return map[string]interface{}{
				"completed": 75,
				"total":     100,
				"progress":  75.0,
			}
		},
	}
	
	scheduler := &Scheduler{
		progressReporter: mockProgressReporter,
	}
	
	progress := scheduler.GetProgress()
	
	if progress == nil {
		t.Fatal("Expected progress report, got nil")
	}
	
	if progress["completed"] != 75 {
		t.Errorf("Expected completed 75, got %v", progress["completed"])
	}
}

func TestScheduler_min(t *testing.T) {
	tests := []struct {
		a, b, expected int
	}{
		{1, 2, 1},
		{5, 3, 3},
		{0, 0, 0},
		{-1, 1, -1},
		{10, 10, 10},
	}
	
	for _, test := range tests {
		result := min(test.a, test.b)
		if result != test.expected {
			t.Errorf("min(%d, %d) = %d, expected %d", test.a, test.b, result, test.expected)
		}
	}
}

// 基准测试
func BenchmarkScheduler_min(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = min(i, i+1)
	}
}

func BenchmarkScheduler_processTasks(b *testing.B) {
	tasks := []domain.DownloadTask{
		{Symbol: "BTCUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Symbol: "ETHUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	
	mockImporter := &mocks.MockImporter{
		ImportDataFunc: func(ctx context.Context, tasks []domain.DownloadTask) error {
			return nil
		},
	}
	
	scheduler := &Scheduler{
		importer: mockImporter,
	}
	
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = scheduler.processTasks(ctx, tasks)
	}
}
