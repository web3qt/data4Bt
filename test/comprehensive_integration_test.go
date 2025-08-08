package main

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/state"
	"binance-data-loader/pkg/binance"
	"binance-data-loader/pkg/clickhouse"
	"binance-data-loader/pkg/importer"
	"binance-data-loader/pkg/parser"
	"binance-data-loader/test/testutils"
)

// TestFullPipeline 测试完整的数据处理管道
func TestFullPipeline(t *testing.T) {
	testutils.SkipIfNoDatabase(t)
	
	tempDir := testutils.CreateTempDir(t, "integration_test")
	
	// 创建测试配置
	cfg := createIntegrationTestConfig(tempDir)
	
	ctx := testutils.WithTimeout(t, 60*time.Second)
	
	// 创建组件
	repo, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	testutils.AssertNoError(t, err)
	defer func() {
		repo.ClearAllData(ctx)
		repo.Close()
	}()
	
	stateManager, err := state.NewFileStateManager(cfg.State)
	testutils.AssertNoError(t, err)
	
	downloader := binance.NewBinanceDownloader(cfg.Binance, cfg.Downloader)
	csvParser := parser.NewCSVParser(cfg.Parser)
	_ = importer.NewImporter(cfg.Importer, downloader, csvParser, repo, stateManager, nil)
	
	// 初始化数据库
	err = repo.CreateTables(ctx)
	testutils.AssertNoError(t, err)
	
	// 设置测试符号（模拟场景）
	testSymbols := []string{"BTCUSDT"}
	
	// 生成任务（模拟场景）
	var tasks []domain.DownloadTask
	for _, symbol := range testSymbols {
		task := domain.DownloadTask{
			Symbol: symbol,
			Date:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		tasks = append(tasks, task)
	}
	
	// 由于网络限制，我们模拟数据而不是真正下载
	mockCSVData := testutils.CreateMockCSVData()
	
	// 解析数据
	parseCtx := testutils.WithTimeout(t, 60*time.Second)
	klines, validationResult, err := csvParser.Parse(parseCtx, []byte(mockCSVData), "BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if !validationResult.Valid {
		t.Fatalf("CSV validation failed: %+v", validationResult)
	}
	
	// 保存数据
	err = repo.Save(ctx, klines)
	testutils.AssertNoError(t, err)
	
	// 验证数据被保存
	lastDate, err := repo.GetLastDate(ctx, "BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if lastDate.IsZero() {
		t.Error("Expected non-zero last date")
	}
	
	// 验证状态管理
	testState := &domain.ProcessingState{
		Symbol:      "BTCUSDT",
		LastDate:    lastDate,
		Status:      "completed",
		Processed:   len(klines),
		LastUpdated: time.Now(),
	}
	
	err = stateManager.SaveState(testState)
	testutils.AssertNoError(t, err)
	
	retrievedState, err := stateManager.GetState("BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if retrievedState.Status != "completed" {
		t.Errorf("Expected status completed, got %s", retrievedState.Status)
	}
	
	if retrievedState.Processed != len(klines) {
		t.Errorf("Expected processed %d, got %d", len(klines), retrievedState.Processed)
	}
	
	// 创建物化视图
	err = repo.CreateMaterializedViews(ctx, []string{"5m", "1h"})
	testutils.AssertNoError(t, err)
	
	// 刷新物化视图
	err = repo.RefreshMaterializedViews(ctx)
	testutils.AssertNoError(t, err)
	
	t.Logf("Full pipeline test completed successfully")
}

// TestConcurrentProcessing 测试并发处理
func TestConcurrentProcessing(t *testing.T) {
	testutils.SkipIfNoDatabase(t)
	
	tempDir := testutils.CreateTempDir(t, "concurrent_test")
	cfg := createIntegrationTestConfig(tempDir)
	cfg.Scheduler.MaxConcurrentSymbols = 3
	
	ctx := testutils.WithTimeout(t, 60*time.Second)
	
	// 创建组件
	repo, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	testutils.AssertNoError(t, err)
	defer func() {
		repo.ClearAllData(ctx)
		repo.Close()
	}()
	
	stateManager, err := state.NewFileStateManager(cfg.State)
	testutils.AssertNoError(t, err)
	
	// 初始化数据库
	err = repo.CreateTables(ctx)
	testutils.AssertNoError(t, err)
	
	// 模拟多个符号的并发处理
	symbols := []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"}
	
	// 为每个符号创建状态
	for _, symbol := range symbols {
		state := &domain.ProcessingState{
			Symbol:      symbol,
			Status:      "pending",
			LastUpdated: time.Now(),
		}
		err = stateManager.SaveState(state)
		testutils.AssertNoError(t, err)
	}
	
	// 并发处理状态更新
	done := make(chan bool, len(symbols))
	
	for _, symbol := range symbols {
		go func(sym string) {
			defer func() { done <- true }()
			
			// 模拟处理过程
			for i := 0; i < 5; i++ {
				state, err := stateManager.GetState(sym)
				if err != nil {
					t.Errorf("Error getting state for %s: %v", sym, err)
					return
				}
				
				state.Processed = i * 100
				state.Status = "processing"
				state.LastUpdated = time.Now()
				
				err = stateManager.SaveState(state)
				if err != nil {
					t.Errorf("Error saving state for %s: %v", sym, err)
					return
				}
				
				time.Sleep(10 * time.Millisecond) // 模拟处理时间
			}
			
			// 标记为完成
			finalState, _ := stateManager.GetState(sym)
			finalState.Status = "completed"
			finalState.Processed = 500
			stateManager.SaveState(finalState)
		}(symbol)
	}
	
	// 等待所有处理完成
	for range symbols {
		<-done
	}
	
	// 验证最终状态
	allStates, err := stateManager.GetAllStates()
	testutils.AssertNoError(t, err)
	
	if len(allStates) != len(symbols) {
		t.Errorf("Expected %d states, got %d", len(symbols), len(allStates))
	}
	
	for _, symbol := range symbols {
		state, exists := allStates[symbol]
		if !exists {
			t.Errorf("Expected state for symbol %s to exist", symbol)
			continue
		}
		
		if state.Status != "completed" {
			t.Errorf("Expected status completed for %s, got %s", symbol, state.Status)
		}
		
		if state.Processed != 500 {
			t.Errorf("Expected processed 500 for %s, got %d", symbol, state.Processed)
		}
	}
	
	t.Logf("Concurrent processing test completed successfully")
}

// TestErrorRecovery 测试错误恢复机制
func TestErrorRecovery(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "error_recovery_test")
	
	cfg := createIntegrationTestConfig(tempDir)
	stateManager, err := state.NewFileStateManager(cfg.State)
	testutils.AssertNoError(t, err)
	
	// 保存初始状态
	initialState := &domain.ProcessingState{
		Symbol:      "BTCUSDT",
		Status:      "processing",
		Processed:   100,
		Failed:      5,
		LastUpdated: time.Now(),
	}
	
	err = stateManager.SaveState(initialState)
	testutils.AssertNoError(t, err)
	
	// 创建备份
	err = stateManager.Backup()
	testutils.AssertNoError(t, err)
	
	// 模拟处理过程中的错误（修改状态）
	errorState := &domain.ProcessingState{
		Symbol:       "BTCUSDT",
		Status:       "failed",
		Processed:    50,
		Failed:       20,
		ErrorMessage: "Simulated error",
		LastUpdated:  time.Now(),
	}
	
	err = stateManager.SaveState(errorState)
	testutils.AssertNoError(t, err)
	
	// 验证错误状态被保存
	currentState, err := stateManager.GetState("BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if currentState.Status != "failed" {
		t.Errorf("Expected status failed, got %s", currentState.Status)
	}
	
	if currentState.Processed != 50 {
		t.Errorf("Expected processed 50, got %d", currentState.Processed)
	}
	
	// 恢复备份
	backupFiles, err := filepath.Glob(cfg.State.FilePath + ".backup_*")
	testutils.AssertNoError(t, err)
	
	if len(backupFiles) == 0 {
		t.Fatal("No backup files found")
	}
	
	err = stateManager.Restore(backupFiles[0])
	testutils.AssertNoError(t, err)
	
	// 验证状态被恢复
	restoredState, err := stateManager.GetState("BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if restoredState.Status != "processing" {
		t.Errorf("Expected status processing after restore, got %s", restoredState.Status)
	}
	
	if restoredState.Processed != 100 {
		t.Errorf("Expected processed 100 after restore, got %d", restoredState.Processed)
	}
	
	if restoredState.Failed != 5 {
		t.Errorf("Expected failed 5 after restore, got %d", restoredState.Failed)
	}
	
	t.Logf("Error recovery test completed successfully")
}

// TestDataValidation 测试数据验证
func TestDataValidation(t *testing.T) {
	testutils.SkipIfNoDatabase(t)
	
	tempDir := testutils.CreateTempDir(t, "validation_test")
	cfg := createIntegrationTestConfig(tempDir)
	
	ctx := testutils.WithTimeout(t, 30*time.Second)
	
	// 创建仓库
	repo, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	testutils.AssertNoError(t, err)
	defer func() {
		repo.ClearAllData(ctx)
		repo.Close()
	}()
	
	// 初始化数据库
	err = repo.CreateTables(ctx)
	testutils.AssertNoError(t, err)
	
	// 创建解析器
	csvParser := parser.NewCSVParser(cfg.Parser)
	
	// 测试有效数据
	validCSV := testutils.CreateMockCSVData()
	klines, validationResult, err := csvParser.Parse(ctx, []byte(validCSV), "BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if !validationResult.Valid {
		t.Errorf("Expected valid CSV data to pass validation")
	}
	
	if len(klines) == 0 {
		t.Error("Expected some klines to be parsed")
	}
	
	// 保存有效数据
	err = repo.Save(ctx, klines)
	testutils.AssertNoError(t, err)
	
	// 验证保存的数据
	testDate := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := repo.ValidateData(ctx, "BTCUSDT", testDate)
	testutils.AssertNoError(t, err)
	
	if !result.Valid {
		t.Errorf("Expected saved data to be valid: %+v", result)
	}
	
	if result.TotalRows == 0 {
		t.Error("Expected some rows to be validated")
	}
	
	// 测试无效数据
	invalidCSV := `invalid,csv,format
	missing,columns
	wrong,data,types,here`
	
	_, invalidResult, err := csvParser.Parse(ctx, []byte(invalidCSV), "INVALID")
	// 解析器应该能处理无效数据并返回验证结果
	testutils.AssertNoError(t, err)
	
	if invalidResult.Valid {
		t.Error("Expected invalid CSV data to fail validation")
	}
	
	if len(invalidResult.Errors) == 0 {
		t.Error("Expected validation errors for invalid data")
	}
	
	t.Logf("Data validation test completed successfully")
}

// TestLongRunningProcess 测试长时间运行的处理过程
func TestLongRunningProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long running test in short mode")
	}
	
	tempDir := testutils.CreateTempDir(t, "long_running_test")
	cfg := createIntegrationTestConfig(tempDir)
	
	stateManager, err := state.NewFileStateManager(cfg.State)
	testutils.AssertNoError(t, err)
	
	// 模拟长时间处理过程
	symbol := "BTCUSDT"
	totalTasks := 100
	processedTasks := 0
	
	// 创建初始状态
	state := &domain.ProcessingState{
		Symbol:      symbol,
		Status:      "processing",
		TotalFiles:  totalTasks,
		Processed:   0,
		Failed:      0,
		LastUpdated: time.Now(),
	}
	
	err = stateManager.SaveState(state)
	testutils.AssertNoError(t, err)
	
	// 模拟逐步处理
	ctx := testutils.WithTimeout(t, 30*time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			t.Fatal("Test timed out")
		case <-ticker.C:
			processedTasks++
			
			// 更新状态
			state.Processed = processedTasks
			state.ProgressPercent = float64(processedTasks) / float64(totalTasks) * 100
			state.LastUpdated = time.Now()
			
			if processedTasks%10 == 0 { // 模拟偶发错误
				state.Failed++
			}
			
			if processedTasks >= totalTasks {
				state.Status = "completed"
			}
			
			err = stateManager.SaveState(state)
			testutils.AssertNoError(t, err)
			
			// 验证状态持久化
			retrievedState, err := stateManager.GetState(symbol)
			testutils.AssertNoError(t, err)
			
			if retrievedState.Processed != processedTasks {
				t.Errorf("Expected processed %d, got %d", processedTasks, retrievedState.Processed)
			}
			
			if processedTasks >= totalTasks {
				break
			}
		}
	}
	
	// 验证最终状态
	finalState, err := stateManager.GetState(symbol)
	testutils.AssertNoError(t, err)
	
	if finalState.Status != "completed" {
		t.Errorf("Expected final status completed, got %s", finalState.Status)
	}
	
	if finalState.Processed != totalTasks {
		t.Errorf("Expected final processed %d, got %d", totalTasks, finalState.Processed)
	}
	
	if finalState.ProgressPercent != 100.0 {
		t.Errorf("Expected final progress 100.0, got %f", finalState.ProgressPercent)
	}
	
	t.Logf("Long running process test completed successfully")
}

// TestSystemResourceUsage 测试系统资源使用情况
func TestSystemResourceUsage(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "resource_test")
	cfg := createIntegrationTestConfig(tempDir)
	
	// 创建多个组件实例来测试资源使用
	stateManagers := make([]*state.FileStateManager, 10)
	var err error
	
	// 创建多个状态管理器实例
	for i := 0; i < 10; i++ {
		cfg.State.FilePath = filepath.Join(tempDir, fmt.Sprintf("progress_%d.json", i))
		stateManagers[i], err = state.NewFileStateManager(cfg.State)
		testutils.AssertNoError(t, err)
	}
	
	// 并发写入大量状态
	symbols := []string{"BTCUSDT", "ETHUSDT", "ADAUSDT", "BNBUSDT", "XRPUSDT"}
	
	start := time.Now()
	
	for i, manager := range stateManagers {
		for j, symbol := range symbols {
			state := &domain.ProcessingState{
				Symbol:      fmt.Sprintf("%s_%d", symbol, i),
				Status:      "processing",
				Processed:   j * 100,
				Failed:      j,
				LastUpdated: time.Now(),
			}
			
			err = manager.SaveState(state)
			testutils.AssertNoError(t, err)
		}
	}
	
	duration := time.Since(start)
	t.Logf("Created %d states in %v", len(stateManagers)*len(symbols), duration)
	
	// 验证所有状态都能正确读取
	for i, manager := range stateManagers {
		allStates, err := manager.GetAllStates()
		testutils.AssertNoError(t, err)
		
		if len(allStates) != len(symbols) {
			t.Errorf("Manager %d: Expected %d states, got %d", i, len(symbols), len(allStates))
		}
	}
	
	t.Logf("System resource usage test completed successfully")
}

// 辅助函数创建集成测试配置
func createIntegrationTestConfig(tempDir string) *config.Config {
	return &config.Config{
		Log: config.LogConfig{
			Level:  "info",
			Format: "console",
			Output: "stdout",
		},
		Binance: config.BinanceConfig{
			BaseURL:        "https://data.binance.vision",
			DataPath:       "/data/spot/monthly/klines",
			SymbolsFilter:  "USDT",
			Interval:       "1m",
			Timeout:        30 * time.Second,
			RetryCount:     3,
			RetryDelay:     5 * time.Second,
		},
		Downloader: config.DownloaderConfig{
			Concurrency:       5,
			BufferSize:        50,
			UserAgent:         "IntegrationTest/1.0",
			EnableCompression: true,
			MaxFileSize:       "100MB",
		},
		Parser: config.ParserConfig{
			Concurrency:     3,
			BufferSize:      25,
			ValidateData:    true,
			SkipInvalidRows: true,
		},
		Database: config.DatabaseConfig{
			ClickHouse: config.ClickHouseConfig{
				Hosts:           []string{"localhost:9000"},
				Database:        "data4BT_integration_test",
				Username:        "default",
				Password:        "",
				Compression:     "lz4",
				DialTimeout:     30 * time.Second,
				MaxOpenConns:    5,
				MaxIdleConns:    2,
				ConnMaxLifetime: 5 * time.Minute,
				Settings:        map[string]int{
					"max_execution_time": 60,
				},
			},
		},
		Importer: config.ImporterConfig{
			BatchSize:           1000,
			BufferSize:          10,
			FlushInterval:       10 * time.Second,
			EnableDeduplication: true,
		},
		State: config.StateConfig{
			StorageType:  "file",
			FilePath:     filepath.Join(tempDir, "progress.json"),
			BackupCount:  3,
			SaveInterval: 5 * time.Second,
		},
		MaterializedViews: config.MaterializedViewsConfig{
			Enabled:         true,
			Intervals:       []string{"5m", "1h"},
			RefreshInterval: 30 * time.Second,
			ParallelRefresh: true,
		},
		Monitoring: config.MonitoringConfig{
			Enabled:                false, // 集成测试中禁用监控
			MetricsPort:            0,
			HealthCheckPort:        0,
			ProgressReportInterval: 10 * time.Second,
		},
		Scheduler: config.SchedulerConfig{
			EndDate:               "",
			BatchDays:             1,
			MaxConcurrentSymbols:  2,
		},
	}
}
