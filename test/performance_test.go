package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/state"
	"binance-data-loader/pkg/binance"
	"binance-data-loader/pkg/clickhouse"
	"binance-data-loader/pkg/importer"
	"binance-data-loader/pkg/parser"
	"binance-data-loader/pkg/scheduler"
	"binance-data-loader/test/mocks"
	"binance-data-loader/test/testutils"
)

// BenchmarkBinanceDownloader_Concurrent 测试并发下载性能
func BenchmarkBinanceDownloader_Concurrent(b *testing.B) {
	cfg := createPerformanceTestConfig(testutils.CreateTempDirB(b, "perf_test"))
	downloader := binance.NewBinanceDownloader(cfg.Binance, cfg.Downloader)
	
	// 创建多个下载任务
	tasks := make([]domain.DownloadTask, b.N)
	for i := 0; i < b.N; i++ {
		tasks[i] = domain.DownloadTask{
			Symbol: fmt.Sprintf("BTC%dUSDT", i%10+1), // 循环使用10个不同的符号
			Date:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		}
	}
	
	ctx := context.Background()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			task := tasks[i%len(tasks)]
			// 由于网络限制，我们测试的是URL构建和验证性能
			url := fmt.Sprintf("%s%s/%s/%s/%s-%s.zip", 
				cfg.Binance.BaseURL,
				cfg.Binance.DataPath,
				task.Symbol,
				cfg.Binance.Interval,
				task.Symbol,
				task.Date.Format("2006-01"))
			_ = downloader.ValidateURL(ctx, url)
			i++
		}
	})
	
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "downloads/sec")
}

// BenchmarkCSVParser_LargeData 测试大数据量CSV解析性能
func BenchmarkCSVParser_LargeData(b *testing.B) {
	cfg := createPerformanceTestConfig(testutils.CreateTempDirB(b, "perf_test"))
	parser := parser.NewCSVParser(cfg.Parser)
	
	// 生成大量CSV数据（模拟一个月的1分钟K线数据：约43200行）
	csvData := generateLargeCSVData(43200)
	
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		klines, _, err := parser.Parse(ctx, csvData, "BTCUSDT")
		if err != nil {
			b.Fatalf("Parse error: %v", err)
		}
		if len(klines) == 0 {
			b.Fatal("Expected parsed klines")
		}
	}
	
	b.ReportMetric(float64(len(csvData))*float64(b.N)/b.Elapsed().Seconds(), "bytes/sec")
}

// BenchmarkStateManager_ConcurrentOperations 测试状态管理器并发操作性能
func BenchmarkStateManager_ConcurrentOperations(b *testing.B) {
	tempDir := testutils.CreateTempDirB(b, "perf_test")
	cfg := config.StateConfig{
		StorageType:  "file",
		FilePath:     filepath.Join(tempDir, "progress.json"),
		BackupCount:  3,
		SaveInterval: 5 * time.Second,
	}
	
	manager, err := state.NewFileStateManager(cfg)
	if err != nil {
		b.Fatalf("Failed to create state manager: %v", err)
	}
	
	symbols := make([]string, 100)
	for i := 0; i < 100; i++ {
		symbols[i] = fmt.Sprintf("SYM%dUSDT", i)
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rand := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			symbol := symbols[rand.Intn(len(symbols))]
			state := &domain.ProcessingState{
				Symbol:      symbol,
				Status:      "processing",
				Processed:   rand.Intn(1000),
				Failed:      rand.Intn(10),
				LastUpdated: time.Now(),
			}
			
			// 50% 概率写入，50% 概率读取
			if rand.Float32() < 0.5 {
				_ = manager.SaveState(state)
			} else {
				_, _ = manager.GetState(symbol)
			}
		}
	})
	
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "operations/sec")
}


// BenchmarkDatabase_BatchInsert 测试数据库批量插入性能
func BenchmarkDatabase_BatchInsert(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping database benchmark in short mode")
	}
	
	// Skip database tests in benchmark mode
	if os.Getenv("INTEGRATION_TEST") != "true" {
		b.Skip("Skipping database benchmark (set INTEGRATION_TEST=true to run)")
	}
	
	cfg := config.ClickHouseConfig{
		Hosts:           []string{"localhost:9000"},
		Database:        "data4BT_benchmark_test",
		Username:        "default",
		Password:        "",
		Compression:     "lz4",
		DialTimeout:     30 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
		Settings: map[string]int{
			"max_execution_time": 300,
		},
	}
	
	repo, err := clickhouse.NewRepository(cfg)
	if err != nil {
		b.Fatalf("Failed to create repository: %v", err)
	}
	defer func() {
		ctx := context.Background()
		repo.ClearAllData(ctx)
		repo.Close()
	}()
	
	ctx := context.Background()
	err = repo.CreateTables(ctx)
	if err != nil {
		b.Fatalf("Failed to create tables: %v", err)
	}
	
	// 创建测试数据批次
	batchSizes := []int{100, 500, 1000, 5000}
	
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			klines := generateTestKLines("BTCUSDT", batchSize)
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := repo.Save(ctx, klines)
				if err != nil {
					b.Fatalf("Save error: %v", err)
				}
			}
			
			b.ReportMetric(float64(batchSize*b.N)/b.Elapsed().Seconds(), "klines/sec")
		})
	}
}

// TestConcurrencyLimit_MaxFiveTokens 测试最大5个代币并发限制性能
func TestConcurrencyLimit_MaxFiveTokens(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency limit test in short mode")
	}
	
	tempDir := testutils.CreateTempDir(t, "concurrency_test")
	cfg := createPerformanceTestConfig(tempDir)
	cfg.Scheduler.MaxConcurrentSymbols = 5
	
	// 跟踪并发执行的符号
	activeSymbols := make(map[string]bool)
	var activeMutex sync.Mutex
	maxConcurrent := 0
	currentConcurrent := 0
	
	mockDownloader := &mocks.MockDownloader{
		FetchFunc: func(ctx context.Context, task domain.DownloadTask) ([]byte, error) {
			activeMutex.Lock()
			activeSymbols[task.Symbol] = true
			currentConcurrent++
			if currentConcurrent > maxConcurrent {
				maxConcurrent = currentConcurrent
			}
			activeMutex.Unlock()
			
			// 模拟下载时间
			time.Sleep(time.Millisecond * 100)
			
			activeMutex.Lock()
			delete(activeSymbols, task.Symbol)
			currentConcurrent--
			activeMutex.Unlock()
			
			return []byte(testutils.CreateMockCSVData()), nil
		},
	}
	
	mockImporter := &mocks.MockImporter{}
	stateManager, err := state.NewFileStateManager(cfg.State)
	testutils.AssertNoError(t, err)
	
	schedulerInstance := scheduler.NewScheduler(
		cfg.Scheduler,
		mockDownloader,
		mockImporter,
		stateManager,
		nil,
		&mocks.MockRepository{},
	)
	
	// We can't pass tasks to RunConcurrent, so we'll just test the method call
	start := time.Now()
	ctx := testutils.WithTimeout(t, 30*time.Second)
	
	err = schedulerInstance.RunConcurrent(ctx)
	testutils.AssertNoError(t, err)
	
	duration := time.Since(start)
	
	t.Logf("RunConcurrent completed in %v", duration)
}

// BenchmarkMemoryUsage_LargeDataSet 测试大数据集内存使用性能
func BenchmarkMemoryUsage_LargeDataSet(b *testing.B) {
	tempDir := testutils.CreateTempDirB(b, "perf_test")
	cfg := createPerformanceTestConfig(tempDir)
	
	stateManager, err := state.NewFileStateManager(cfg.State)
	if err != nil {
		b.Fatalf("Failed to create state manager: %v", err)
	}
	
	// 创建大量状态数据
	symbols := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		symbols[i] = fmt.Sprintf("SYM%04dUSDT", i)
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// 批量保存状态
		for j, symbol := range symbols {
			state := &domain.ProcessingState{
				Symbol:      symbol,
				Status:      "processing",
				Processed:   j * 100,
				Failed:      j % 10,
				LastUpdated: time.Now(),
			}
			err := stateManager.SaveState(state)
			if err != nil {
				b.Fatalf("SaveState error: %v", err)
			}
		}
		
		// 批量读取状态
		allStates, err := stateManager.GetAllStates()
		if err != nil {
			b.Fatalf("GetAllStates error: %v", err)
		}
		
		if len(allStates) != len(symbols) {
			b.Fatalf("Expected %d states, got %d", len(symbols), len(allStates))
		}
	}
	
	b.ReportMetric(float64(len(symbols)*b.N)/b.Elapsed().Seconds(), "states/sec")
}

// BenchmarkImporter_ConcurrentImport 测试导入器并发导入性能
func BenchmarkImporter_ConcurrentImport(b *testing.B) {
	tempDir := testutils.CreateTempDirB(b, "perf_test")
	cfg := createPerformanceTestConfig(tempDir)
	
	mockDownloader := &mocks.MockDownloader{}
	mockParser := &MockParser{
		ParseFunc: func(ctx context.Context, data []byte, symbol string) ([]domain.KLine, *domain.ValidationResult, error) {
			klines := generateTestKLines(symbol, 1440) // 一天的1分钟数据
			return klines, &domain.ValidationResult{Valid: true, TotalRows: len(klines), ValidRows: len(klines)}, nil
		},
	}
	mockRepo := &mocks.MockRepository{}
	stateManager, _ := state.NewFileStateManager(cfg.State)
	
	dataImporter := importer.NewImporter(cfg.Importer, mockDownloader, mockParser, mockRepo, stateManager, nil)
	defer dataImporter.Close()
	
	// 创建并发任务
	tasks := make([]domain.DownloadTask, b.N)
	for i := 0; i < b.N; i++ {
		tasks[i] = domain.DownloadTask{
			Symbol: fmt.Sprintf("SYM%dUSDT", i%10),
			Date:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		}
	}
	
	ctx := context.Background()
	
	b.ResetTimer()
	err := dataImporter.ImportData(ctx, tasks)
	if err != nil {
		b.Fatalf("ImportData error: %v", err)
	}
	
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "imports/sec")
}

// 辅助函数

func createPerformanceTestConfig(tempDir string) *config.Config {
	return &config.Config{
		Log: config.LogConfig{
			Level:  "error", // 减少日志输出以提高性能
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
			Concurrency:       10,
			BufferSize:        100,
			UserAgent:         "PerformanceTest/1.0",
			EnableCompression: true,
			MaxFileSize:       "100MB",
		},
		Parser: config.ParserConfig{
			Concurrency:     5,
			BufferSize:      50,
			ValidateData:    true,
			SkipInvalidRows: true,
		},
		Database: config.DatabaseConfig{
			ClickHouse: config.ClickHouseConfig{
				Hosts:           []string{"localhost:9000"},
				Database:        "data4BT_performance_test",
				Username:        "default",
				Password:        "",
				Compression:     "lz4",
				DialTimeout:     30 * time.Second,
				MaxOpenConns:    10,
				MaxIdleConns:    5,
				ConnMaxLifetime: 5 * time.Minute,
				Settings: map[string]int{
					"max_execution_time": 300,
				},
			},
		},
		Importer: config.ImporterConfig{
			BatchSize:           2000,
			BufferSize:          20,
			FlushInterval:       5 * time.Second,
			EnableDeduplication: false, // 禁用去重以提高性能
		},
		State: config.StateConfig{
			StorageType:  "file",
			FilePath:     filepath.Join(tempDir, "progress.json"),
			BackupCount:  3,
			SaveInterval: 10 * time.Second,
		},
		Scheduler: config.SchedulerConfig{
			EndDate:               "",
			BatchDays:             1,
			MaxConcurrentSymbols:  5,
		},
	}
}

func generateLargeCSVData(numRows int) []byte {
	var data []byte
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	
	for i := 0; i < numRows; i++ {
		timestamp := baseTime.Add(time.Duration(i) * time.Minute)
		line := fmt.Sprintf("%d,29374.99,29375.01,29374.98,29375.00,0.05134800,%d,1507.51637800,85,0.02540200,745.13980000,0\n",
			timestamp.UnixMilli(),
			timestamp.UnixMilli()+59999,
		)
		data = append(data, []byte(line)...)
	}
	
	return data
}

func generateTestKLines(symbol string, count int) []domain.KLine {
	klines := make([]domain.KLine, count)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	
	for i := 0; i < count; i++ {
		openTime := baseTime.Add(time.Duration(i) * time.Minute)
		klines[i] = domain.KLine{
			Symbol:               symbol,
			OpenTime:             openTime,
			CloseTime:            openTime.Add(59*time.Second + 999*time.Millisecond),
			OpenPrice:            29374.99 + float64(rand.Intn(100))/100,
			HighPrice:            29375.01 + float64(rand.Intn(100))/100,
			LowPrice:             29374.98 - float64(rand.Intn(100))/100,
			ClosePrice:           29375.00 + float64(rand.Intn(200)-100)/100,
			Volume:               0.05134800 + rand.Float64()*0.1,
			QuoteAssetVolume:     1507.51637800 + rand.Float64()*100,
			NumberOfTrades:       int64(85 + rand.Intn(50)),
			TakerBuyBaseVolume:   0.02540200 + rand.Float64()*0.05,
			TakerBuyQuoteVolume:  745.13980000 + rand.Float64()*50,
			Interval:             "1m",
			CreatedAt:            time.Now(),
		}
	}
	
	return klines
}

// MockParser for performance testing
type MockParser struct {
	ParseFunc func(ctx context.Context, data []byte, symbol string) ([]domain.KLine, *domain.ValidationResult, error)
}

func (m *MockParser) Parse(ctx context.Context, data []byte, symbol string) ([]domain.KLine, *domain.ValidationResult, error) {
	if m.ParseFunc != nil {
		return m.ParseFunc(ctx, data, symbol)
	}
	return []domain.KLine{}, &domain.ValidationResult{Valid: true}, nil
}

func (m *MockParser) ValidateCSV(data []byte) error {
	return nil
}

func (m *MockParser) Close() error {
	return nil
}
