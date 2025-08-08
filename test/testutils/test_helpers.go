package testutils

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
)

// CreateTempDir 创建临时目录用于测试
func CreateTempDir(t *testing.T, prefix string) string {
	dir, err := ioutil.TempDir("", prefix)
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	
	return dir
}

// CreateTempDirB 创建临时目录用于基准测试
func CreateTempDirB(b *testing.B, prefix string) string {
	dir, err := ioutil.TempDir("", prefix)
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	
	b.Cleanup(func() {
		os.RemoveAll(dir)
	})
	
	return dir
}

// CreateTestConfig 创建测试配置
func CreateTestConfig(tempDir string) *config.Config {
	return &config.Config{
		Log: config.LogConfig{
			Level:    "debug",
			Format:   "json",
			Output:   "stdout",
			FilePath: "",
		},
		Binance: config.BinanceConfig{
			BaseURL:        "https://data.binance.vision",
			DataPath:       "/data/spot/daily/klines",
			SymbolsFilter:  "USDT",
			Interval:       "1m",
			Timeout:        30 * time.Second,
			RetryCount:     3,
			RetryDelay:     5 * time.Second,
			ProxyURL:       "",
		},
		Downloader: config.DownloaderConfig{
			Concurrency:       10,
			BufferSize:        100,
			UserAgent:         "BinanceDataLoader/1.0",
			EnableCompression: true,
			MaxFileSize:       "100MB",
		},
		Parser: config.ParserConfig{
			Concurrency:      5,
			BufferSize:       50,
			ValidateData:     true,
			SkipInvalidRows:  true,
		},
		Database: config.DatabaseConfig{
			ClickHouse: config.ClickHouseConfig{
				Hosts:           []string{"localhost:9000"},
				Database:        "data4BT_test",
				Username:        "default",
				Password:        "",
				Compression:     "lz4",
				DialTimeout:     30 * time.Second,
				MaxOpenConns:    10,
				MaxIdleConns:    5,
				ConnMaxLifetime: 10 * time.Minute,
			},
		},
		Importer: config.ImporterConfig{
			BatchSize:           10000,
			BufferSize:          20,
			FlushInterval:       30 * time.Second,
			EnableDeduplication: true,
		},
		State: config.StateConfig{
			StorageType:  "file",
			FilePath:     filepath.Join(tempDir, "progress.json"),
			BackupCount:  5,
			SaveInterval: 10 * time.Second,
		},
		MaterializedViews: config.MaterializedViewsConfig{
			Enabled:         true,
			Intervals:       []string{"5m", "15m", "1h", "4h", "1d"},
			RefreshInterval: 1 * time.Minute,
			ParallelRefresh: true,
		},
		Monitoring: config.MonitoringConfig{
			Enabled:                  true,
			MetricsPort:              8080,
			HealthCheckPort:          8081,
			ProgressReportInterval:   30 * time.Second,
		},
		Scheduler: config.SchedulerConfig{
			EndDate:               "",
			BatchDays:             7,
			MaxConcurrentSymbols:  5,
		},
	}
}

// CreateTestKLine 创建测试K线数据
func CreateTestKLine(symbol string, openTime time.Time) domain.KLine {
	return domain.KLine{
		Symbol:               symbol,
		OpenTime:             openTime,
		CloseTime:            openTime.Add(59*time.Second + 999*time.Millisecond),
		OpenPrice:            50000.00,
		HighPrice:            50100.00,
		LowPrice:             49900.00,
		ClosePrice:           50050.00,
		Volume:               1.23456,
		QuoteAssetVolume:     61782.345,
		NumberOfTrades:       100,
		TakerBuyBaseVolume:   0.65432,
		TakerBuyQuoteVolume:  32765.123,
		Interval:             "1m",
		CreatedAt:            time.Now(),
	}
}

// CreateTestDownloadTask 创建测试下载任务
func CreateTestDownloadTask(symbol string, date time.Time) domain.DownloadTask {
	return domain.DownloadTask{
		Symbol: symbol,
		Date:   date,
		URL:    "",
	}
}

// CreateTestProcessingState 创建测试处理状态
func CreateTestProcessingState(symbol string) *domain.ProcessingState {
	return &domain.ProcessingState{
		Symbol:          symbol,
		LastDate:        time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		TotalFiles:      100,
		Processed:       50,
		Failed:          2,
		LastUpdated:     time.Now(),
		Status:          "processing",
		WorkerID:        1,
		StartDate:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndDate:         time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC),
		CurrentMonth:    "2024-01",
		ErrorMessage:    "",
		ProgressPercent: 50.0,
	}
}

// CreateTestSymbolTimeline 创建测试代币时间线
func CreateTestSymbolTimeline(symbol string) *domain.SymbolTimeline {
	return &domain.SymbolTimeline{
		Symbol:               symbol,
		AvailableMonths:      []string{"2024-01", "2024-02", "2024-03"},
		ImportedMonths:       []string{"2024-01", "2024-02"},
		TotalMonths:          3,
		ImportedMonthsCount:  2,
		HistoricalStartDate:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		LatestAvailableDate:  time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		Status:               "processing",
		LastUpdated:          time.Now(),
	}
}

// CreateTestWorkerState 创建测试Worker状态
func CreateTestWorkerState(workerID int) *domain.WorkerState {
	return &domain.WorkerState{
		WorkerID:       workerID,
		Status:         "working",
		CurrentSymbol:  "BTCUSDT",
		CurrentMonth:   "2024-01",
		StartTime:      time.Now().Add(-1 * time.Hour),
		LastUpdate:     time.Now(),
		TasksCount:     10,
		CompletedTasks: 7,
		FailedTasks:    1,
		ErrorMessage:   "",
	}
}

// CreateTestSymbolProgress 创建测试币种进度
func CreateTestSymbolProgress(symbol string) *domain.SymbolProgressInfo {
	return &domain.SymbolProgressInfo{
		Symbol:          symbol,
		TotalMonths:     12,
		CompletedMonths: 8,
		FailedMonths:    1,
		CurrentMonth:    "2024-08",
		Progress:        66.67,
		Status:          "processing",
		LastUpdate:      time.Now(),
		WorkerID:        1,
	}
}

// AssertTimeEqual 断言时间相等（忽略微秒）
func AssertTimeEqual(t *testing.T, expected, actual time.Time, msgAndArgs ...interface{}) {
	expectedTrunc := expected.Truncate(time.Second)
	actualTrunc := actual.Truncate(time.Second)
	
	if !expectedTrunc.Equal(actualTrunc) {
		t.Errorf("Expected time %v, got %v", expected, actual)
		if len(msgAndArgs) > 0 {
			t.Errorf("Message: %v", msgAndArgs...)
		}
	}
}

// AssertNotZeroTime 断言时间不为零值
func AssertNotZeroTime(t *testing.T, actual time.Time, msgAndArgs ...interface{}) {
	if actual.IsZero() {
		t.Error("Expected non-zero time")
		if len(msgAndArgs) > 0 {
			t.Errorf("Message: %v", msgAndArgs...)
		}
	}
}

// CreateMockCSVData 创建模拟CSV数据
func CreateMockCSVData() string {
	return `1609459200000,29374.99,29375.00,29374.98,29375.00,0.05134800,1609459259999,1507.51637800,85,0.02540200,745.13980000,0
1609459260000,29375.00,29375.01,29374.99,29375.01,0.03421100,1609459319999,1005.12345000,67,0.01823400,535.67890000,0
1609459320000,29375.01,29375.02,29375.00,29375.02,0.02156700,1609459379999,633.45678000,45,0.01234500,362.89012000,0`
}

// CreateMockZipData 创建模拟ZIP数据（用于测试下载）
func CreateMockZipData() []byte {
	// 这是一个简化的ZIP文件头，实际测试中可能需要真实的ZIP数据
	return []byte{
		0x50, 0x4b, 0x03, 0x04, // ZIP file signature
		0x14, 0x00, 0x00, 0x00, // Version, flags
		0x00, 0x00, 0x00, 0x00, // Compression method, modification time
		0x00, 0x00, 0x00, 0x00, // CRC-32, compressed size, uncompressed size
		0x00, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00, // File name length, extra field length
		't', 'e', 's', 't', '.', 'c', 's', 'v', // File name "test.csv"
	}
}

// SkipIfNoDatabase 如果没有数据库连接则跳过测试
func SkipIfNoDatabase(t *testing.T) {
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping test that requires database (set INTEGRATION_TEST=true to run)")
	}
}

// SkipIfNoDatabaseB 如果没有数据库连接则跳过基准测试
func SkipIfNoDatabaseB(b *testing.B) {
	if os.Getenv("INTEGRATION_TEST") != "true" {
		b.Skip("Skipping benchmark that requires database (set INTEGRATION_TEST=true to run)")
	}
}

// SkipIfNoNetwork 如果没有网络连接则跳过测试
func SkipIfNoNetwork(t *testing.T) {
	if os.Getenv("NETWORK_TEST") != "true" {
		t.Skip("Skipping test that requires network (set NETWORK_TEST=true to run)")
	}
}

// WithTimeout 为测试创建带超时的上下文
func WithTimeout(t *testing.T, timeout time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

// AssertNoError 断言没有错误
func AssertNoError(t *testing.T, err error, msgAndArgs ...interface{}) {
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
		if len(msgAndArgs) > 0 {
			t.Fatalf("Message: %v", msgAndArgs...)
		}
	}
}

// AssertError 断言有错误
func AssertError(t *testing.T, err error, msgAndArgs ...interface{}) {
	if err == nil {
		t.Error("Expected error, got nil")
		if len(msgAndArgs) > 0 {
			t.Errorf("Message: %v", msgAndArgs...)
		}
	}
}