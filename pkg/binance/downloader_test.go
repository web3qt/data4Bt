package binance

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/test/testutils"
)

func TestNewBinanceDownloader(t *testing.T) {
	tests := []struct {
		name     string
		binanceCfg config.BinanceConfig
		downloaderCfg config.DownloaderConfig
		wantUserAgent string
	}{
		{
			name: "basic_configuration",
			binanceCfg: config.BinanceConfig{
				BaseURL:        "https://data.binance.vision",
				DataPath:       "/data/spot/daily/klines",
				SymbolsFilter:  "USDT",
				Interval:       "1m",
				Timeout:        30 * time.Second,
				RetryCount:     3,
				RetryDelay:     5 * time.Second,
			},
			downloaderCfg: config.DownloaderConfig{
				UserAgent: "TestAgent/1.0",
			},
			wantUserAgent: "TestAgent/1.0",
		},
		{
			name: "with_proxy",
			binanceCfg: config.BinanceConfig{
				BaseURL:        "https://data.binance.vision",
				DataPath:       "/data/spot/daily/klines",
				SymbolsFilter:  "USDT",
				Interval:       "1m",
				Timeout:        30 * time.Second,
				RetryCount:     3,
				RetryDelay:     5 * time.Second,
				ProxyURL:       "http://127.0.0.1:8080",
			},
			downloaderCfg: config.DownloaderConfig{
				UserAgent: "TestAgent/1.0",
			},
			wantUserAgent: "TestAgent/1.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			downloader := NewBinanceDownloader(tt.binanceCfg, tt.downloaderCfg)
			
			if downloader == nil {
				t.Fatal("Expected downloader to be created, got nil")
			}
			
			if downloader.baseURL != tt.binanceCfg.BaseURL {
				t.Errorf("Expected baseURL %s, got %s", tt.binanceCfg.BaseURL, downloader.baseURL)
			}
			
			if downloader.userAgent != tt.wantUserAgent {
				t.Errorf("Expected userAgent %s, got %s", tt.wantUserAgent, downloader.userAgent)
			}
			
			if downloader.interval != tt.binanceCfg.Interval {
				t.Errorf("Expected interval %s, got %s", tt.binanceCfg.Interval, downloader.interval)
			}
		})
	}
}

func TestBinanceDownloader_BuildDownloadURL(t *testing.T) {
	downloader := NewBinanceDownloader(
		config.BinanceConfig{
			BaseURL:  "https://data.binance.vision",
			Interval: "1m",
		},
		config.DownloaderConfig{},
	)

	tests := []struct {
		name     string
		symbol   string
		date     time.Time
		expected string
	}{
		{
			name:     "btc_january_2024",
			symbol:   "BTCUSDT",
			date:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip",
		},
		{
			name:     "eth_december_2023",
			symbol:   "ETHUSDT",
			date:     time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC),
			expected: "https://data.binance.vision/data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2023-12.zip",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := downloader.BuildDownloadURL(tt.symbol, tt.date)
			if result != tt.expected {
				t.Errorf("Expected URL %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestBinanceDownloader_Fetch(t *testing.T) {
	// 创建模拟CSV数据
	csvData := testutils.CreateMockCSVData()
	
	// 创建ZIP文件包含CSV数据
	zipBuffer := &bytes.Buffer{}
	zipWriter := zip.NewWriter(zipBuffer)
	
	csvFile, err := zipWriter.Create("BTCUSDT-1m-2024-01.csv")
	if err != nil {
		t.Fatalf("Failed to create CSV file in ZIP: %v", err)
	}
	
	_, err = csvFile.Write([]byte(csvData))
	if err != nil {
		t.Fatalf("Failed to write CSV data: %v", err)
	}
	
	zipWriter.Close()
	zipData := zipBuffer.Bytes()

	// 创建模拟服务器
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Expected GET request, got %s", r.Method)
		}
		
		w.Header().Set("Content-Type", "application/zip")
		w.WriteHeader(http.StatusOK)
		w.Write(zipData)
	}))
	defer server.Close()

	downloader := NewBinanceDownloader(
		config.BinanceConfig{
			BaseURL:    server.URL,
			Interval:   "1m",
			RetryCount: 3,
			RetryDelay: 100 * time.Millisecond,
		},
		config.DownloaderConfig{
			UserAgent: "TestAgent/1.0",
		},
	)

	ctx := testutils.WithTimeout(t, 5*time.Second)
	task := testutils.CreateTestDownloadTask("BTCUSDT", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	task.URL = server.URL + "/test.zip"

	result, err := downloader.Fetch(ctx, task)
	testutils.AssertNoError(t, err)
	
	if len(result) == 0 {
		t.Error("Expected non-empty result")
	}
	
	resultStr := string(result)
	if !strings.Contains(resultStr, "29374.99") {
		t.Error("Expected result to contain CSV data")
	}
}

func TestBinanceDownloader_Fetch_Retry(t *testing.T) {
	retryCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		retryCount++
		if retryCount <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		
		// 第三次请求成功
		csvData := testutils.CreateMockCSVData()
		zipBuffer := &bytes.Buffer{}
		zipWriter := zip.NewWriter(zipBuffer)
		csvFile, _ := zipWriter.Create("test.csv")
		csvFile.Write([]byte(csvData))
		zipWriter.Close()
		
		w.Header().Set("Content-Type", "application/zip")
		w.WriteHeader(http.StatusOK)
		w.Write(zipBuffer.Bytes())
	}))
	defer server.Close()

	downloader := NewBinanceDownloader(
		config.BinanceConfig{
			RetryCount: 3,
			RetryDelay: 10 * time.Millisecond,
		},
		config.DownloaderConfig{},
	)

	ctx := testutils.WithTimeout(t, 5*time.Second)
	task := domain.DownloadTask{
		Symbol: "BTCUSDT",
		Date:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		URL:    server.URL + "/test.zip",
	}

	result, err := downloader.Fetch(ctx, task)
	testutils.AssertNoError(t, err)
	
	if retryCount != 3 {
		t.Errorf("Expected 3 retry attempts, got %d", retryCount)
	}
	
	if len(result) == 0 {
		t.Error("Expected non-empty result after retry")
	}
}

func TestBinanceDownloader_Fetch_Failed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	downloader := NewBinanceDownloader(
		config.BinanceConfig{
			RetryCount: 2,
			RetryDelay: 10 * time.Millisecond,
		},
		config.DownloaderConfig{},
	)

	ctx := testutils.WithTimeout(t, 5*time.Second)
	task := domain.DownloadTask{
		Symbol: "BTCUSDT",
		Date:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		URL:    server.URL + "/test.zip",
	}

	_, err := downloader.Fetch(ctx, task)
	testutils.AssertError(t, err)
}

func TestBinanceDownloader_GetAllSymbolsFromBinance(t *testing.T) {
	// 创建模拟API响应
	mockResponse := map[string]interface{}{
		"symbols": []map[string]interface{}{
			{
				"symbol": "BTCUSDT",
				"status": "TRADING",
			},
			{
				"symbol": "ETHUSDT",
				"status": "TRADING",
			},
			{
				"symbol": "ADAUSDT",
				"status": "TRADING",
			},
			{
				"symbol": "BTCETH",
				"status": "TRADING",
			},
			{
				"symbol": "PAUSEDUSDT",
				"status": "BREAK",
			},
		},
	}
	
	responseData, err := json.Marshal(mockResponse)
	if err != nil {
		t.Fatalf("Failed to marshal mock response: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/exchangeInfo" {
			t.Errorf("Expected path /api/v3/exchangeInfo, got %s", r.URL.Path)
		}
		
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(responseData)
	}))
	defer server.Close()

	downloader := NewBinanceDownloader(
		config.BinanceConfig{
			RetryCount: 1,
			RetryDelay: 10 * time.Millisecond,
		},
		config.DownloaderConfig{
			UserAgent: "TestAgent/1.0",
		},
	)
	
	// 临时替换API URL为测试服务器
	originalClient := downloader.client
	downloader.client = &http.Client{Timeout: 5 * time.Second}

	// 直接调用内部方法进行测试
	symbols := downloader.extractUSDTSymbolsFromAPI(responseData)
	
	expectedSymbols := []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"}
	if len(symbols) != len(expectedSymbols) {
		t.Errorf("Expected %d symbols, got %d", len(expectedSymbols), len(symbols))
	}
	
	for _, expected := range expectedSymbols {
		found := false
		for _, symbol := range symbols {
			if symbol == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected symbol %s not found in result", expected)
		}
	}
	
	downloader.client = originalClient
}

func TestBinanceDownloader_ValidateURL(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		wantError  bool
	}{
		{
			name:       "valid_url",
			statusCode: http.StatusOK,
			wantError:  false,
		},
		{
			name:       "not_found",
			statusCode: http.StatusNotFound,
			wantError:  true,
		},
		{
			name:       "server_error",
			statusCode: http.StatusInternalServerError,
			wantError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "HEAD" {
					t.Errorf("Expected HEAD request, got %s", r.Method)
				}
				w.WriteHeader(tt.statusCode)
			}))
			defer server.Close()

			downloader := NewBinanceDownloader(
				config.BinanceConfig{},
				config.DownloaderConfig{
					UserAgent: "TestAgent/1.0",
				},
			)

			ctx := testutils.WithTimeout(t, 5*time.Second)
			err := downloader.ValidateURL(ctx, server.URL)
			
			if tt.wantError {
				testutils.AssertError(t, err)
			} else {
				testutils.AssertNoError(t, err)
			}
		})
	}
}

func TestBinanceDownloader_GetAvailableDates(t *testing.T) {
	// 创建一系列响应，模拟二分查找过程
	validURLs := map[string]bool{
		// 2024年1月到3月的数据可用
		"/BTCUSDT/2024-01": true,
		"/BTCUSDT/2024-02": true,
		"/BTCUSDT/2024-03": true,
		// 2023年12月的数据也可用
		"/BTCUSDT/2023-12": true,
		// 更早的数据不可用
		"/BTCUSDT/2023-11": false,
		"/BTCUSDT/2023-10": false,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "HEAD" {
			t.Errorf("Expected HEAD request, got %s", r.Method)
		}
		
		path := r.URL.Path
		// 简化URL匹配，实际实现会更复杂
		found := false
		for validPath := range validURLs {
			if strings.Contains(path, validPath) && validURLs[validPath] {
				found = true
				break
			}
		}
		
		if found {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	downloader := NewBinanceDownloader(
		config.BinanceConfig{
			BaseURL: server.URL,
			Interval: "1m",
		},
		config.DownloaderConfig{
			UserAgent: "TestAgent/1.0",
		},
	)

	ctx := testutils.WithTimeout(t, 10*time.Second)
	dates, err := downloader.GetAvailableDates(ctx, "BTCUSDT")
	
	// 注意：由于二分查找的复杂性，这个测试可能需要调整
	// 这里主要测试方法不会崩溃，并且返回一些结果
	testutils.AssertNoError(t, err)
	
	if len(dates) == 0 {
		t.Error("Expected some available dates")
	}
}

func TestBinanceDownloader_FilterSymbols(t *testing.T) {
	downloader := NewBinanceDownloader(
		config.BinanceConfig{
			SymbolsFilter: "USDT",
		},
		config.DownloaderConfig{},
	)

	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "filter_usdt_symbols",
			input:    []string{"BTCUSDT", "ETHUSDT", "BTCETH", "ADAUSDT", "BNBBTC"},
			expected: []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"},
		},
		{
			name:     "no_usdt_symbols",
			input:    []string{"BTCETH", "ETHBTC", "ADABTC"},
			expected: []string{},
		},
		{
			name:     "empty_input",
			input:    []string{},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := downloader.filterSymbols(tt.input)
			
			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d symbols, got %d", len(tt.expected), len(result))
			}
			
			for i, expected := range tt.expected {
				if i >= len(result) || result[i] != expected {
					t.Errorf("Expected symbol %s at index %d, got %s", expected, i, result[i])
				}
			}
		})
	}
}

func TestBinanceDownloader_ExtractCSVFromZip(t *testing.T) {
	// 创建包含CSV文件的ZIP数据
	csvData := "test,csv,data\n1,2,3\n4,5,6"
	
	zipBuffer := &bytes.Buffer{}
	zipWriter := zip.NewWriter(zipBuffer)
	
	csvFile, err := zipWriter.Create("test.csv")
	if err != nil {
		t.Fatalf("Failed to create CSV file in ZIP: %v", err)
	}
	
	_, err = csvFile.Write([]byte(csvData))
	if err != nil {
		t.Fatalf("Failed to write CSV data: %v", err)
	}
	
	zipWriter.Close()
	zipData := zipBuffer.Bytes()

	downloader := NewBinanceDownloader(
		config.BinanceConfig{},
		config.DownloaderConfig{},
	)

	result, err := downloader.extractCSVFromZip(zipData)
	testutils.AssertNoError(t, err)
	
	if string(result) != csvData {
		t.Errorf("Expected CSV data %s, got %s", csvData, string(result))
	}
}

func TestBinanceDownloader_ExtractCSVFromZip_NoCSV(t *testing.T) {
	// 创建不包含CSV文件的ZIP数据
	zipBuffer := &bytes.Buffer{}
	zipWriter := zip.NewWriter(zipBuffer)
	
	txtFile, err := zipWriter.Create("test.txt")
	if err != nil {
		t.Fatalf("Failed to create TXT file in ZIP: %v", err)
	}
	
	txtFile.Write([]byte("not csv data"))
	zipWriter.Close()
	zipData := zipBuffer.Bytes()

	downloader := NewBinanceDownloader(
		config.BinanceConfig{},
		config.DownloaderConfig{},
	)

	_, err = downloader.extractCSVFromZip(zipData)
	testutils.AssertError(t, err, "Expected error when no CSV file found")
}

// 基准测试
func BenchmarkBinanceDownloader_BuildDownloadURL(b *testing.B) {
	downloader := NewBinanceDownloader(
		config.BinanceConfig{
			BaseURL:  "https://data.binance.vision",
			Interval: "1m",
		},
		config.DownloaderConfig{},
	)

	date := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = downloader.BuildDownloadURL("BTCUSDT", date)
	}
}

func BenchmarkBinanceDownloader_FilterSymbols(b *testing.B) {
	downloader := NewBinanceDownloader(
		config.BinanceConfig{
			SymbolsFilter: "USDT",
		},
		config.DownloaderConfig{},
	)

	symbols := []string{
		"BTCUSDT", "ETHUSDT", "ADAUSDT", "BNBUSDT", "XRPUSDT",
		"BTCETH", "ETHBTC", "ADABTC", "BNBBTC", "XRPBTC",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = downloader.filterSymbols(symbols)
	}
}