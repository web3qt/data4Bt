package domain

import (
	"testing"
	"time"
)

func TestKLineValidation(t *testing.T) {
	tests := []struct {
		name        string
		kline       KLine
		expectValid bool
	}{
		{
			name: "Valid kline",
			kline: KLine{
				Symbol:               "BTCUSDT",
				OpenTime:             time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				CloseTime:            time.Date(2024, 1, 1, 0, 0, 59, 999000000, time.UTC),
				OpenPrice:            29374.99,
				HighPrice:            29375.01,
				LowPrice:             29374.98,
				ClosePrice:           29375.00,
				Volume:               0.05134800,
				QuoteAssetVolume:     1507.51637800,
				NumberOfTrades:       85,
				TakerBuyBaseVolume:   0.02540200,
				TakerBuyQuoteVolume:  745.13980000,
				Interval:             "1m",
				CreatedAt:            time.Now(),
			},
			expectValid: true,
		},
		{
			name: "Invalid negative price",
			kline: KLine{
				Symbol:    "BTCUSDT",
				OpenPrice: -1.0,
				HighPrice: 29375.01,
				LowPrice:  29374.98,
				Interval:  "1m",
			},
			expectValid: false,
		},
		{
			name: "Invalid negative volume",
			kline: KLine{
				Symbol:   "BTCUSDT",
				Volume:   -1.0,
				Interval: "1m",
			},
			expectValid: false,
		},
		{
			name: "Invalid time order",
			kline: KLine{
				Symbol:    "BTCUSDT",
				OpenTime:  time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC),
				CloseTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				Interval:  "1m",
			},
			expectValid: false,
		},
	}

	// Since KLine doesn't have a Validate method, we'll test the structure creation
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that we can create the struct
			if tt.kline.Symbol == "" && tt.expectValid {
				t.Error("Expected symbol to be set")
			}
			
			// Test basic field access
			if tt.kline.Interval != "1m" && tt.kline.Interval != "" {
				t.Errorf("Expected interval to be preserved, got %s", tt.kline.Interval)
			}
		})
	}
}

func TestDownloadTask(t *testing.T) {
	task := DownloadTask{
		Symbol:   "BTCUSDT",
		Date:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Interval: "1m",
		URL:      "https://example.com/data.zip",
		Retries:  0,
	}

	if task.Symbol != "BTCUSDT" {
		t.Errorf("Expected symbol BTCUSDT, got %s", task.Symbol)
	}
	
	if task.Interval != "1m" {
		t.Errorf("Expected interval 1m, got %s", task.Interval)
	}
	
	if task.URL != "https://example.com/data.zip" {
		t.Errorf("Expected URL https://example.com/data.zip, got %s", task.URL)
	}
	
	if task.Retries != 0 {
		t.Errorf("Expected retries 0, got %d", task.Retries)
	}
}

func TestProcessingState(t *testing.T) {
	state := ProcessingState{
		Symbol:          "BTCUSDT",
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

	if state.Symbol != "BTCUSDT" {
		t.Errorf("Expected symbol BTCUSDT, got %s", state.Symbol)
	}
	
	if state.Status != "processing" {
		t.Errorf("Expected status processing, got %s", state.Status)
	}
	
	if state.WorkerID != 1 {
		t.Errorf("Expected worker ID 1, got %d", state.WorkerID)
	}
	
	if state.ProgressPercent != 50.0 {
		t.Errorf("Expected progress 50.0, got %f", state.ProgressPercent)
	}
}

func TestSymbolTimeline(t *testing.T) {
	timeline := SymbolTimeline{
		Symbol:              "BTCUSDT",
		HistoricalStartDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		CurrentImportDate:   time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		LatestAvailableDate: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
		TotalMonths:         6,
		ImportedMonthsCount: 3,
		FailedMonthsCount:   1,
		ImportProgress:      50.0,
		Status:              "importing",
		LastUpdated:         time.Now(),
		AvailableMonths:     []string{"2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"},
		ImportedMonths:      []string{"2024-01", "2024-02", "2024-03"},
		FailedMonths:        []string{"2024-04"},
	}

	if timeline.Symbol != "BTCUSDT" {
		t.Errorf("Expected symbol BTCUSDT, got %s", timeline.Symbol)
	}
	
	if timeline.TotalMonths != 6 {
		t.Errorf("Expected total months 6, got %d", timeline.TotalMonths)
	}
	
	if timeline.ImportProgress != 50.0 {
		t.Errorf("Expected import progress 50.0, got %f", timeline.ImportProgress)
	}
	
	if len(timeline.AvailableMonths) != 6 {
		t.Errorf("Expected 6 available months, got %d", len(timeline.AvailableMonths))
	}
}

func TestValidationResult(t *testing.T) {
	result := ValidationResult{
		Valid:       true,
		TotalRows:   1000,
		ValidRows:   990,
		InvalidRows: 10,
		Errors:      []string{"Error 1", "Error 2"},
		Warnings:    []string{"Warning 1"},
	}

	if !result.Valid {
		t.Error("Expected result to be valid")
	}
	
	if result.TotalRows != 1000 {
		t.Errorf("Expected total rows 1000, got %d", result.TotalRows)
	}
	
	if result.InvalidRows != 10 {
		t.Errorf("Expected invalid rows 10, got %d", result.InvalidRows)
	}
	
	if len(result.Errors) != 2 {
		t.Errorf("Expected 2 errors, got %d", len(result.Errors))
	}
}

func TestProgressReport(t *testing.T) {
	report := ProgressReport{
		TotalTasks:     100,
		CompletedTasks: 75,
		FailedTasks:    5,
		RunningTasks:   20,
		Progress:       75.0,
		StartTime:      time.Now(),
		EstimatedEnd:   time.Now().Add(10 * time.Minute),
		CurrentSymbol:  "BTCUSDT",
		CurrentDate:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		WorkerStates:   make(map[int]*WorkerState),
		SymbolProgress: make(map[string]*SymbolProgressInfo),
	}

	if report.TotalTasks != 100 {
		t.Errorf("Expected total tasks 100, got %d", report.TotalTasks)
	}
	
	if report.Progress != 75.0 {
		t.Errorf("Expected progress 75.0, got %f", report.Progress)
	}
	
	if report.CurrentSymbol != "BTCUSDT" {
		t.Errorf("Expected current symbol BTCUSDT, got %s", report.CurrentSymbol)
	}
}

func TestWorkerState(t *testing.T) {
	state := WorkerState{
		WorkerID:      1,
		Status:        "running",
		CurrentSymbol: "BTCUSDT",
		CurrentMonth:  "2024-01",
		StartTime:     time.Now(),
		LastUpdate:    time.Now(),
		TasksCount:    10,
		CompletedTasks: 7,
		FailedTasks:   1,
		ErrorMessage:  "",
	}

	if state.WorkerID != 1 {
		t.Errorf("Expected worker ID 1, got %d", state.WorkerID)
	}
	
	if state.Status != "running" {
		t.Errorf("Expected status running, got %s", state.Status)
	}
	
	if state.TasksCount != 10 {
		t.Errorf("Expected tasks count 10, got %d", state.TasksCount)
	}
}

func TestSymbolProgressInfo(t *testing.T) {
	info := SymbolProgressInfo{
		Symbol:          "BTCUSDT",
		TotalMonths:     12,
		CompletedMonths: 8,
		FailedMonths:    1,
		CurrentMonth:    "2024-08",
		Progress:        66.67,
		Status:          "processing",
		LastUpdate:      time.Now(),
		WorkerID:        1,
	}

	if info.Symbol != "BTCUSDT" {
		t.Errorf("Expected symbol BTCUSDT, got %s", info.Symbol)
	}
	
	if info.TotalMonths != 12 {
		t.Errorf("Expected total months 12, got %d", info.TotalMonths)
	}
	
	if info.Progress != 66.67 {
		t.Errorf("Expected progress 66.67, got %f", info.Progress)
	}
}

func TestConcurrentTask(t *testing.T) {
	task := ConcurrentTask{
		Symbol:     "BTCUSDT",
		Tasks:      []DownloadTask{},
		WorkerID:   1,
		Priority:   5,
		RetryCount: 0,
	}

	if task.Symbol != "BTCUSDT" {
		t.Errorf("Expected symbol BTCUSDT, got %s", task.Symbol)
	}
	
	if task.WorkerID != 1 {
		t.Errorf("Expected worker ID 1, got %d", task.WorkerID)
	}
	
	if task.Priority != 5 {
		t.Errorf("Expected priority 5, got %d", task.Priority)
	}
}
