package monitor

import (
	"context"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
)

func TestProgressReporterInitialization(t *testing.T) {
	cfg := config.MonitoringConfig{
		Enabled:                true,
		MetricsPort:            8080,
		HealthCheckPort:        8081,
		ProgressReportInterval: 10 * time.Second,
	}
	
	reporter := NewProgressReporter(cfg)
	if reporter == nil {
		t.Error("Expected ProgressReporter to be created")
	}
}

func TestProgressReporterStartStop(t *testing.T) {
	cfg := config.MonitoringConfig{
		Enabled:                false, // Disable to avoid port conflicts
		MetricsPort:            8080,
		HealthCheckPort:        8081,
		ProgressReportInterval: 10 * time.Second,
	}
	
	reporter := NewProgressReporter(cfg)
	
	// Test starting the reporter
	err := reporter.Start(100)
	if err != nil {
		t.Errorf("Expected Start to succeed, got error: %v", err)
	}
	
	// Test stopping the reporter
	ctx := context.Background()
	err = reporter.Stop(ctx)
	if err != nil {
		t.Errorf("Expected Stop to succeed, got error: %v", err)
	}
}

func TestProgressReporterReportProgress(t *testing.T) {
	cfg := config.MonitoringConfig{
		Enabled:                false, // Disable to avoid port conflicts
		MetricsPort:            8080,
		HealthCheckPort:        8081,
		ProgressReportInterval: 10 * time.Second,
	}
	
	reporter := NewProgressReporter(cfg)
	
	// Create a mock progress report
	progress := &domain.ProgressReport{
		TotalTasks:     100,
		CompletedTasks: 50,
		FailedTasks:    5,
		RunningTasks:   45,
		Progress:       50.0,
		StartTime:      time.Now(),
		EstimatedEnd:   time.Now().Add(10 * time.Minute),
		CurrentSymbol:  "BTCUSDT",
		CurrentDate:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		WorkerStates:   make(map[int]*domain.WorkerState),
		SymbolProgress: make(map[string]*domain.SymbolProgressInfo),
	}
	
	// Test reporting progress
	reporter.ReportProgress(progress)
	
	// If we get here without panicking, the tests pass
}

func TestProgressReporterWithWorkers(t *testing.T) {
	cfg := config.MonitoringConfig{
		Enabled:                false, // Disable to avoid port conflicts
		MetricsPort:            8080,
		HealthCheckPort:        8081,
		ProgressReportInterval: 10 * time.Second,
	}
	
	reporter := NewProgressReporter(cfg)
	
	// Create worker states
	workerStates := make(map[int]*domain.WorkerState)
	workerStates[1] = &domain.WorkerState{
		WorkerID:       1,
		Status:         "running",
		CurrentSymbol:  "BTCUSDT",
		CurrentMonth:   "2024-01",
		StartTime:      time.Now(),
		LastUpdate:     time.Now(),
		TasksCount:     10,
		CompletedTasks: 7,
		FailedTasks:    1,
		ErrorMessage:   "",
	}
	
	// Create symbol progress
	symbolProgress := make(map[string]*domain.SymbolProgressInfo)
	symbolProgress["BTCUSDT"] = &domain.SymbolProgressInfo{
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
	
	// Create a progress report with worker states
	progress := &domain.ProgressReport{
		TotalTasks:     100,
		CompletedTasks: 50,
		FailedTasks:    5,
		RunningTasks:   45,
		Progress:       50.0,
		StartTime:      time.Now(),
		EstimatedEnd:   time.Now().Add(10 * time.Minute),
		CurrentSymbol:  "BTCUSDT",
		CurrentDate:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		WorkerStates:   workerStates,
		SymbolProgress: symbolProgress,
	}
	
	// Test reporting progress with worker states
	reporter.ReportProgress(progress)
	
	// If we get here without panicking, the tests pass
}

func TestGetOverallProgress(t *testing.T) {
	cfg := config.MonitoringConfig{
		Enabled:                false,
		MetricsPort:            8080,
		HealthCheckPort:        8081,
		ProgressReportInterval: 10 * time.Second,
	}
	
	reporter := NewProgressReporter(cfg)
	
	// Start with some tasks
	err := reporter.Start(100)
	if err != nil {
		t.Fatalf("Failed to start reporter: %v", err)
	}
	
	// Create a progress report
	progress := &domain.ProgressReport{
		TotalTasks:     100,
		CompletedTasks: 75,
		FailedTasks:    5,
		RunningTasks:   20,
		Progress:       75.0,
		StartTime:      time.Now(),
		EstimatedEnd:   time.Now().Add(5 * time.Minute),
		CurrentSymbol:  "BTCUSDT",
		CurrentDate:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		WorkerStates:   make(map[int]*domain.WorkerState),
		SymbolProgress: make(map[string]*domain.SymbolProgressInfo),
	}
	
	// Report progress
	reporter.ReportProgress(progress)
	
	// Get overall progress
	overall := reporter.GetOverallProgress()
	
	// Verify some key fields exist
	if overall["total_tasks"] != 100 {
		t.Errorf("Expected total_tasks 100, got %v", overall["total_tasks"])
	}
	
	if overall["completed_tasks"] != 75 {
		t.Errorf("Expected completed_tasks 75, got %v", overall["completed_tasks"])
	}
}

func TestGetDetailedProgress(t *testing.T) {
	cfg := config.MonitoringConfig{
		Enabled:                false,
		MetricsPort:            8080,
		HealthCheckPort:        8081,
		ProgressReportInterval: 10 * time.Second,
	}
	
	reporter := NewProgressReporter(cfg)
	
	// Create a progress report
	progress := &domain.ProgressReport{
		TotalTasks:     100,
		CompletedTasks: 50,
		FailedTasks:    5,
		RunningTasks:   45,
		Progress:       50.0,
		StartTime:      time.Now(),
		EstimatedEnd:   time.Now().Add(10 * time.Minute),
		CurrentSymbol:  "BTCUSDT",
		CurrentDate:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		WorkerStates:   make(map[int]*domain.WorkerState),
		SymbolProgress: make(map[string]*domain.SymbolProgressInfo),
	}
	
	// Report progress
	reporter.ReportProgress(progress)
	
	// Get detailed progress
	detailed := reporter.GetDetailedProgress()
	
	// Should have one entry
	if len(detailed) != 1 {
		t.Errorf("Expected 1 detailed progress entry, got %d", len(detailed))
	}
}
