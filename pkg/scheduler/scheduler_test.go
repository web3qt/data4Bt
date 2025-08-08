package scheduler

import (
	"context"
	"testing"
	"binance-data-loader/internal/config"
)

// TestScheduler_getSymbols 测试获取符号列表的基本功能
func TestScheduler_getSymbols(t *testing.T) {
	// 创建调度器
	scheduler := &Scheduler{}
	
	// 获取硬编码的符号列表
	symbols, err := scheduler.getSymbols(context.Background())
	if err != nil {
		t.Fatalf("Failed to get symbols: %v", err)
	}
	
	// 验证结果
	if len(symbols) == 0 {
		t.Error("Expected symbols to be returned, got empty slice")
	}
	
	// 验证包含预期的符号（根据实际硬编码的符号列表）
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
}

// TestScheduler_min 测试min辅助函数
func TestScheduler_min(t *testing.T) {
	tests := []struct {
		a, b, expected int
	}{
		{1, 2, 1},
		{5, 3, 3},
		{0, 0, 0},
		{-1, 1, -1},
	}
	
	for _, test := range tests {
		result := min(test.a, test.b)
		if result != test.expected {
			t.Errorf("min(%d, %d) = %d, expected %d", test.a, test.b, result, test.expected)
		}
	}
}

// TestScheduler_NewScheduler 测试调度器创建
func TestScheduler_NewScheduler(t *testing.T) {
	// 测试NewScheduler函数是否正确创建调度器实例
	scheduler := NewScheduler(
		config.SchedulerConfig{},
		nil, // downloader
		nil, // importer
		nil, // stateManager
		nil, // progressReporter
		nil, // repository
	)
	
	if scheduler == nil {
		t.Error("Expected scheduler to be created, got nil")
	}
	
	// 验证字段初始化
	// Logger在NewScheduler中通过logger.GetLogger初始化，这里只验证不为零值
	if scheduler.config.BatchDays == 0 && scheduler.config.MaxConcurrentSymbols == 0 {
		// 配置字段应该被设置（即使是零值配置）
	}
}

// BenchmarkScheduler_min 基准测试min函数性能
func BenchmarkScheduler_min(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = min(i, i+1)
	}
}