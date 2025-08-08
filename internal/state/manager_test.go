package state

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/test/testutils"
)

func TestNewFileStateManager(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	
	cfg := config.StateConfig{
		StorageType:  "file",
		FilePath:     filepath.Join(tempDir, "progress.json"),
		BackupCount:  3,
		SaveInterval: 10 * time.Second,
	}
	
	manager, err := NewFileStateManager(cfg)
	testutils.AssertNoError(t, err)
	
	if manager == nil {
		t.Fatal("Expected state manager to be created, got nil")
	}
	
	if manager.filePath != cfg.FilePath {
		t.Errorf("Expected filePath %s, got %s", cfg.FilePath, manager.filePath)
	}
	
	if manager.backupCount != 3 {
		t.Errorf("Expected backupCount 3, got %d", manager.backupCount)
	}
	
	// 验证目录被创建
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		t.Error("Expected state directory to be created")
	}
}

func TestFileStateManager_GetState_NewSymbol(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	state, err := manager.GetState("BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if state == nil {
		t.Fatal("Expected state to be returned, got nil")
	}
	
	if state.Symbol != "BTCUSDT" {
		t.Errorf("Expected symbol BTCUSDT, got %s", state.Symbol)
	}
	
	if state.Status != "pending" {
		t.Errorf("Expected status pending, got %s", state.Status)
	}
	
	if !state.LastDate.IsZero() {
		t.Error("Expected LastDate to be zero for new symbol")
	}
	
	if state.WorkerID != -1 {
		t.Errorf("Expected WorkerID -1, got %d", state.WorkerID)
	}
}

func TestFileStateManager_SaveState(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	testState := testutils.CreateTestProcessingState("BTCUSDT")
	testState.Processed = 100
	testState.Failed = 5
	
	err := manager.SaveState(testState)
	testutils.AssertNoError(t, err)
	
	// 验证状态被保存
	retrievedState, err := manager.GetState("BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if retrievedState.Processed != 100 {
		t.Errorf("Expected Processed 100, got %d", retrievedState.Processed)
	}
	
	if retrievedState.Failed != 5 {
		t.Errorf("Expected Failed 5, got %d", retrievedState.Failed)
	}
	
	if retrievedState.Status != testState.Status {
		t.Errorf("Expected Status %s, got %s", testState.Status, retrievedState.Status)
	}
	
	// 验证文件被创建
	if _, err := os.Stat(manager.filePath); os.IsNotExist(err) {
		t.Error("Expected state file to be created")
	}
}

func TestFileStateManager_GetAllStates(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	// 保存多个状态
	symbols := []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"}
	for _, symbol := range symbols {
		state := testutils.CreateTestProcessingState(symbol)
		err := manager.SaveState(state)
		testutils.AssertNoError(t, err)
	}
	
	allStates, err := manager.GetAllStates()
	testutils.AssertNoError(t, err)
	
	if len(allStates) != len(symbols) {
		t.Errorf("Expected %d states, got %d", len(symbols), len(allStates))
	}
	
	for _, symbol := range symbols {
		if state, exists := allStates[symbol]; !exists {
			t.Errorf("Expected state for symbol %s to exist", symbol)
		} else if state.Symbol != symbol {
			t.Errorf("Expected symbol %s, got %s", symbol, state.Symbol)
		}
	}
}

func TestFileStateManager_DeleteState(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	// 保存状态
	state := testutils.CreateTestProcessingState("BTCUSDT")
	err := manager.SaveState(state)
	testutils.AssertNoError(t, err)
	
	// 删除状态
	err = manager.DeleteState("BTCUSDT")
	testutils.AssertNoError(t, err)
	
	// 验证状态被删除
	allStates, err := manager.GetAllStates()
	testutils.AssertNoError(t, err)
	
	if _, exists := allStates["BTCUSDT"]; exists {
		t.Error("Expected state for BTCUSDT to be deleted")
	}
}

func TestFileStateManager_Timeline(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	timeline := testutils.CreateTestSymbolTimeline("BTCUSDT")
	timeline.TotalMonths = 24
	timeline.ImportedMonthsCount = 12
	timeline.Status = "processing"
	
	// 保存时间线
	err := manager.SaveTimeline(timeline)
	testutils.AssertNoError(t, err)
	
	// 获取时间线
	retrievedTimeline, err := manager.GetTimeline("BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if retrievedTimeline.Symbol != "BTCUSDT" {
		t.Errorf("Expected symbol BTCUSDT, got %s", retrievedTimeline.Symbol)
	}
	
	if retrievedTimeline.TotalMonths != 24 {
		t.Errorf("Expected TotalMonths 24, got %d", retrievedTimeline.TotalMonths)
	}
	
	if retrievedTimeline.ImportedMonthsCount != 12 {
		t.Errorf("Expected ImportedMonthsCount 12, got %d", retrievedTimeline.ImportedMonthsCount)
	}
	
	if retrievedTimeline.Status != "processing" {
		t.Errorf("Expected Status processing, got %s", retrievedTimeline.Status)
	}
}

func TestFileStateManager_GetAllTimelines(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	symbols := []string{"BTCUSDT", "ETHUSDT"}
	for _, symbol := range symbols {
		timeline := testutils.CreateTestSymbolTimeline(symbol)
		err := manager.SaveTimeline(timeline)
		testutils.AssertNoError(t, err)
	}
	
	allTimelines, err := manager.GetAllTimelines()
	testutils.AssertNoError(t, err)
	
	if len(allTimelines) != len(symbols) {
		t.Errorf("Expected %d timelines, got %d", len(symbols), len(allTimelines))
	}
	
	for _, symbol := range symbols {
		if timeline, exists := allTimelines[symbol]; !exists {
			t.Errorf("Expected timeline for symbol %s to exist", symbol)
		} else if timeline.Symbol != symbol {
			t.Errorf("Expected symbol %s, got %s", symbol, timeline.Symbol)
		}
	}
}

func TestFileStateManager_DeleteTimeline(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	timeline := testutils.CreateTestSymbolTimeline("BTCUSDT")
	err := manager.SaveTimeline(timeline)
	testutils.AssertNoError(t, err)
	
	// 删除时间线
	err = manager.DeleteTimeline("BTCUSDT")
	testutils.AssertNoError(t, err)
	
	// 验证时间线被删除
	_, err = manager.GetTimeline("BTCUSDT")
	testutils.AssertError(t, err)
}

func TestFileStateManager_WorkerState(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	workerState := testutils.CreateTestWorkerState(1)
	workerState.Status = "working"
	workerState.CurrentSymbol = "BTCUSDT"
	workerState.CompletedTasks = 10
	
	// 更新Worker状态
	err := manager.UpdateWorkerState(1, workerState)
	testutils.AssertNoError(t, err)
	
	// 获取Worker状态
	retrievedState, err := manager.GetWorkerState(1)
	testutils.AssertNoError(t, err)
	
	if retrievedState.WorkerID != 1 {
		t.Errorf("Expected WorkerID 1, got %d", retrievedState.WorkerID)
	}
	
	if retrievedState.Status != "working" {
		t.Errorf("Expected Status working, got %s", retrievedState.Status)
	}
	
	if retrievedState.CurrentSymbol != "BTCUSDT" {
		t.Errorf("Expected CurrentSymbol BTCUSDT, got %s", retrievedState.CurrentSymbol)
	}
	
	if retrievedState.CompletedTasks != 10 {
		t.Errorf("Expected CompletedTasks 10, got %d", retrievedState.CompletedTasks)
	}
}

func TestFileStateManager_GetAllWorkerStates(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	workerIDs := []int{1, 2, 3}
	for _, workerID := range workerIDs {
		workerState := testutils.CreateTestWorkerState(workerID)
		err := manager.UpdateWorkerState(workerID, workerState)
		testutils.AssertNoError(t, err)
	}
	
	allWorkerStates, err := manager.GetAllWorkerStates()
	testutils.AssertNoError(t, err)
	
	if len(allWorkerStates) != len(workerIDs) {
		t.Errorf("Expected %d worker states, got %d", len(workerIDs), len(allWorkerStates))
	}
	
	for _, workerID := range workerIDs {
		if state, exists := allWorkerStates[workerID]; !exists {
			t.Errorf("Expected worker state for ID %d to exist", workerID)
		} else if state.WorkerID != workerID {
			t.Errorf("Expected WorkerID %d, got %d", workerID, state.WorkerID)
		}
	}
}

func TestFileStateManager_SymbolProgress(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	symbolProgress := testutils.CreateTestSymbolProgress("BTCUSDT")
	symbolProgress.TotalMonths = 24
	symbolProgress.CompletedMonths = 15
	symbolProgress.Progress = 62.5
	
	// 更新币种进度
	err := manager.UpdateSymbolProgress("BTCUSDT", symbolProgress)
	testutils.AssertNoError(t, err)
	
	// 获取币种进度
	retrievedProgress, err := manager.GetSymbolProgress("BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if retrievedProgress.Symbol != "BTCUSDT" {
		t.Errorf("Expected Symbol BTCUSDT, got %s", retrievedProgress.Symbol)
	}
	
	if retrievedProgress.TotalMonths != 24 {
		t.Errorf("Expected TotalMonths 24, got %d", retrievedProgress.TotalMonths)
	}
	
	if retrievedProgress.CompletedMonths != 15 {
		t.Errorf("Expected CompletedMonths 15, got %d", retrievedProgress.CompletedMonths)
	}
	
	if retrievedProgress.Progress != 62.5 {
		t.Errorf("Expected Progress 62.5, got %f", retrievedProgress.Progress)
	}
}

func TestFileStateManager_GetAllSymbolProgress(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	symbols := []string{"BTCUSDT", "ETHUSDT"}
	for _, symbol := range symbols {
		progress := testutils.CreateTestSymbolProgress(symbol)
		err := manager.UpdateSymbolProgress(symbol, progress)
		testutils.AssertNoError(t, err)
	}
	
	allProgress, err := manager.GetAllSymbolProgress()
	testutils.AssertNoError(t, err)
	
	if len(allProgress) != len(symbols) {
		t.Errorf("Expected %d symbol progress, got %d", len(symbols), len(allProgress))
	}
	
	for _, symbol := range symbols {
		if progress, exists := allProgress[symbol]; !exists {
			t.Errorf("Expected progress for symbol %s to exist", symbol)
		} else if progress.Symbol != symbol {
			t.Errorf("Expected symbol %s, got %s", symbol, progress.Symbol)
		}
	}
}

func TestFileStateManager_GetIncompleteSymbols(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	// 保存一些状态
	completeState := testutils.CreateTestProcessingState("BTCUSDT")
	completeState.Status = "completed"
	err := manager.SaveState(completeState)
	testutils.AssertNoError(t, err)
	
	incompleteState := testutils.CreateTestProcessingState("ETHUSDT")
	incompleteState.Status = "processing"
	err = manager.SaveState(incompleteState)
	testutils.AssertNoError(t, err)
	
	// 保存一些进度
	incompleteProgress := testutils.CreateTestSymbolProgress("ADAUSDT")
	incompleteProgress.Status = "processing"
	incompleteProgress.Progress = 50.0
	err = manager.UpdateSymbolProgress("ADAUSDT", incompleteProgress)
	testutils.AssertNoError(t, err)
	
	incompleteSymbols, err := manager.GetIncompleteSymbols()
	testutils.AssertNoError(t, err)
	
	// 应该包含未完成的符号
	expectedIncomplete := []string{"ETHUSDT", "ADAUSDT"}
	if len(incompleteSymbols) != len(expectedIncomplete) {
		t.Errorf("Expected %d incomplete symbols, got %d", len(expectedIncomplete), len(incompleteSymbols))
	}
	
	// 不应该包含已完成的符号
	for _, symbol := range incompleteSymbols {
		if symbol == "BTCUSDT" {
			t.Error("Expected completed symbol BTCUSDT not to be in incomplete list")
		}
	}
}

func TestFileStateManager_Backup(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	// 保存一些数据
	state := testutils.CreateTestProcessingState("BTCUSDT")
	err := manager.SaveState(state)
	testutils.AssertNoError(t, err)
	
	// 创建备份
	err = manager.Backup()
	testutils.AssertNoError(t, err)
	
	// 检查备份文件是否存在
	files, err := filepath.Glob(manager.filePath + ".backup_*")
	testutils.AssertNoError(t, err)
	
	if len(files) == 0 {
		t.Error("Expected backup file to be created")
	}
}

func TestFileStateManager_Restore(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	// 保存一些数据
	originalState := testutils.CreateTestProcessingState("BTCUSDT")
	originalState.Processed = 100
	err := manager.SaveState(originalState)
	testutils.AssertNoError(t, err)
	
	// 创建备份
	err = manager.Backup()
	testutils.AssertNoError(t, err)
	
	// 获取备份文件路径
	files, err := filepath.Glob(manager.filePath + ".backup_*")
	testutils.AssertNoError(t, err)
	if len(files) == 0 {
		t.Fatal("No backup files found")
	}
	backupPath := files[0]
	
	// 修改原始数据
	modifiedState := testutils.CreateTestProcessingState("BTCUSDT")
	modifiedState.Processed = 200
	err = manager.SaveState(modifiedState)
	testutils.AssertNoError(t, err)
	
	// 恢复备份
	err = manager.Restore(backupPath)
	testutils.AssertNoError(t, err)
	
	// 验证数据被恢复
	restoredState, err := manager.GetState("BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if restoredState.Processed != 100 {
		t.Errorf("Expected Processed 100 after restore, got %d", restoredState.Processed)
	}
}

func TestFileStateManager_PersistenceAcrossInstances(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	
	// 创建第一个管理器实例并保存数据
	manager1 := createTestStateManager(t, tempDir)
	state := testutils.CreateTestProcessingState("BTCUSDT")
	state.Processed = 150
	err := manager1.SaveState(state)
	testutils.AssertNoError(t, err)
	
	// 创建第二个管理器实例，应该能加载之前的数据
	manager2 := createTestStateManager(t, tempDir)
	retrievedState, err := manager2.GetState("BTCUSDT")
	testutils.AssertNoError(t, err)
	
	if retrievedState.Processed != 150 {
		t.Errorf("Expected Processed 150 in new instance, got %d", retrievedState.Processed)
	}
	
	if retrievedState.Symbol != "BTCUSDT" {
		t.Errorf("Expected Symbol BTCUSDT in new instance, got %s", retrievedState.Symbol)
	}
}

func TestFileStateManager_ConcurrentAccess(t *testing.T) {
	tempDir := testutils.CreateTempDir(t, "state_manager_test")
	manager := createTestStateManager(t, tempDir)
	
	// 并发读写测试
	symbols := []string{"BTCUSDT", "ETHUSDT", "ADAUSDT", "BNBUSDT", "XRPUSDT"}
	done := make(chan bool, len(symbols)*2)
	
	// 并发写入
	for i, symbol := range symbols {
		go func(symbol string, processed int) {
			state := testutils.CreateTestProcessingState(symbol)
			state.Processed = processed
			err := manager.SaveState(state)
			if err != nil {
				t.Errorf("Error saving state for %s: %v", symbol, err)
			}
			done <- true
		}(symbol, i*10)
	}
	
	// 并发读取
	for _, symbol := range symbols {
		go func(symbol string) {
			_, err := manager.GetState(symbol)
			if err != nil {
				t.Errorf("Error getting state for %s: %v", symbol, err)
			}
			done <- true
		}(symbol)
	}
	
	// 等待所有操作完成
	for i := 0; i < len(symbols)*2; i++ {
		<-done
	}
	
	// 验证数据完整性
	allStates, err := manager.GetAllStates()
	testutils.AssertNoError(t, err)
	
	if len(allStates) != len(symbols) {
		t.Errorf("Expected %d states after concurrent access, got %d", len(symbols), len(allStates))
	}
}

// 辅助函数
func createTestStateManager(t *testing.T, tempDir string) *FileStateManager {
	cfg := config.StateConfig{
		StorageType:  "file",
		FilePath:     filepath.Join(tempDir, "progress.json"),
		BackupCount:  5,
		SaveInterval: 10 * time.Second,
	}
	
	manager, err := NewFileStateManager(cfg)
	testutils.AssertNoError(t, err)
	
	return manager
}

// 基准测试
func BenchmarkFileStateManager_SaveState(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_state_manager")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	cfg := config.StateConfig{
		StorageType:  "file",
		FilePath:     filepath.Join(tempDir, "progress.json"),
		BackupCount:  5,
		SaveInterval: 10 * time.Second,
	}
	
	manager, err := NewFileStateManager(cfg)
	if err != nil {
		b.Fatalf("Failed to create state manager: %v", err)
	}
	
	state := &domain.ProcessingState{
		Symbol:      "BTCUSDT",
		Status:      "processing",
		Processed:   100,
		Failed:      5,
		LastUpdated: time.Now(),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state.Processed = i
		_ = manager.SaveState(state)
	}
}

func BenchmarkFileStateManager_GetState(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_state_manager")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	cfg := config.StateConfig{
		StorageType:  "file",
		FilePath:     filepath.Join(tempDir, "progress.json"),
		BackupCount:  5,
		SaveInterval: 10 * time.Second,
	}
	
	manager, err := NewFileStateManager(cfg)
	if err != nil {
		b.Fatalf("Failed to create state manager: %v", err)
	}
	
	// 预先保存一些数据
	state := &domain.ProcessingState{
		Symbol:      "BTCUSDT",
		Status:      "processing",
		Processed:   100,
		Failed:      5,
		LastUpdated: time.Now(),
	}
	_ = manager.SaveState(state)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = manager.GetState("BTCUSDT")
	}
}