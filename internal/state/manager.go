package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
)

// FileStateManager 基于文件的状态管理器
type FileStateManager struct {
	mu            sync.RWMutex
	filePath      string
	timelinePath  string
	backupCount   int
	states        map[string]*domain.ProcessingState
	timelines     map[string]*domain.SymbolTimeline
	logger        zerolog.Logger
}

// NewFileStateManager 创建新的文件状态管理器
func NewFileStateManager(cfg config.StateConfig) (*FileStateManager, error) {
	// 创建状态文件目录
	stateDir := filepath.Dir(cfg.FilePath)
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}
	
	// 生成时间线文件路径
	timelinePath := filepath.Join(stateDir, "timelines.json")
	
	manager := &FileStateManager{
		filePath:     cfg.FilePath,
		timelinePath: timelinePath,
		backupCount:  cfg.BackupCount,
		states:       make(map[string]*domain.ProcessingState),
		timelines:    make(map[string]*domain.SymbolTimeline),
		logger:       logger.GetLogger("state_manager"),
	}
	
	// 加载现有状态
	if err := manager.load(); err != nil {
		manager.logger.Warn().Err(err).Msg("Failed to load existing state, starting fresh")
	}
	
	// 加载现有时间线
	if err := manager.loadTimelines(); err != nil {
		manager.logger.Warn().Err(err).Msg("Failed to load existing timelines, starting fresh")
	}
	
	return manager, nil
}

// GetState 获取处理状态
func (m *FileStateManager) GetState(symbol string) (*domain.ProcessingState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	state, exists := m.states[symbol]
	if !exists {
		// 返回默认状态
		return &domain.ProcessingState{
			Symbol:      symbol,
			LastDate:    time.Time{}, // 零值表示从未处理过
			TotalFiles:  0,
			Processed:   0,
			Failed:      0,
			LastUpdated: time.Now(),
		}, nil
	}
	
	// 返回状态的副本
	stateCopy := *state
	return &stateCopy, nil
}

// SaveState 保存处理状态
func (m *FileStateManager) SaveState(state *domain.ProcessingState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// 更新最后更新时间
	state.LastUpdated = time.Now()
	
	// 保存到内存
	m.states[state.Symbol] = state
	
	// 持久化到文件
	if err := m.save(); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}
	
	m.logger.Debug().
		Str("symbol", state.Symbol).
		Time("last_date", state.LastDate).
		Int("processed", state.Processed).
		Int("failed", state.Failed).
		Msg("State saved")
	
	return nil
}

// GetAllStates 获取所有状态
func (m *FileStateManager) GetAllStates() (map[string]*domain.ProcessingState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// 创建状态副本
	statesCopy := make(map[string]*domain.ProcessingState)
	for symbol, state := range m.states {
		stateCopy := *state
		statesCopy[symbol] = &stateCopy
	}
	
	return statesCopy, nil
}

// DeleteState 删除状态
func (m *FileStateManager) DeleteState(symbol string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	delete(m.states, symbol)
	
	if err := m.save(); err != nil {
		return fmt.Errorf("failed to save state after deletion: %w", err)
	}
	
	m.logger.Info().Str("symbol", symbol).Msg("State deleted")
	return nil
}

// Backup 备份状态
func (m *FileStateManager) Backup() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// 生成备份文件名
	timestamp := time.Now().Format("20060102_150405")
	backupPath := fmt.Sprintf("%s.backup_%s", m.filePath, timestamp)
	
	// 复制当前状态文件
	if err := m.copyFile(m.filePath, backupPath); err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}
	
	// 清理旧备份
	if err := m.cleanupOldBackups(); err != nil {
		m.logger.Warn().Err(err).Msg("Failed to cleanup old backups")
	}
	
	m.logger.Info().Str("backup_path", backupPath).Msg("State backup created")
	return nil
}

// Restore 恢复状态
func (m *FileStateManager) Restore(backupPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// 检查备份文件是否存在
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup file does not exist: %s", backupPath)
	}
	
	// 备份当前状态
	currentBackup := fmt.Sprintf("%s.before_restore_%s", m.filePath, time.Now().Format("20060102_150405"))
	if err := m.copyFile(m.filePath, currentBackup); err != nil {
		m.logger.Warn().Err(err).Msg("Failed to backup current state before restore")
	}
	
	// 恢复备份
	if err := m.copyFile(backupPath, m.filePath); err != nil {
		return fmt.Errorf("failed to restore backup: %w", err)
	}
	
	// 重新加载状态
	if err := m.load(); err != nil {
		return fmt.Errorf("failed to load restored state: %w", err)
	}
	
	m.logger.Info().Str("backup_path", backupPath).Msg("State restored from backup")
	return nil
}

// load 从文件加载状态
func (m *FileStateManager) load() error {
	if _, err := os.Stat(m.filePath); os.IsNotExist(err) {
		// 文件不存在，使用空状态
		m.states = make(map[string]*domain.ProcessingState)
		return nil
	}
	
	data, err := os.ReadFile(m.filePath)
	if err != nil {
		return fmt.Errorf("failed to read state file: %w", err)
	}
	
	if len(data) == 0 {
		// 空文件，使用空状态
		m.states = make(map[string]*domain.ProcessingState)
		return nil
	}
	
	var states map[string]*domain.ProcessingState
	if err := json.Unmarshal(data, &states); err != nil {
		return fmt.Errorf("failed to unmarshal state data: %w", err)
	}
	
	m.states = states
	m.logger.Info().Int("symbols_count", len(states)).Msg("State loaded from file")
	return nil
}

// save 保存状态到文件
func (m *FileStateManager) save() error {
	data, err := json.MarshalIndent(m.states, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state data: %w", err)
	}
	
	// 写入临时文件
	tempPath := m.filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp state file: %w", err)
	}
	
	// 原子性替换
	if err := os.Rename(tempPath, m.filePath); err != nil {
		return fmt.Errorf("failed to replace state file: %w", err)
	}
	
	return nil
}

// copyFile 复制文件
func (m *FileStateManager) copyFile(src, dst string) error {
	srcData, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	
	return os.WriteFile(dst, srcData, 0644)
}

// cleanupOldBackups 清理旧备份文件
func (m *FileStateManager) cleanupOldBackups() error {
	dir := filepath.Dir(m.filePath)
	baseName := filepath.Base(m.filePath)
	
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	
	// 查找备份文件
	var backupFiles []os.DirEntry
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		
		name := entry.Name()
		if len(name) > len(baseName)+8 && name[:len(baseName)+8] == baseName+".backup_" {
			backupFiles = append(backupFiles, entry)
		}
	}
	
	// 如果备份文件数量超过限制，删除最旧的
	if len(backupFiles) > m.backupCount {
		// 按修改时间排序（最旧的在前）
		for i := 0; i < len(backupFiles)-m.backupCount; i++ {
			backupPath := filepath.Join(dir, backupFiles[i].Name())
			if err := os.Remove(backupPath); err != nil {
				m.logger.Warn().Err(err).Str("file", backupPath).Msg("Failed to remove old backup")
			}
		}
	}
	
	return nil
}

// GetProgress 获取总体进度
func (m *FileStateManager) GetProgress() (*domain.ProgressReport, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	totalTasks := 0
	completedTasks := 0
	failedTasks := 0
	
	for _, state := range m.states {
		totalTasks += state.TotalFiles
		completedTasks += state.Processed
		failedTasks += state.Failed
	}
	
	progress := float64(0)
	if totalTasks > 0 {
		progress = float64(completedTasks) / float64(totalTasks) * 100
	}
	
	return &domain.ProgressReport{
		TotalTasks:     totalTasks,
		CompletedTasks: completedTasks,
		FailedTasks:    failedTasks,
		Progress:       progress,
	}, nil
}

// GetTimeline 获取代币时间线状态
func (m *FileStateManager) GetTimeline(symbol string) (*domain.SymbolTimeline, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if timeline, exists := m.timelines[symbol]; exists {
		// 返回副本以避免并发修改
		copy := *timeline
		return &copy, nil
	}
	
	return nil, fmt.Errorf("timeline not found for symbol: %s", symbol)
}

// SaveTimeline 保存代币时间线状态
func (m *FileStateManager) SaveTimeline(timeline *domain.SymbolTimeline) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// 更新最后修改时间
	timeline.LastUpdated = time.Now()
	
	// 保存到内存
	m.timelines[timeline.Symbol] = timeline
	
	// 持久化到文件
	if err := m.saveTimelines(); err != nil {
		return fmt.Errorf("failed to save timeline for %s: %w", timeline.Symbol, err)
	}
	
	m.logger.Debug().
		Str("symbol", timeline.Symbol).
		Str("status", timeline.Status).
		Int("total_months", timeline.TotalMonths).
		Int("imported_months", timeline.ImportedMonthsCount).
		Msg("Timeline saved successfully")
	
	return nil
}

// GetAllTimelines 获取所有代币时间线状态
func (m *FileStateManager) GetAllTimelines() (map[string]*domain.SymbolTimeline, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// 返回副本以避免并发修改
	result := make(map[string]*domain.SymbolTimeline)
	for symbol, timeline := range m.timelines {
		copy := *timeline
		result[symbol] = &copy
	}
	
	return result, nil
}

// DeleteTimeline 删除代币时间线状态
func (m *FileStateManager) DeleteTimeline(symbol string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	delete(m.timelines, symbol)
	
	if err := m.saveTimelines(); err != nil {
		return fmt.Errorf("failed to delete timeline for %s: %w", symbol, err)
	}
	
	m.logger.Info().
		Str("symbol", symbol).
		Msg("Timeline deleted successfully")
	
	return nil
}

// loadTimelines 从文件加载时间线状态
func (m *FileStateManager) loadTimelines() error {
	if _, err := os.Stat(m.timelinePath); os.IsNotExist(err) {
		m.logger.Info().Msg("Timeline file does not exist, starting with empty timelines")
		return nil
	}
	
	data, err := os.ReadFile(m.timelinePath)
	if err != nil {
		return fmt.Errorf("failed to read timeline file: %w", err)
	}
	
	if len(data) == 0 {
		m.logger.Info().Msg("Timeline file is empty, starting with empty timelines")
		return nil
	}
	
	if err := json.Unmarshal(data, &m.timelines); err != nil {
		return fmt.Errorf("failed to unmarshal timeline data: %w", err)
	}
	
	m.logger.Info().
		Int("timeline_count", len(m.timelines)).
		Msg("Timelines loaded successfully")
	
	return nil
}

// saveTimelines 保存时间线状态到文件
func (m *FileStateManager) saveTimelines() error {
	data, err := json.MarshalIndent(m.timelines, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal timeline data: %w", err)
	}
	
	if err := os.WriteFile(m.timelinePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write timeline file: %w", err)
	}
	
	return nil
}