package importer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/semaphore"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
)

// Importer 数据导入器
type Importer struct {
	config           config.ImporterConfig
	downloader       domain.Downloader
	parser           domain.Parser
	repository       domain.KLineRepository
	stateManager     domain.StateManager
	progressReporter domain.ProgressReporter
	logger           zerolog.Logger
	semaphore        *semaphore.Weighted
	buffer           []domain.KLine
	bufferMutex      sync.Mutex
	lastFlush        time.Time
	
	// 并发控制
	maxWorkers       int
	// 并发worker状态管理
	workerStates     map[int]*domain.WorkerState
	workerStatesMux  sync.RWMutex
}

// NewImporter 创建新的数据导入器
func NewImporter(
	cfg config.ImporterConfig,
	downloader domain.Downloader,
	parser domain.Parser,
	repository domain.KLineRepository,
	stateManager domain.StateManager,
	progressReporter domain.ProgressReporter,
) *Importer {
	// 设置默认最大worker数为5
	maxWorkers := 5
	
	return &Importer{
		config:           cfg,
		downloader:       downloader,
		parser:           parser,
		repository:       repository,
		stateManager:     stateManager,
		progressReporter: progressReporter,
		logger:           logger.GetLogger("importer"),
		semaphore:        semaphore.NewWeighted(int64(cfg.BufferSize)),
		buffer:           make([]domain.KLine, 0, cfg.BufferSize),
		lastFlush:        time.Now(),
		maxWorkers:       maxWorkers,
		// 初始化worker状态管理
		workerStates:     make(map[int]*domain.WorkerState),
	}
}

// ImportData 导入数据 - 顺序处理，确保任务成功后才继续下一个
func (i *Importer) ImportData(ctx context.Context, tasks []domain.DownloadTask) error {
	i.logger.Info().
		Int("task_count", len(tasks)).
		Msg("Starting data import (sequential processing)")

	start := time.Now()
	defer func() {
		logger.LogPerformance("importer", "import_data", time.Since(start), map[string]interface{}{
			"task_count": len(tasks),
		})
	}()

	// 启动缓冲区刷新协程
	flushCtx, flushCancel := context.WithCancel(ctx)
	defer flushCancel()
	go i.bufferFlusher(flushCtx)

	// 顺序处理任务，确保每个任务成功后才继续下一个
	successCount := 0
	for idx, task := range tasks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		i.logger.Info().
			Int("current", idx+1).
			Int("total", len(tasks)).
			Str("symbol", task.Symbol).
			Str("month", task.Date.Format("2006-01")).
			Msg("Processing monthly task")

		if err := i.processTask(ctx, task); err != nil {
			i.logger.Error().
				Err(err).
				Str("symbol", task.Symbol).
				Str("month", task.Date.Format("2006-01")).
				Msg("Failed to process monthly task, stopping import")
			return fmt.Errorf("failed to process task for %s %s: %w", task.Symbol, task.Date.Format("2006-01"), err)
		}

		successCount++
		i.logger.Info().
			Int("completed", successCount).
			Int("total", len(tasks)).
			Str("symbol", task.Symbol).
			Str("month", task.Date.Format("2006-01")).
			Msg("Monthly task completed successfully")
	}

	// 最终刷新缓冲区
	if err := i.flushBuffer(ctx); err != nil {
		i.logger.Error().Err(err).Msg("Failed to flush final buffer")
		return err
	}

	i.logger.Info().
		Int("completed_tasks", successCount).
		Int("total_tasks", len(tasks)).
		Dur("duration", time.Since(start)).
		Msg("Monthly data import completed successfully")

	return nil
}

// worker 工作协程
func (i *Importer) worker(ctx context.Context, taskChan <-chan domain.DownloadTask, errorChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	for task := range taskChan {
		select {
		case <-ctx.Done():
			errorChan <- ctx.Err()
			return
		default:
		}

		if err := i.processTask(ctx, task); err != nil {
			i.logger.Error().
				Err(err).
				Str("symbol", task.Symbol).
				Str("date", task.Date.Format("2006-01-02")).
				Msg("Failed to process task")
			errorChan <- err
		} else {
			errorChan <- nil
		}
	}
}

// processTask 处理单个任务
func (i *Importer) processTask(ctx context.Context, task domain.DownloadTask) error {
	// 获取信号量
	if err := i.semaphore.Acquire(ctx, 1); err != nil {
		return err
	}
	defer i.semaphore.Release(1)

	start := time.Now()
	defer func() {
		logger.LogPerformance("importer", "process_task", time.Since(start), map[string]interface{}{
			"symbol": task.Symbol,
			"date":   task.Date.Format("2006-01-02"),
		})
	}()

	i.logger.Debug().
		Str("symbol", task.Symbol).
		Str("date", task.Date.Format("2006-01-02")).
		Msg("Processing task")

	// 检查状态管理器中的进度
	state, err := i.stateManager.GetState(task.Symbol)
	if err != nil {
		return fmt.Errorf("failed to get state: %w", err)
	}

	// 检查是否已经处理过这个日期
		if !state.LastDate.IsZero() && !task.Date.After(state.LastDate) {
			i.logger.Debug().
				Str("symbol", task.Symbol).
				Str("date", task.Date.Format("2006-01-02")).
				Msg("Date already processed, skipping")
			return nil
		}

	// 下载数据
	data, err := i.downloader.Fetch(ctx, task)
	if err != nil {
		return fmt.Errorf("failed to download data: %w", err)
	}

	if len(data) == 0 {
		i.logger.Warn().
			Str("symbol", task.Symbol).
			Str("date", task.Date.Format("2006-01-02")).
			Msg("No data downloaded")
		return nil
	}

	// 解析数据
	klines, validationResult, err := i.parser.Parse(ctx, data, task.Symbol)
	if err != nil {
		return fmt.Errorf("failed to parse data: %w", err)
	}

	// 记录解析结果
	logger.LogDataQuality("importer", task.Symbol, validationResult.TotalRows, validationResult.ValidRows, validationResult.InvalidRows)

	// 如果启用了去重，检查重复数据
	if i.config.EnableDeduplication {
		klines = i.deduplicateKLines(klines)
	}

	// 添加到缓冲区
	if err := i.addToBuffer(ctx, klines); err != nil {
		return fmt.Errorf("failed to add to buffer: %w", err)
	}

	// 更新状态
	state.LastDate = task.Date
	state.Processed++
	state.LastUpdated = time.Now()

	if err := i.stateManager.SaveState(state); err != nil {
		i.logger.Warn().
			Err(err).
			Str("symbol", task.Symbol).
			Msg("Failed to save state")
	}

	// 报告进度
	if i.progressReporter != nil {
		progress := &domain.ProgressReport{
			CurrentSymbol:  task.Symbol,
			CurrentDate:    task.Date,
			CompletedTasks: state.Processed,
			TotalTasks:     state.TotalFiles,
			Progress:       float64(state.Processed) / float64(state.TotalFiles) * 100,
			StartTime:      time.Now(),
		}
		i.progressReporter.ReportProgress(progress)
	}

	i.logger.Debug().
		Str("symbol", task.Symbol).
		Str("date", task.Date.Format("2006-01-02")).
		Int("records", len(klines)).
		Msg("Task processed successfully")

	return nil
}

// addToBuffer 添加数据到缓冲区
func (i *Importer) addToBuffer(ctx context.Context, klines []domain.KLine) error {
	i.bufferMutex.Lock()
	defer i.bufferMutex.Unlock()

	i.buffer = append(i.buffer, klines...)

	// 检查是否需要刷新缓冲区
	if len(i.buffer) >= i.config.BufferSize {
		return i.flushBufferUnsafe(ctx)
	}

	return nil
}

// bufferFlusher 定期刷新缓冲区
func (i *Importer) bufferFlusher(ctx context.Context) {
	ticker := time.NewTicker(i.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := i.flushBuffer(ctx); err != nil {
				i.logger.Error().Err(err).Msg("Failed to flush buffer")
			}
		}
	}
}

// flushBuffer 刷新缓冲区
func (i *Importer) flushBuffer(ctx context.Context) error {
	i.bufferMutex.Lock()
	defer i.bufferMutex.Unlock()
	return i.flushBufferUnsafe(ctx)
}

// flushBufferUnsafe 刷新缓冲区（不加锁）
func (i *Importer) flushBufferUnsafe(ctx context.Context) error {
	if len(i.buffer) == 0 {
		return nil
	}

	start := time.Now()
	batchSize := len(i.buffer)

	// 保存到数据库
	if err := i.repository.Save(ctx, i.buffer); err != nil {
		return fmt.Errorf("failed to save batch: %w", err)
	}

	// 清空缓冲区
	i.buffer = i.buffer[:0]
	i.lastFlush = time.Now()

	logger.LogPerformance("importer", "flush_buffer", time.Since(start), map[string]interface{}{
		"batch_size": batchSize,
	})

	i.logger.Debug().
		Int("batch_size", batchSize).
		Dur("duration", time.Since(start)).
		Msg("Buffer flushed successfully")

	return nil
}

// deduplicateKLines 去重K线数据
func (i *Importer) deduplicateKLines(klines []domain.KLine) []domain.KLine {
	if len(klines) <= 1 {
		return klines
	}

	seen := make(map[string]bool)
	result := make([]domain.KLine, 0, len(klines))

	for _, kline := range klines {
		// 创建唯一键：symbol + open_time
		key := fmt.Sprintf("%s_%d", kline.Symbol, kline.OpenTime.Unix())
		if !seen[key] {
			seen[key] = true
			result = append(result, kline)
		}
	}

	if len(result) < len(klines) {
		i.logger.Debug().
			Int("original", len(klines)).
			Int("deduplicated", len(result)).
			Int("removed", len(klines)-len(result)).
			Msg("Deduplicated K-lines")
	}

	return result
}

// GetBufferStatus 获取缓冲区状态
func (i *Importer) GetBufferStatus() map[string]interface{} {
	i.bufferMutex.Lock()
	defer i.bufferMutex.Unlock()

	return map[string]interface{}{
		"buffer_size":     len(i.buffer),
		"buffer_capacity": cap(i.buffer),
		"last_flush":      i.lastFlush,
		"time_since_flush": time.Since(i.lastFlush),
	}
}

// ImportDataConcurrent 并发导入数据，支持多个worker同时处理不同币种
func (i *Importer) ImportDataConcurrent(ctx context.Context, concurrentTasks []domain.ConcurrentTask) error {
	i.logger.Info().
		Int("concurrent_tasks", len(concurrentTasks)).
		Int("max_workers", i.maxWorkers).
		Msg("Starting concurrent data import")

	start := time.Now()
	defer func() {
		logger.LogPerformance("importer", "import_data_concurrent", time.Since(start), map[string]interface{}{
			"concurrent_tasks": len(concurrentTasks),
			"max_workers":      i.maxWorkers,
		})
	}()

	// 启动缓冲区刷新协程
	flushCtx, flushCancel := context.WithCancel(ctx)
	defer flushCancel()
	go i.bufferFlusher(flushCtx)

	// 初始化worker状态
	for workerID := 0; workerID < i.maxWorkers; workerID++ {
		i.updateWorkerState(workerID, &domain.WorkerState{
			WorkerID:       workerID,
			Status:         "idle",
			StartTime:      time.Now(),
			LastUpdate:     time.Now(),
			TasksCount:     0,
			CompletedTasks: 0,
			FailedTasks:    0,
		})
	}

	// 创建任务通道和错误通道
	taskChan := make(chan domain.ConcurrentTask, len(concurrentTasks))
	errorChan := make(chan error, len(concurrentTasks))
	var wg sync.WaitGroup

	// 启动worker协程
	for workerID := 0; workerID < i.maxWorkers; workerID++ {
		wg.Add(1)
		go i.concurrentWorker(ctx, workerID, taskChan, errorChan, &wg)
	}

	// 发送任务到通道
	for _, task := range concurrentTasks {
		taskChan <- task
	}
	close(taskChan)

	// 等待所有worker完成
	go func() {
		wg.Wait()
		close(errorChan)
	}()

	// 收集错误
	var errors []error
	for err := range errorChan {
		if err != nil {
			errors = append(errors, err)
		}
	}

	// 最终刷新缓冲区
	if err := i.flushBuffer(ctx); err != nil {
		i.logger.Error().Err(err).Msg("Failed to flush final buffer")
		errors = append(errors, err)
	}

	// 更新所有worker状态为完成
	for workerID := 0; workerID < i.maxWorkers; workerID++ {
		i.updateWorkerState(workerID, &domain.WorkerState{
			WorkerID:   workerID,
			Status:     "completed",
			LastUpdate: time.Now(),
		})
	}

	if len(errors) > 0 {
		i.logger.Error().
			Int("error_count", len(errors)).
			Msg("Concurrent import completed with errors")
		return fmt.Errorf("concurrent import failed with %d errors: %v", len(errors), errors[0])
	}

	i.logger.Info().
		Int("completed_tasks", len(concurrentTasks)).
		Dur("duration", time.Since(start)).
		Msg("Concurrent data import completed successfully")

	return nil
}

// concurrentWorker worker协程，处理并发任务
func (i *Importer) concurrentWorker(ctx context.Context, workerID int, taskChan <-chan domain.ConcurrentTask, errorChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	i.logger.Info().
		Int("worker_id", workerID).
		Msg("Worker started")

	for concurrentTask := range taskChan {
		select {
		case <-ctx.Done():
			errorChan <- ctx.Err()
			return
		default:
		}

		// 更新worker状态
		i.updateWorkerState(workerID, &domain.WorkerState{
			WorkerID:      workerID,
			Status:        "running",
			CurrentSymbol: concurrentTask.Symbol,
			StartTime:     time.Now(),
			LastUpdate:    time.Now(),
			TasksCount:    len(concurrentTask.Tasks),
		})

		i.logger.Info().
			Int("worker_id", workerID).
			Str("symbol", concurrentTask.Symbol).
			Int("tasks_count", len(concurrentTask.Tasks)).
			Msg("Worker processing symbol")

		// 处理该币种的所有任务
		if err := i.processConcurrentTask(ctx, workerID, concurrentTask); err != nil {
			i.logger.Error().
				Err(err).
				Int("worker_id", workerID).
				Str("symbol", concurrentTask.Symbol).
				Msg("Worker failed to process symbol")

			// 更新worker状态为失败
			i.updateWorkerState(workerID, &domain.WorkerState{
				WorkerID:     workerID,
				Status:       "failed",
				LastUpdate:   time.Now(),
				FailedTasks:  1,
				ErrorMessage: err.Error(),
			})

			errorChan <- fmt.Errorf("worker %d failed to process symbol %s: %w", workerID, concurrentTask.Symbol, err)
		} else {
			i.logger.Info().
				Int("worker_id", workerID).
				Str("symbol", concurrentTask.Symbol).
				Int("completed_tasks", len(concurrentTask.Tasks)).
				Msg("Worker completed symbol successfully")

			// 更新worker状态为完成
			i.updateWorkerState(workerID, &domain.WorkerState{
				WorkerID:       workerID,
				Status:         "idle",
				LastUpdate:     time.Now(),
				CompletedTasks: 1,
			})

			errorChan <- nil
		}
	}

	i.logger.Info().
		Int("worker_id", workerID).
		Msg("Worker finished")
}

// processConcurrentTask 处理单个并发任务（一个币种的所有月份数据）
func (i *Importer) processConcurrentTask(ctx context.Context, workerID int, concurrentTask domain.ConcurrentTask) error {
	start := time.Now()
	defer func() {
		logger.LogPerformance("importer", "process_concurrent_task", time.Since(start), map[string]interface{}{
			"worker_id": workerID,
			"symbol":    concurrentTask.Symbol,
			"tasks":     len(concurrentTask.Tasks),
		})
	}()

	i.logger.Debug().
		Int("worker_id", workerID).
		Str("symbol", concurrentTask.Symbol).
		Int("tasks_count", len(concurrentTask.Tasks)).
		Msg("Processing concurrent task")

	// 更新币种进度信息
	if err := i.stateManager.UpdateSymbolProgress(concurrentTask.Symbol, &domain.SymbolProgressInfo{
		Symbol:       concurrentTask.Symbol,
		TotalMonths:  len(concurrentTask.Tasks),
		CurrentMonth: concurrentTask.Tasks[0].Date.Format("2006-01"),
		Status:       "running",
		LastUpdate:   time.Now(),
		WorkerID:     workerID,
	}); err != nil {
		i.logger.Warn().Err(err).Msg("Failed to update symbol progress")
	}

	// 顺序处理该币种的所有任务，确保时间顺序
	successCount := 0
	for idx, task := range concurrentTask.Tasks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 更新当前处理的月份
		if err := i.stateManager.UpdateSymbolProgress(concurrentTask.Symbol, &domain.SymbolProgressInfo{
			Symbol:          concurrentTask.Symbol,
			TotalMonths:     len(concurrentTask.Tasks),
			CompletedMonths: successCount,
			CurrentMonth:    task.Date.Format("2006-01"),
			Progress:        float64(successCount) / float64(len(concurrentTask.Tasks)) * 100,
			Status:          "running",
			LastUpdate:      time.Now(),
			WorkerID:        workerID,
		}); err != nil {
			i.logger.Warn().Err(err).Msg("Failed to update symbol progress")
		}

		i.logger.Debug().
			Int("worker_id", workerID).
			Int("current", idx+1).
			Int("total", len(concurrentTask.Tasks)).
			Str("symbol", task.Symbol).
			Str("month", task.Date.Format("2006-01")).
			Msg("Processing monthly task in worker")

		if err := i.processTask(ctx, task); err != nil {
			i.logger.Error().
				Err(err).
				Int("worker_id", workerID).
				Str("symbol", task.Symbol).
				Str("month", task.Date.Format("2006-01")).
				Msg("Failed to process monthly task in worker")

			// 更新失败状态
			if err := i.stateManager.UpdateSymbolProgress(concurrentTask.Symbol, &domain.SymbolProgressInfo{
				Symbol:       concurrentTask.Symbol,
				FailedMonths: 1,
				Status:       "failed",
				LastUpdate:   time.Now(),
				WorkerID:     workerID,
			}); err != nil {
				i.logger.Warn().Err(err).Msg("Failed to update symbol progress")
			}

			return fmt.Errorf("failed to process task for %s %s: %w", task.Symbol, task.Date.Format("2006-01"), err)
		}

		successCount++
		i.logger.Debug().
			Int("worker_id", workerID).
			Int("completed", successCount).
			Int("total", len(concurrentTask.Tasks)).
			Str("symbol", task.Symbol).
			Str("month", task.Date.Format("2006-01")).
			Msg("Monthly task completed successfully in worker")
	}

	// 更新最终完成状态
	if err := i.stateManager.UpdateSymbolProgress(concurrentTask.Symbol, &domain.SymbolProgressInfo{
		Symbol:          concurrentTask.Symbol,
		TotalMonths:     len(concurrentTask.Tasks),
		CompletedMonths: successCount,
		Progress:        100.0,
		Status:          "completed",
		LastUpdate:      time.Now(),
		WorkerID:        workerID,
	}); err != nil {
		i.logger.Warn().Err(err).Msg("Failed to update final symbol progress")
	}

	i.logger.Info().
		Int("worker_id", workerID).
		Str("symbol", concurrentTask.Symbol).
		Int("completed_tasks", successCount).
		Int("total_tasks", len(concurrentTask.Tasks)).
		Dur("duration", time.Since(start)).
		Msg("Concurrent task completed successfully")

	return nil
}

// updateWorkerState 更新worker状态
func (i *Importer) updateWorkerState(workerID int, state *domain.WorkerState) {
	i.workerStatesMux.Lock()
	defer i.workerStatesMux.Unlock()

	// 如果是更新现有状态，保留一些字段
	if existingState, exists := i.workerStates[workerID]; exists {
		if state.TasksCount == 0 {
			state.TasksCount = existingState.TasksCount
		}
		if state.CompletedTasks == 0 {
			state.CompletedTasks = existingState.CompletedTasks
		}
		if state.FailedTasks == 0 {
			state.FailedTasks = existingState.FailedTasks
		}
		if state.StartTime.IsZero() {
			state.StartTime = existingState.StartTime
		}
	}

	i.workerStates[workerID] = state

	// 同时更新到状态管理器
	if err := i.stateManager.UpdateWorkerState(workerID, state); err != nil {
		i.logger.Warn().
			Err(err).
			Int("worker_id", workerID).
			Msg("Failed to update worker state in state manager")
	}
}

// GetWorkerStates 获取所有worker状态
func (i *Importer) GetWorkerStates() map[int]*domain.WorkerState {
	i.workerStatesMux.RLock()
	defer i.workerStatesMux.RUnlock()

	states := make(map[int]*domain.WorkerState)
	for id, state := range i.workerStates {
		stateCopy := *state
		states[id] = &stateCopy
	}
	return states
}

// Close 关闭导入器
func (i *Importer) Close() error {
	// 刷新剩余缓冲区
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := i.flushBuffer(ctx); err != nil {
		i.logger.Error().Err(err).Msg("Failed to flush buffer during close")
		return err
	}

	i.logger.Info().Msg("Importer closed successfully")
	return nil
}