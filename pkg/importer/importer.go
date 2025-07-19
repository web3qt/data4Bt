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
	}
}

// ImportData 导入数据
func (i *Importer) ImportData(ctx context.Context, tasks []domain.DownloadTask) error {
	i.logger.Info().
		Int("task_count", len(tasks)).
		Msg("Starting data import")

	start := time.Now()
	defer func() {
		logger.LogPerformance("importer", "import_data", time.Since(start), map[string]interface{}{
			"task_count": len(tasks),
		})
	}()

	// 创建工作池
	workerCount := i.config.BatchSize
	if workerCount > len(tasks) {
		workerCount = len(tasks)
	}

	taskChan := make(chan domain.DownloadTask, len(tasks))
	errorChan := make(chan error, len(tasks))
	var wg sync.WaitGroup

	// 启动工作协程
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go i.worker(ctx, taskChan, errorChan, &wg)
	}

	// 启动缓冲区刷新协程
	flushCtx, flushCancel := context.WithCancel(ctx)
	defer flushCancel()
	go i.bufferFlusher(flushCtx)

	// 发送任务
	for _, task := range tasks {
		select {
		case taskChan <- task:
		case <-ctx.Done():
			close(taskChan)
			return ctx.Err()
		}
	}
	close(taskChan)

	// 等待所有工作完成
	wg.Wait()
	close(errorChan)

	// 最终刷新缓冲区
	if err := i.flushBuffer(ctx); err != nil {
		i.logger.Error().Err(err).Msg("Failed to flush final buffer")
		return err
	}

	// 检查错误
	var errors []error
	for err := range errorChan {
		if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		i.logger.Error().
			Int("error_count", len(errors)).
			Msg("Import completed with errors")
		return fmt.Errorf("import failed with %d errors: %v", len(errors), errors[0])
	}

	i.logger.Info().
		Dur("duration", time.Since(start)).
		Msg("Data import completed successfully")

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