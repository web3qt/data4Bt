package scheduler

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/rs/zerolog"
	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
)

// Scheduler 调度器
type Scheduler struct {
	config           config.SchedulerConfig
	repository       domain.KLineRepository
	downloader       domain.Downloader
	stateManager     domain.StateManager
	progressReporter domain.ProgressReporter
	importer         domain.Importer
	logger           zerolog.Logger
}

// NewScheduler 创建新的调度器
func NewScheduler(
	cfg config.SchedulerConfig,
	downloader domain.Downloader,
	importer domain.Importer,
	stateManager domain.StateManager,
	progressReporter domain.ProgressReporter,
	repository domain.KLineRepository,
) *Scheduler {
	return &Scheduler{
		config:           cfg,
		downloader:       downloader,
		importer:         importer,
		stateManager:     stateManager,
		progressReporter: progressReporter,
		repository:       repository,
		logger:           logger.GetLogger("scheduler"),
	}
}

// Run 运行调度器
func (s *Scheduler) Run(ctx context.Context) error {
	// 解析结束日期
	var endDate time.Time
	var err error
	if s.config.EndDate != "" {
		endDate, err = time.Parse("2006-01-02", s.config.EndDate)
		if err != nil {
			return fmt.Errorf("invalid end date: %w", err)
		}
	} else {
		// 默认使用昨天作为结束日期
		endDate = time.Now().AddDate(0, 0, -1)
	}

	s.logger.Info().
		Str("end_date", endDate.Format("2006-01-02")).
		Int("batch_days", s.config.BatchDays).
		Int("concurrent_symbols", s.config.MaxConcurrentSymbols).
		Msg("Starting scheduler")

	start := time.Now()
	defer func() {
		logger.LogPerformance("scheduler", "run", time.Since(start), map[string]interface{}{
			"end_date": endDate.Format("2006-01-02"),
		})
	}()

	// 获取可用的交易对
	symbols, err := s.getSymbols(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbols: %w", err)
	}

	s.logger.Info().
		Int("symbol_count", len(symbols)).
		Strs("symbols", symbols[:min(len(symbols), 10)]). // 只显示前10个
		Msg("Retrieved symbols")

	// 生成下载任务
	tasks, err := s.generateTasksWithEndDate(ctx, symbols, endDate)
	if err != nil {
		return fmt.Errorf("failed to generate tasks: %w", err)
	}

	s.logger.Info().
		Int("task_count", len(tasks)).
		Msg("Generated download tasks")

	// 启动进度报告器
	if s.progressReporter != nil {
		if err := s.progressReporter.Start(len(tasks)); err != nil {
			s.logger.Warn().Err(err).Msg("Failed to start progress reporter")
		}
	}

	// 按批次处理任务
	if err := s.processTasks(ctx, tasks); err != nil {
		return fmt.Errorf("failed to process tasks: %w", err)
	}

	// 创建物化视图
	if err := s.createMaterializedViews(ctx); err != nil {
		s.logger.Warn().Err(err).Msg("Failed to create materialized views")
	}

	s.logger.Info().
		Dur("total_duration", time.Since(start)).
		Msg("Scheduler completed successfully")

	return nil
}

// getSymbols 获取交易对列表
func (s *Scheduler) getSymbols(ctx context.Context) ([]string, error) {
	// 从下载器获取可用的交易对
	allSymbols, err := s.downloader.GetSymbols(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get symbols from downloader: %w", err)
	}

	s.logger.Debug().
		Int("total_symbols", len(allSymbols)).
		Msg("Retrieved all available symbols")

	return allSymbols, nil
}

// generateTasksWithEndDate 生成下载任务
func (s *Scheduler) generateTasksWithEndDate(ctx context.Context, symbols []string, endDate time.Time) ([]domain.DownloadTask, error) {
	var allTasks []domain.DownloadTask

	// 为每个交易对生成任务
	for _, symbol := range symbols {
		tasks, err := s.generateTasksForSymbolWithEndDate(ctx, symbol, endDate)
		if err != nil {
			s.logger.Warn().
				Err(err).
				Str("symbol", symbol).
				Msg("Failed to generate tasks for symbol")
			continue
		}
		allTasks = append(allTasks, tasks...)
	}

	// 按代币分组，然后按日期排序 - 串行处理每个代币
	sort.Slice(allTasks, func(i, j int) bool {
		// 首先按代币排序，确保同一代币的任务连续
		if allTasks[i].Symbol != allTasks[j].Symbol {
			return allTasks[i].Symbol < allTasks[j].Symbol
		}
		// 同一代币内按日期排序
		return allTasks[i].Date.Before(allTasks[j].Date)
	})

	return allTasks, nil
}

// generateTasksForSymbolWithEndDate 为指定交易对生成任务
func (s *Scheduler) generateTasksForSymbolWithEndDate(ctx context.Context, symbol string, endDate time.Time) ([]domain.DownloadTask, error) {
	var tasks []domain.DownloadTask

	s.logger.Info().
		Str("symbol", symbol).
		Msg("Fetching available monthly data from Binance")

	// 直接从Binance获取该代币的所有可用月份
	availableMonths, err := s.downloader.GetAvailableDates(ctx, symbol)
	if err != nil {
		s.logger.Error().
			Err(err).
			Str("symbol", symbol).
			Msg("Failed to fetch available months from Binance")
		return nil, fmt.Errorf("failed to get available months for %s: %w", symbol, err)
	}

	if len(availableMonths) == 0 {
		s.logger.Warn().
			Str("symbol", symbol).
			Msg("No monthly data available for symbol")
		return tasks, nil
	}

	// 获取状态信息，确定哪些月份已经处理过
	state, err := s.stateManager.GetState(symbol)
	if err != nil {
		s.logger.Debug().
			Err(err).
			Str("symbol", symbol).
			Msg("No existing state found")
		state = &domain.ProcessingState{
			Symbol:   symbol,
			LastDate: time.Time{},
		}
	}

	// 从数据库获取最后处理的日期
	lastProcessedDate := state.LastDate
	if lastProcessedDate.IsZero() {
		dbLastDate, err := s.repository.GetLastDate(ctx, symbol)
		if err == nil && !dbLastDate.IsZero() {
			lastProcessedDate = dbLastDate
		}
	}

	s.logger.Debug().
		Str("symbol", symbol).
		Str("last_processed_date", lastProcessedDate.Format("2006-01-02")).
		Int("available_months", len(availableMonths)).
		Msg("Processing available months")

	// 过滤出需要处理的月份
	for _, monthDate := range availableMonths {
		// 跳过未来的月份
		if monthDate.After(endDate) {
			continue
		}

		// 如果已经处理过这个月份，跳过
		if !lastProcessedDate.IsZero() {
			lastProcessedMonth := time.Date(lastProcessedDate.Year(), lastProcessedDate.Month(), 1, 0, 0, 0, 0, time.UTC)
			if monthDate.Before(lastProcessedMonth) || monthDate.Equal(lastProcessedMonth) {
				s.logger.Debug().
					Str("symbol", symbol).
					Str("month", monthDate.Format("2006-01")).
					Msg("Skipping already processed month")
				continue
			}
		}

		task := domain.DownloadTask{
			Symbol: symbol,
			Date:   monthDate,
		}
		tasks = append(tasks, task)

		s.logger.Debug().
			Str("symbol", symbol).
			Str("month", monthDate.Format("2006-01")).
			Msg("Added monthly task")
	}

	s.logger.Info().
		Str("symbol", symbol).
		Int("total_available", len(availableMonths)).
		Int("tasks_generated", len(tasks)).
		Msg("Generated monthly tasks based on Binance availability")

	return tasks, nil
}

// isDataAvailable 检查指定日期的数据是否可用
func (s *Scheduler) isDataAvailable(ctx context.Context, symbol string, date time.Time) (bool, error) {
	// 检查日期是否太新（币安可能还没有数据）
	if date.After(time.Now().AddDate(0, 0, -1)) {
		return false, nil
	}

	// 可以通过检查URL是否可访问来验证数据可用性
	// 这里简化处理，假设历史数据都可用
	return true, nil
}

// processTasks 处理任务
func (s *Scheduler) processTasks(ctx context.Context, tasks []domain.DownloadTask) error {
	if len(tasks) == 0 {
		s.logger.Info().Msg("No tasks to process")
		return nil
	}

	// 按代币分组处理 - 串行处理每个代币
	symbolTasks := make(map[string][]domain.DownloadTask)
	for _, task := range tasks {
		symbolTasks[task.Symbol] = append(symbolTasks[task.Symbol], task)
	}

	// 获取所有代币并排序，确保处理顺序一致
	var symbols []string
	for symbol := range symbolTasks {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols)

	s.logger.Info().
		Int("total_tasks", len(tasks)).
		Int("symbol_count", len(symbols)).
		Strs("symbols", symbols).
		Msg("Processing tasks by symbol (sequential)")

	// 串行处理每个代币
	for i, symbol := range symbols {
		symbolTaskList := symbolTasks[symbol]
		
		s.logger.Info().
			Int("symbol_index", i+1).
			Int("total_symbols", len(symbols)).
			Str("symbol", symbol).
			Int("symbol_tasks", len(symbolTaskList)).
			Msg("Starting symbol processing")

		// 处理当前代币的所有任务
		if err := s.importer.ImportData(ctx, symbolTaskList); err != nil {
			return fmt.Errorf("failed to import data for symbol %s: %w", symbol, err)
		}

		s.logger.Info().
			Str("symbol", symbol).
			Int("completed_tasks", len(symbolTaskList)).
			Msg("Symbol processing completed")

		// 检查上下文是否被取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 代币间的短暂休息
		if i+1 < len(symbols) {
			time.Sleep(2 * time.Second)
			s.logger.Info().
				Str("next_symbol", symbols[i+1]).
				Msg("Moving to next symbol")
		}
	}

	s.logger.Info().
		Int("total_symbols_processed", len(symbols)).
		Int("total_tasks_processed", len(tasks)).
		Msg("All symbols processing completed")

	return nil
}

// createMaterializedViews 创建物化视图
func (s *Scheduler) createMaterializedViews(ctx context.Context) error {
	s.logger.Info().Msg("Creating materialized views")

	// 定义要创建的时间间隔
	intervals := []string{"5m", "15m", "1h", "4h", "1d"}

	if err := s.repository.CreateMaterializedViews(ctx, intervals); err != nil {
		return fmt.Errorf("failed to create materialized views: %w", err)
	}

	// 刷新物化视图
	if err := s.repository.RefreshMaterializedViews(ctx); err != nil {
		s.logger.Warn().Err(err).Msg("Failed to refresh materialized views")
	}

	s.logger.Info().Msg("Materialized views created successfully")
	return nil
}

// ValidateData 验证数据完整性
func (s *Scheduler) ValidateData(ctx context.Context, symbols []string, endDate time.Time) error {
	s.logger.Info().
		Int("symbol_count", len(symbols)).
		Str("end_date", endDate.Format("2006-01-02")).
		Msg("Starting data validation")

	var totalErrors, totalWarnings int

	for _, symbol := range symbols {
		// 获取该代币在数据库中的最早日期
		firstDate, err := s.repository.GetFirstDate(ctx, symbol)
		if err != nil {
			s.logger.Warn().
				Err(err).
				Str("symbol", symbol).
				Msg("Failed to get first date for symbol, skipping")
			continue
		}
		if firstDate.IsZero() {
			s.logger.Info().
				Str("symbol", symbol).
				Msg("No data found for symbol, skipping validation")
			continue
		}

		currentDate := firstDate
		for currentDate.Before(endDate) || currentDate.Equal(endDate) {
			result, err := s.repository.ValidateData(ctx, symbol, currentDate)
			if err != nil {
				s.logger.Error().
					Err(err).
					Str("symbol", symbol).
					Str("date", currentDate.Format("2006-01-02")).
					Msg("Failed to validate data")
				currentDate = currentDate.AddDate(0, 0, 1)
				continue
			}

			if !result.Valid {
				totalErrors += len(result.Errors)
				s.logger.Warn().
					Str("symbol", symbol).
					Str("date", currentDate.Format("2006-01-02")).
					Int("total_rows", result.TotalRows).
					Int("invalid_rows", result.InvalidRows).
					Strs("errors", result.Errors).
					Msg("Data validation failed")
			}

			if len(result.Warnings) > 0 {
				totalWarnings += len(result.Warnings)
				s.logger.Debug().
					Str("symbol", symbol).
					Str("date", currentDate.Format("2006-01-02")).
					Strs("warnings", result.Warnings).
					Msg("Data validation warnings")
			}

			currentDate = currentDate.AddDate(0, 0, 1)
		}
	}

	s.logger.Info().
		Int("total_errors", totalErrors).
		Int("total_warnings", totalWarnings).
		Msg("Data validation completed")

	return nil
}

// GetProgress 获取处理进度
func (s *Scheduler) GetProgress() map[string]interface{} {
	if s.progressReporter != nil {
		return s.progressReporter.GetOverallProgress()
	}
	return map[string]interface{}{
		"message": "Progress reporting not enabled",
	}
}

// Stop 停止调度器
func (s *Scheduler) Stop(ctx context.Context) error {
	s.logger.Info().Msg("Stopping scheduler")

	// 停止进度报告器
	if s.progressReporter != nil {
		if err := s.progressReporter.Stop(ctx); err != nil {
			s.logger.Warn().Err(err).Msg("Failed to stop progress reporter")
		}
	}

	// 关闭导入器
	if s.importer != nil {
		if err := s.importer.Close(); err != nil {
			s.logger.Warn().Err(err).Msg("Failed to close importer")
		}
	}

	s.logger.Info().Msg("Scheduler stopped")
	return nil
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}