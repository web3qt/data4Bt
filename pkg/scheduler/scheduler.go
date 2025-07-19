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

	// 按日期排序任务
	sort.Slice(allTasks, func(i, j int) bool {
		if allTasks[i].Date.Equal(allTasks[j].Date) {
			return allTasks[i].Symbol < allTasks[j].Symbol
		}
		return allTasks[i].Date.Before(allTasks[j].Date)
	})

	return allTasks, nil
}

// generateTasksForSymbolWithEndDate 为指定交易对生成任务
func (s *Scheduler) generateTasksForSymbolWithEndDate(ctx context.Context, symbol string, endDate time.Time) ([]domain.DownloadTask, error) {
	var tasks []domain.DownloadTask

	// 获取状态信息，确定起始日期
	state, err := s.stateManager.GetState(symbol)
	if err != nil {
		s.logger.Debug().
			Err(err).
			Str("symbol", symbol).
			Msg("No existing state found, will check database for last date")
		state = &domain.ProcessingState{
			Symbol:   symbol,
			LastDate: time.Time{}, // 零值表示需要从数据库查询
		}
	}

	s.logger.Debug().
		Str("symbol", symbol).
		Str("state_last_date", state.LastDate.Format("2006-01-02")).
		Bool("state_is_zero", state.LastDate.IsZero()).
		Msg("Retrieved state")

	// 确定起始日期：如果状态中没有记录，从数据库获取最后日期
	var startDate time.Time
	if state.LastDate.IsZero() {
		// 从数据库获取该代币的最后日期
		lastDate, err := s.repository.GetLastDate(ctx, symbol)
		s.logger.Debug().
			Str("symbol", symbol).
			Str("db_last_date", lastDate.Format("2006-01-02")).
			Bool("db_last_date_is_zero", lastDate.IsZero()).
			Msg("Retrieved last date from database")
		if err != nil {
			s.logger.Debug().
				Err(err).
				Str("symbol", symbol).
				Msg("Failed to get last date from database, will start from earliest available")
			// 如果数据库中没有数据，从最近7天开始
			startDate = time.Now().AddDate(0, 0, -7)
		} else if lastDate.IsZero() {
			// 数据库中没有该代币的数据，从最近7天开始
			startDate = time.Now().AddDate(0, 0, -7)
			s.logger.Debug().
				Str("symbol", symbol).
				Str("calculated_start_date", startDate.Format("2006-01-02")).
				Msg("Set start date to 7 days ago (no data in DB)")
		} else {
			// 从数据库中的最后日期的下一天开始
			startDate = lastDate.AddDate(0, 0, 1)
			s.logger.Debug().
				Str("symbol", symbol).
				Str("calculated_start_date", startDate.Format("2006-01-02")).
				Msg("Set start date to day after last DB date")
		}
	} else {
		// 从状态中的最后日期的下一天开始
		startDate = state.LastDate.AddDate(0, 0, 1)
		s.logger.Debug().
			Str("symbol", symbol).
			Str("calculated_start_date", startDate.Format("2006-01-02")).
			Msg("Set start date to day after state date")
	}

	s.logger.Debug().
		Str("symbol", symbol).
		Str("start_date", startDate.Format("2006-01-02")).
		Str("end_date", endDate.Format("2006-01-02")).
		Msg("Date range determined")

	// 生成日期范围内的任务
	currentDate := startDate
	for currentDate.Before(endDate) || currentDate.Equal(endDate) {
		// 检查是否已经处理过
		dateStr := currentDate.Format("2006-01-02")
		// 如果当前日期小于等于最后处理日期，则跳过
		if state.LastDate.IsZero() || currentDate.After(state.LastDate) {
			// 检查数据是否可用
			available, err := s.isDataAvailable(ctx, symbol, currentDate)
			if err != nil {
				s.logger.Debug().
					Err(err).
					Str("symbol", symbol).
					Str("date", dateStr).
					Msg("Failed to check data availability")
			} else if available {
				task := domain.DownloadTask{
					Symbol: symbol,
					Date:   currentDate,
				}
				s.logger.Debug().
					Str("symbol", task.Symbol).
					Str("date", task.Date.Format("2006-01-02")).
					Msg("Generated task")
				tasks = append(tasks, task)
			}
		}

		currentDate = currentDate.AddDate(0, 0, 1)
	}

	s.logger.Debug().
		Str("symbol", symbol).
		Int("task_count", len(tasks)).
		Str("start_date", startDate.Format("2006-01-02")).
		Msg("Generated tasks for symbol")

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

	// 按批次处理
	batchSize := s.config.BatchDays * s.config.MaxConcurrentSymbols
	if batchSize <= 0 {
		batchSize = 100 // 默认批次大小
	}

	s.logger.Info().
		Int("total_tasks", len(tasks)).
		Int("batch_size", batchSize).
		Msg("Processing tasks in batches")

	for i := 0; i < len(tasks); i += batchSize {
		end := i + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}

		batch := tasks[i:end]
		s.logger.Info().
			Int("batch_start", i+1).
			Int("batch_end", end).
			Int("batch_size", len(batch)).
			Msg("Processing batch")

		if err := s.importer.ImportData(ctx, batch); err != nil {
			return fmt.Errorf("failed to import batch %d-%d: %w", i+1, end, err)
		}

		// 检查上下文是否被取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 批次间的短暂休息
		if i+batchSize < len(tasks) {
			time.Sleep(1 * time.Second)
		}
	}

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