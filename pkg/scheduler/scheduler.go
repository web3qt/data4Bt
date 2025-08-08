package scheduler

import (
	"context"
	"fmt"
	"sort"
	"sync"
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
	// 从Binance获取所有USDT交易对
	allSymbols, err := s.downloader.GetSymbols(ctx)
	if err != nil {
		// 如果获取失败，使用备用的硬编码列表
		s.logger.Warn().
			Err(err).
			Msg("Failed to get symbols from Binance, using fallback list")
		
		hardcodedSymbols := []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"}
		return hardcodedSymbols, nil
	}
	
	s.logger.Info().
		Int("total_symbols", len(allSymbols)).
		Msg("Retrieved all available symbols from Binance")
	
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

	// 对可用月份进行排序，确保按时间顺序处理
	sort.Slice(availableMonths, func(i, j int) bool {
		return availableMonths[i].Before(availableMonths[j])
	})

	s.logger.Info().
		Str("symbol", symbol).
		Int("available_months", len(availableMonths)).
		Str("earliest_month", availableMonths[0].Format("2006-01")).
		Str("latest_month", availableMonths[len(availableMonths)-1].Format("2006-01")).
		Msg("Processing all available months in chronological order")

	// 获取该代币的处理状态
	processingState, err := s.stateManager.GetState(symbol)
	if err != nil {
		s.logger.Error().
			Err(err).
			Str("symbol", symbol).
			Msg("Failed to get processing state")
		return nil, fmt.Errorf("failed to get processing state for %s: %w", symbol, err)
	}

	// 生成所有可用月份的任务（按时间顺序）
	for _, monthDate := range availableMonths {
		// 跳过未来的月份和当前月份（当前月份数据可能不完整）
		currentMonth := time.Now().UTC().Truncate(24 * time.Hour)
		currentMonth = time.Date(currentMonth.Year(), currentMonth.Month(), 1, 0, 0, 0, 0, time.UTC)
		
		if monthDate.After(endDate) || !monthDate.Before(currentMonth) {
			s.logger.Debug().
				Str("symbol", symbol).
				Str("month", monthDate.Format("2006-01")).
				Msg("Skipping future or current month")
			continue
		}

		// 检查该月份是否已经处理过
		// 如果LastDate不为零值且大于等于当前月份的最后一天，则认为该月份已处理
		if !processingState.LastDate.IsZero() {
			// 计算当前月份的最后一天
			nextMonth := monthDate.AddDate(0, 1, 0)
			lastDayOfMonth := nextMonth.Add(-24 * time.Hour)
			
			if processingState.LastDate.After(lastDayOfMonth) || processingState.LastDate.Equal(lastDayOfMonth) {
				s.logger.Debug().
					Str("symbol", symbol).
					Str("month", monthDate.Format("2006-01")).
					Str("last_processed", processingState.LastDate.Format("2006-01-02")).
					Msg("Month already processed, skipping")
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
		Msg("Generated all monthly tasks in chronological order")

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

	// 对每个代币的任务按时间排序
	for symbol := range symbolTasks {
		sort.Slice(symbolTasks[symbol], func(i, j int) bool {
			return symbolTasks[symbol][i].Date.Before(symbolTasks[symbol][j].Date)
		})
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
		Msg("Processing tasks by symbol (sequential, chronological order)")

	// 串行处理每个代币
	for i, symbol := range symbols {
		symbolTaskList := symbolTasks[symbol]
		
		s.logger.Info().
			Int("symbol_index", i+1).
			Int("total_symbols", len(symbols)).
			Str("symbol", symbol).
			Int("symbol_tasks", len(symbolTaskList)).
			Str("earliest_month", symbolTaskList[0].Date.Format("2006-01")).
			Str("latest_month", symbolTaskList[len(symbolTaskList)-1].Date.Format("2006-01")).
			Msg("Starting symbol processing in chronological order")

		// 处理当前代币的所有任务（按时间顺序）
		if err := s.importer.ImportData(ctx, symbolTaskList); err != nil {
			return fmt.Errorf("failed to import data for symbol %s: %w", symbol, err)
		}

		s.logger.Info().
			Str("symbol", symbol).
			Int("completed_tasks", len(symbolTaskList)).
			Msg("Symbol processing completed successfully")

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

// RunConcurrent 并发运行调度器，最大并发数为5
func (s *Scheduler) RunConcurrent(ctx context.Context) error {
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
		Int("max_concurrent_symbols", 5).
		Msg("Starting concurrent scheduler")

	start := time.Now()
	defer func() {
		logger.LogPerformance("scheduler", "run_concurrent", time.Since(start), map[string]interface{}{
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

	// 并发处理任务
	if err := s.processTasksConcurrent(ctx, tasks); err != nil {
		return fmt.Errorf("failed to process tasks concurrently: %w", err)
	}

	// 创建物化视图
	if err := s.createMaterializedViews(ctx); err != nil {
		s.logger.Warn().Err(err).Msg("Failed to create materialized views")
	}

	s.logger.Info().
		Dur("total_duration", time.Since(start)).
		Msg("Concurrent scheduler completed successfully")

	return nil
}

// processTasksConcurrent 并发处理任务，最大并发数为5
func (s *Scheduler) processTasksConcurrent(ctx context.Context, tasks []domain.DownloadTask) error {
	if len(tasks) == 0 {
		s.logger.Info().Msg("No tasks to process")
		return nil
	}

	// 按代币分组处理
	symbolTasks := make(map[string][]domain.DownloadTask)
	for _, task := range tasks {
		symbolTasks[task.Symbol] = append(symbolTasks[task.Symbol], task)
	}

	// 对每个代币的任务按时间排序
	for symbol := range symbolTasks {
		sort.Slice(symbolTasks[symbol], func(i, j int) bool {
			return symbolTasks[symbol][i].Date.Before(symbolTasks[symbol][j].Date)
		})
	}

	// 获取所有代币并排序
	var symbols []string
	for symbol := range symbolTasks {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols)

	s.logger.Info().
		Int("total_tasks", len(tasks)).
		Int("symbol_count", len(symbols)).
		Msg("Processing tasks concurrently with max 5 workers")

	// 使用信号量控制并发数
	semaphore := make(chan struct{}, 5)
	errChan := make(chan error, len(symbols))
	var wg sync.WaitGroup

	// 并发处理每个代币
	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			
			// 获取信号量
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			
			symbolTaskList := symbolTasks[sym]
			
			s.logger.Info().
				Str("symbol", sym).
				Int("symbol_tasks", len(symbolTaskList)).
				Str("earliest_month", symbolTaskList[0].Date.Format("2006-01")).
				Str("latest_month", symbolTaskList[len(symbolTaskList)-1].Date.Format("2006-01")).
				Msg("Starting concurrent symbol processing")
			
			// 处理当前代币的所有任务
			if err := s.importer.ImportData(ctx, symbolTaskList); err != nil {
				errChan <- fmt.Errorf("failed to import data for symbol %s: %w", sym, err)
				return
			}
			
			s.logger.Info().
				Str("symbol", sym).
				Int("completed_tasks", len(symbolTaskList)).
				Msg("Concurrent symbol processing completed")
		}(symbol)
	}

	// 等待所有任务完成
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// 检查错误
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	s.logger.Info().
		Int("total_symbols_processed", len(symbols)).
		Int("total_tasks_processed", len(tasks)).
		Msg("All concurrent tasks processing completed")

	return nil
}

// UpdateToLatest 更新所有币种到最新数据
func (s *Scheduler) UpdateToLatest(ctx context.Context) error {
	s.logger.Info().Msg("Starting update to latest data")

	start := time.Now()
	defer func() {
		logger.LogPerformance("scheduler", "update_to_latest", time.Since(start), nil)
	}()

	// 获取所有币种的当前状态
	allStates, err := s.stateManager.GetAllStates()
	if err != nil {
		return fmt.Errorf("failed to get all states: %w", err)
	}

	// 获取所有可用的交易对
	symbols, err := s.getSymbols(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbols: %w", err)
	}

	// 使用当前时间作为结束日期
	endDate := time.Now().AddDate(0, 0, -1) // 昨天

	var updateTasks []domain.DownloadTask

	// 为每个币种生成更新任务
	for _, symbol := range symbols {
		state, exists := allStates[symbol]
		var startDate time.Time

		if exists && !state.LastDate.IsZero() {
			// 从上次处理的日期的下一个月开始
			startDate = state.LastDate.AddDate(0, 1, 0)
		} else {
			// 如果没有状态记录，从币安最早可用数据开始
			availableDates, err := s.downloader.GetAvailableDates(ctx, symbol)
			if err != nil {
				s.logger.Warn().Err(err).Str("symbol", symbol).Msg("Failed to get available dates")
				continue
			}
			if len(availableDates) == 0 {
				continue
			}
			startDate = availableDates[0]
		}

		// 生成从startDate到endDate的任务
		for current := startDate; current.Before(endDate) || current.Equal(endDate); current = current.AddDate(0, 1, 0) {
			// 检查这个月份的数据是否在币安可用
			availableDates, err := s.downloader.GetAvailableDates(ctx, symbol)
			if err != nil {
				continue
			}

			// 检查当前月份是否在可用日期列表中
			found := false
			for _, availableDate := range availableDates {
				if availableDate.Year() == current.Year() && availableDate.Month() == current.Month() {
					found = true
					break
				}
			}

			if found {
				updateTasks = append(updateTasks, domain.DownloadTask{
					Symbol: symbol,
					Date:   current,
				})
			}
		}
	}

	if len(updateTasks) == 0 {
		s.logger.Info().Msg("No update tasks needed, all symbols are up to date")
		return nil
	}

	s.logger.Info().
		Int("update_tasks", len(updateTasks)).
		Msg("Generated update tasks")

	// 启动进度报告器
	if s.progressReporter != nil {
		if err := s.progressReporter.Start(len(updateTasks)); err != nil {
			s.logger.Warn().Err(err).Msg("Failed to start progress reporter")
		}
	}

	// 并发处理更新任务
	if err := s.processTasksConcurrent(ctx, updateTasks); err != nil {
		return fmt.Errorf("failed to process update tasks: %w", err)
	}

	s.logger.Info().
		Dur("total_duration", time.Since(start)).
		Msg("Update to latest completed successfully")

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