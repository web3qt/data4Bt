package windowimport

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
	"binance-data-loader/internal/state"
	"binance-data-loader/pkg/aggtradeparser"
	"binance-data-loader/pkg/binance"
	"binance-data-loader/pkg/clickhouse"
	"binance-data-loader/pkg/importer"
	"binance-data-loader/pkg/monitor"
)

const defaultLatestWindowDays = 15
const defaultLiquidityRankingDays = 7
const defaultTopLiquiditySymbols = 30

func BuildDailyArchiveURL(baseURL, dataPath, symbol string, date time.Time) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	dataPath = strings.TrimSpace(dataPath)
	if !strings.HasPrefix(dataPath, "/") {
		dataPath = "/" + dataPath
	}
	dataPath = strings.TrimRight(dataPath, "/")

	dateStr := date.UTC().Format("2006-01-02")
	filename := fmt.Sprintf("%s-aggTrades-%s.zip", symbol, dateStr)
	return fmt.Sprintf("%s%s/%s/%s", baseURL, dataPath, symbol, filename)
}

func BuildLatestWindowTasks(symbols []string, windowDays int, now time.Time, baseURL, dataPath string) []domain.ConcurrentTask {
	if windowDays <= 0 || len(symbols) == 0 {
		return nil
	}

	startDay, endDay := BuildBackfillWindow(now, windowDays)
	return BuildMissingWindowTasks(symbols, startDay, endDay, baseURL, dataPath, nil)
}

func BuildRankingWindow(latestAvailableDay time.Time, windowDays int) (time.Time, time.Time) {
	if windowDays <= 0 {
		windowDays = 1
	}

	endDay := truncateToUTCDay(latestAvailableDay)
	startDay := endDay.AddDate(0, 0, -(windowDays - 1))
	return startDay, endDay
}

func BuildBackfillWindow(now time.Time, windowDays int) (time.Time, time.Time) {
	if windowDays <= 0 {
		windowDays = 1
	}

	nowUTC := now.UTC()
	endDay := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	startDay := endDay.AddDate(0, 0, -(windowDays - 1))
	return startDay, endDay
}

func BuildMissingWindowTasks(symbols []string, startDay, endDay time.Time, baseURL, dataPath string, existingRanges map[string]*domain.SymbolDateRange) []domain.ConcurrentTask {
	if len(symbols) == 0 || startDay.After(endDay) {
		return nil
	}

	sortedSymbols := append([]string(nil), symbols...)
	sort.Strings(sortedSymbols)

	concurrentTasks := make([]domain.ConcurrentTask, 0, len(sortedSymbols))
	for _, symbol := range sortedSymbols {
		tasks := make([]domain.DownloadTask, 0)
		existingRange := existingRanges[symbol]

		for current := truncateToUTCDay(startDay); !current.After(endDay); current = current.AddDate(0, 0, 1) {
			if isCoveredByExistingRange(current, existingRange) {
				continue
			}

			tasks = append(tasks, domain.DownloadTask{
				Symbol:   symbol,
				Date:     current,
				Interval: "1s",
				URL:      BuildDailyArchiveURL(baseURL, dataPath, symbol, current),
			})
		}

		if len(tasks) == 0 {
			continue
		}

		concurrentTasks = append(concurrentTasks, domain.ConcurrentTask{
			Symbol:   symbol,
			Tasks:    tasks,
			Priority: 0,
		})
	}

	return concurrentTasks
}

func BuildMissingWindowTasksWithCoveredDays(symbols []string, startDay, endDay time.Time, baseURL, dataPath string, coveredDays map[string]map[string]struct{}, exactCoverageSymbols map[string]struct{}) []domain.ConcurrentTask {
	if len(symbols) == 0 || startDay.After(endDay) {
		return nil
	}

	concurrentTasks := make([]domain.ConcurrentTask, 0, len(symbols))
	for _, symbol := range symbols {
		tasks := make([]domain.DownloadTask, 0)
		symbolCoveredDays := coveredDays[symbol]
		_, exactCoverage := exactCoverageSymbols[symbol]

		for current := truncateToUTCDay(startDay); !current.After(endDay); current = current.AddDate(0, 0, 1) {
			if _, exists := symbolCoveredDays[current.Format("2006-01-02")]; exists && (exactCoverage || !isWindowBoundaryDay(current, startDay, endDay)) {
				continue
			}

			tasks = append(tasks, domain.DownloadTask{
				Symbol:   symbol,
				Date:     current,
				Interval: "1s",
				URL:      BuildDailyArchiveURL(baseURL, dataPath, symbol, current),
			})
		}

		if len(tasks) == 0 {
			continue
		}

		concurrentTasks = append(concurrentTasks, domain.ConcurrentTask{
			Symbol:   symbol,
			Tasks:    tasks,
			Priority: 0,
		})
	}

	return concurrentTasks
}

func MergeWindowCoveredDays(symbols []string, startDay, endDay time.Time, states map[string]*domain.ProcessingState, dbCoveredDays map[string]map[string]struct{}) (map[string]map[string]struct{}, map[string]struct{}) {
	result := make(map[string]map[string]struct{}, len(symbols))
	exactCoverageSymbols := make(map[string]struct{})

	for _, symbol := range symbols {
		if state, exists := states[symbol]; exists {
			if len(state.CompletedTaskDates) > 0 {
				result[symbol] = filterCompletedTaskDatesForWindow(state.CompletedTaskDates, startDay, endDay)
				exactCoverageSymbols[symbol] = struct{}{}
				continue
			}

			// Legacy or partially-seeded state without exact completed dates is not trustworthy.
			result[symbol] = make(map[string]struct{})
			continue
		}

		result[symbol] = copyCoveredDays(dbCoveredDays[symbol])
	}

	return result, exactCoverageSymbols
}

func RunFuturesUM1sLatest15d(ctx context.Context, cfg *config.Config) error {
	windowDays := resolveWindowDays(cfg)
	log := logger.GetLogger("windowimport_futures_um_1s")
	log.Info().Int("window_days", windowDays).Msg("Starting futures UM 1s latest window import")

	downloader := binance.NewBinanceDownloader(cfg.Binance, cfg.Downloader)
	csvParser := aggtradeparser.NewAggTradeCSVParser()

	repository, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to create repository: %w", err)
	}
	defer repository.Close()

	if err := repository.CreateTables(ctx); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	stateManager, err := state.NewFileStateManager(cfg.State)
	if err != nil {
		return fmt.Errorf("failed to create state manager: %w", err)
	}

	var progressReporter domain.ProgressReporter
	if cfg.Monitoring.Enabled {
		progressReporter = monitor.NewProgressReporter(cfg.Monitoring)
	}

	dataImporter := importer.NewImporter(
		cfg.Importer,
		downloader,
		csvParser,
		repository,
		stateManager,
		progressReporter,
	)
	defer dataImporter.Close()

	symbols, err := downloader.GetSymbols(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbols: %w", err)
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no futures UM symbols returned from Binance")
	}

	symbolProgress, err := stateManager.GetAllSymbolProgress()
	if err != nil {
		return fmt.Errorf("failed to load symbol progress: %w", err)
	}

	remainingSymbols := filterRemainingSymbols(symbols, symbolProgress)
	if len(remainingSymbols) == 0 {
		log.Info().Int("symbol_count", len(symbols)).Msg("All futures UM symbols already completed for the current task scope")
		return nil
	}

	concurrentTasks := BuildLatestWindowTasks(
		remainingSymbols,
		windowDays,
		time.Now().UTC(),
		cfg.Binance.BaseURL,
		cfg.Binance.DataPath,
	)
	if len(concurrentTasks) == 0 {
		return fmt.Errorf("no daily tasks generated")
	}

	totalTasks := 0
	for _, concurrentTask := range concurrentTasks {
		totalTasks += len(concurrentTask.Tasks)
		if err := seedSymbolState(stateManager, concurrentTask); err != nil {
			return fmt.Errorf("failed to seed state for %s: %w", concurrentTask.Symbol, err)
		}
	}

	if progressReporter != nil {
		if err := progressReporter.Start(totalTasks); err != nil {
			log.Warn().Err(err).Msg("Failed to start progress reporter")
		}
		defer progressReporter.Stop(context.Background())
	}

	log.Info().
		Int("total_symbols", len(symbols)).
		Int("remaining_symbols", len(concurrentTasks)).
		Int("skipped_completed_symbols", len(symbols)-len(remainingSymbols)).
		Int("task_count", totalTasks).
		Msg("Dispatching futures UM 1s daily tasks")

	if err := dataImporter.ImportDataConcurrent(ctx, concurrentTasks); err != nil {
		return fmt.Errorf("futures UM 1s import failed: %w", err)
	}

	log.Info().
		Int("remaining_symbols", len(concurrentTasks)).
		Int("task_count", totalTasks).
		Msg("Futures UM 1s latest window import completed")

	return nil
}

func RunFuturesUM1sTop30Recent90d(ctx context.Context, cfg *config.Config) error {
	windowDays := resolveWindowDays(cfg)
	log := logger.GetLogger("windowimport_futures_um_1s_top30_window")
	log.Info().
		Int("window_days", windowDays).
		Int("ranking_days", defaultLiquidityRankingDays).
		Int("top_symbols", defaultTopLiquiditySymbols).
		Msg("Starting futures UM 1s top30 recent window import")

	downloader := binance.NewBinanceDownloader(cfg.Binance, cfg.Downloader)
	csvParser := aggtradeparser.NewAggTradeCSVParser()

	repository, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to create repository: %w", err)
	}
	defer repository.Close()

	if err := repository.CreateTables(ctx); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	stateManager, err := state.NewFileStateManager(cfg.State)
	if err != nil {
		return fmt.Errorf("failed to create state manager: %w", err)
	}

	var progressReporter domain.ProgressReporter
	if cfg.Monitoring.Enabled {
		progressReporter = monitor.NewProgressReporter(cfg.Monitoring)
	}

	dataImporter := importer.NewImporter(
		cfg.Importer,
		downloader,
		csvParser,
		repository,
		stateManager,
		progressReporter,
	)
	defer dataImporter.Close()

	baseTable := resolveBaseTableName(cfg)
	rankingStartDay := time.Time{}
	rankingEndDay := time.Time{}
	topSymbols := ResolveConfiguredSymbols(cfg)
	if len(topSymbols) == 0 {
		latestAvailableDay, err := queryLatestAvailableDay(ctx, repository, baseTable)
		if err != nil {
			return fmt.Errorf("failed to resolve latest available day: %w", err)
		}

		rankingStartDay, rankingEndDay = BuildRankingWindow(latestAvailableDay, defaultLiquidityRankingDays)
		topSymbols, err = queryTopLiquiditySymbols(ctx, repository, baseTable, rankingStartDay, rankingEndDay, defaultTopLiquiditySymbols)
		if err != nil {
			return fmt.Errorf("failed to query top liquidity symbols: %w", err)
		}
		if len(topSymbols) == 0 {
			return fmt.Errorf("no top liquidity symbols found for ranking window %s -> %s", rankingStartDay.Format("2006-01-02"), rankingEndDay.Format("2006-01-02"))
		}
	}

	symbolProgress, err := stateManager.GetAllSymbolProgress()
	if err != nil {
		return fmt.Errorf("failed to load symbol progress: %w", err)
	}

	remainingSymbols := filterRemainingSymbols(topSymbols, symbolProgress)
	if len(remainingSymbols) == 0 {
		log.Info().Int("selected_symbols", len(topSymbols)).Msg("All top liquidity symbols already completed for the current task scope")
		return nil
	}

	backfillStartDay, backfillEndDay, err := ResolveWindowRange(cfg, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to resolve backfill window: %w", err)
	}
	existingCoveredDays, err := repository.GetExistingDailyCoverage(ctx, remainingSymbols, backfillStartDay, backfillEndDay)
	if err != nil {
		return fmt.Errorf("failed to query existing daily coverage: %w", err)
	}
	states, err := stateManager.GetAllStates()
	if err != nil {
		return fmt.Errorf("failed to load processing states: %w", err)
	}
	coveredDays, exactCoverageSymbols := MergeWindowCoveredDays(remainingSymbols, backfillStartDay, backfillEndDay, states, existingCoveredDays)

	concurrentTasks := BuildMissingWindowTasksWithCoveredDays(
		remainingSymbols,
		backfillStartDay,
		backfillEndDay,
		cfg.Binance.BaseURL,
		cfg.Binance.DataPath,
		coveredDays,
		exactCoverageSymbols,
	)

	selectedWithoutTasks := findSymbolsWithoutTasks(remainingSymbols, concurrentTasks)
	if err := markTasklessSymbolsCompleted(stateManager, selectedWithoutTasks, backfillStartDay, backfillEndDay); err != nil {
		return fmt.Errorf("failed to mark taskless symbols completed: %w", err)
	}

	totalTasks := 0
	for _, concurrentTask := range concurrentTasks {
		totalTasks += len(concurrentTask.Tasks)
		if err := seedSymbolState(stateManager, concurrentTask); err != nil {
			return fmt.Errorf("failed to seed state for %s: %w", concurrentTask.Symbol, err)
		}
	}

	if progressReporter != nil {
		if err := progressReporter.Start(totalTasks); err != nil {
			log.Warn().Err(err).Msg("Failed to start progress reporter")
		}
		defer progressReporter.Stop(context.Background())
	}

	event := log.Info().
		Str("backfill_window_start", backfillStartDay.Format("2006-01-02")).
		Str("backfill_window_end", backfillEndDay.Format("2006-01-02")).
		Int("selected_symbols", len(topSymbols)).
		Int("remaining_symbols", len(remainingSymbols)).
		Int("already_covered_symbols", len(selectedWithoutTasks)).
		Int("task_count", totalTasks)
	if !rankingStartDay.IsZero() {
		event = event.
			Str("selection_mode", "liquidity_top30").
			Str("ranking_window_start", rankingStartDay.Format("2006-01-02")).
			Str("ranking_window_end", rankingEndDay.Format("2006-01-02"))
	} else {
		event = event.Str("selection_mode", "explicit_symbols")
	}
	event.Msg("Dispatching futures UM 1s top30 recent window tasks")

	if len(concurrentTasks) == 0 {
		log.Info().Msg("No missing daily tasks generated for top30 recent window workflow")
		return nil
	}

	if err := dataImporter.ImportDataConcurrent(ctx, concurrentTasks); err != nil {
		return fmt.Errorf("futures UM 1s top30 recent window import failed: %w", err)
	}

	log.Info().
		Int("selected_symbols", len(topSymbols)).
		Int("task_count", totalTasks).
		Msg("Futures UM 1s top30 recent window import completed")

	return nil
}

func resolveWindowDays(cfg *config.Config) int {
	if cfg != nil && cfg.Scheduler.BatchDays > 0 {
		return cfg.Scheduler.BatchDays
	}
	return defaultLatestWindowDays
}

func ResolveWindowRange(cfg *config.Config, now time.Time) (time.Time, time.Time, error) {
	if cfg != nil {
		startRaw := strings.TrimSpace(cfg.Scheduler.StartDate)
		endRaw := strings.TrimSpace(cfg.Scheduler.EndDate)
		if startRaw != "" || endRaw != "" {
			if startRaw == "" || endRaw == "" {
				return time.Time{}, time.Time{}, fmt.Errorf("scheduler.start_date and scheduler.end_date must be set together")
			}

			startDay, err := time.Parse("2006-01-02", startRaw)
			if err != nil {
				return time.Time{}, time.Time{}, fmt.Errorf("failed to parse scheduler.start_date: %w", err)
			}
			endDay, err := time.Parse("2006-01-02", endRaw)
			if err != nil {
				return time.Time{}, time.Time{}, fmt.Errorf("failed to parse scheduler.end_date: %w", err)
			}
			startDay = truncateToUTCDay(startDay)
			endDay = truncateToUTCDay(endDay)
			if startDay.After(endDay) {
				return time.Time{}, time.Time{}, fmt.Errorf("scheduler.start_date must be on or before scheduler.end_date")
			}
			return startDay, endDay, nil
		}
	}

	windowDays := resolveWindowDays(cfg)
	startDay, endDay := BuildBackfillWindow(now, windowDays)
	return startDay, endDay, nil
}

func ResolveConfiguredSymbols(cfg *config.Config) []string {
	if cfg == nil || len(cfg.Binance.ExplicitSymbols) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(cfg.Binance.ExplicitSymbols))
	result := make([]string, 0, len(cfg.Binance.ExplicitSymbols))
	for _, raw := range cfg.Binance.ExplicitSymbols {
		symbol := strings.ToUpper(strings.TrimSpace(raw))
		if symbol == "" {
			continue
		}
		if _, exists := seen[symbol]; exists {
			continue
		}
		seen[symbol] = struct{}{}
		result = append(result, symbol)
	}

	return result
}

func resolveBaseTableName(cfg *config.Config) string {
	if cfg != nil {
		baseTable := strings.TrimSpace(cfg.Database.ClickHouse.BaseTable)
		if baseTable != "" {
			return baseTable
		}
	}
	return "klines_1m"
}

func filterRemainingSymbols(symbols []string, progress map[string]*domain.SymbolProgressInfo) []string {
	if len(symbols) == 0 {
		return nil
	}

	remaining := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		if status, exists := progress[symbol]; exists && isCompletedSymbolProgress(status) {
			continue
		}
		remaining = append(remaining, symbol)
	}

	return remaining
}

func isCompletedSymbolProgress(progress *domain.SymbolProgressInfo) bool {
	if progress == nil {
		return false
	}
	if strings.EqualFold(progress.Status, "completed") {
		return true
	}
	if progress.Progress >= 100 {
		return true
	}
	return progress.TotalMonths > 0 && progress.CompletedMonths >= progress.TotalMonths
}

func truncateToUTCDay(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}

func isWindowBoundaryDay(day, startDay, endDay time.Time) bool {
	currentDay := truncateToUTCDay(day)
	return currentDay.Equal(truncateToUTCDay(startDay)) || currentDay.Equal(truncateToUTCDay(endDay))
}

func isCoveredByExistingRange(day time.Time, existingRange *domain.SymbolDateRange) bool {
	if existingRange == nil || !existingRange.HasData {
		return false
	}

	currentDay := truncateToUTCDay(day)
	firstDay := truncateToUTCDay(existingRange.FirstDate)
	lastDay := truncateToUTCDay(existingRange.LastDate)
	return !currentDay.Before(firstDay) && !currentDay.After(lastDay)
}

func queryLatestAvailableDay(ctx context.Context, repository *clickhouse.Repository, baseTable string) (time.Time, error) {
	rows, err := repository.QueryContext(ctx, fmt.Sprintf("SELECT max(toDate(open_time)) FROM %s", baseTable))
	if err != nil {
		return time.Time{}, err
	}
	defer rows.Close()

	if !rows.Next() {
		return time.Time{}, fmt.Errorf("no rows returned while resolving latest available day")
	}

	var latestAvailableDay time.Time
	if err := rows.Scan(&latestAvailableDay); err != nil {
		return time.Time{}, fmt.Errorf("failed to scan latest available day: %w", err)
	}
	if latestAvailableDay.IsZero() {
		return time.Time{}, fmt.Errorf("latest available day is empty in %s", baseTable)
	}

	return truncateToUTCDay(latestAvailableDay), nil
}

func queryTopLiquiditySymbols(ctx context.Context, repository *clickhouse.Repository, baseTable string, startDay, endDay time.Time, limit int) ([]string, error) {
	endExclusive := truncateToUTCDay(endDay).AddDate(0, 0, 1)
	query := fmt.Sprintf(`
		SELECT symbol, sum(quote_asset_volume) AS quote_volume
		FROM %s
		WHERE open_time >= ? AND open_time < ?
		GROUP BY symbol
		ORDER BY quote_volume DESC, symbol ASC
		LIMIT %d
	`, baseTable, limit)

	rows, err := repository.QueryContext(ctx, query, truncateToUTCDay(startDay), endExclusive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var symbols []string
	for rows.Next() {
		var symbol string
		var quoteVolume float64
		if err := rows.Scan(&symbol, &quoteVolume); err != nil {
			return nil, fmt.Errorf("failed to scan top liquidity row: %w", err)
		}
		symbols = append(symbols, symbol)
	}

	return symbols, rows.Err()
}

func findSymbolsWithoutTasks(symbols []string, concurrentTasks []domain.ConcurrentTask) []string {
	if len(symbols) == 0 {
		return nil
	}

	withTasks := make(map[string]struct{}, len(concurrentTasks))
	for _, task := range concurrentTasks {
		withTasks[task.Symbol] = struct{}{}
	}

	var result []string
	for _, symbol := range symbols {
		if _, exists := withTasks[symbol]; !exists {
			result = append(result, symbol)
		}
	}

	return result
}

func markTasklessSymbolsCompleted(stateManager domain.StateManager, symbols []string, startDay, endDay time.Time) error {
	for _, symbol := range symbols {
		state, err := stateManager.GetState(symbol)
		if err != nil {
			return err
		}
		state.Symbol = symbol
		state.StartDate = startDay
		state.EndDate = endDay
		state.CompletedTaskDates = nil
		state.TotalFiles = 0
		state.Processed = 0
		state.Status = "completed"
		if err := stateManager.SaveState(state); err != nil {
			return err
		}

		if err := stateManager.UpdateSymbolProgress(symbol, &domain.SymbolProgressInfo{
			Symbol:          symbol,
			TotalMonths:     0,
			CompletedMonths: 0,
			FailedMonths:    0,
			CurrentMonth:    "",
			Progress:        100,
			Status:          "completed",
			WorkerID:        -1,
		}); err != nil {
			return err
		}
	}

	return nil
}

func seedSymbolState(stateManager domain.StateManager, concurrentTask domain.ConcurrentTask) error {
	state, err := stateManager.GetState(concurrentTask.Symbol)
	if err != nil {
		return err
	}

	if len(concurrentTask.Tasks) > 0 {
		state.StartDate = concurrentTask.Tasks[0].Date
		state.EndDate = concurrentTask.Tasks[len(concurrentTask.Tasks)-1].Date
	}
	state.CompletedTaskDates = filterCompletedTaskDatesForTasks(state.CompletedTaskDates, concurrentTask.Tasks)
	state.TotalFiles = len(concurrentTask.Tasks)
	state.Processed = len(state.CompletedTaskDates)
	state.Failed = 0
	state.LastDate = time.Time{}
	state.Status = "pending"
	state.Symbol = concurrentTask.Symbol

	return stateManager.SaveState(state)
}

func filterCompletedTaskDatesForWindow(completedDates []string, startDay, endDay time.Time) map[string]struct{} {
	result := make(map[string]struct{})
	if len(completedDates) == 0 {
		return result
	}

	startKey := truncateToUTCDay(startDay).Format("2006-01-02")
	endKey := truncateToUTCDay(endDay).Format("2006-01-02")
	for _, completedDate := range completedDates {
		if completedDate >= startKey && completedDate <= endKey {
			result[completedDate] = struct{}{}
		}
	}

	return result
}

func filterCompletedTaskDatesForTasks(completedDates []string, tasks []domain.DownloadTask) []string {
	if len(completedDates) == 0 || len(tasks) == 0 {
		return nil
	}

	taskDates := make(map[string]struct{}, len(tasks))
	for _, task := range tasks {
		taskDates[task.Date.UTC().Format("2006-01-02")] = struct{}{}
	}

	filtered := make([]string, 0, len(completedDates))
	for _, completedDate := range completedDates {
		if _, exists := taskDates[completedDate]; exists {
			filtered = append(filtered, completedDate)
		}
	}

	sort.Strings(filtered)
	return filtered
}

func copyCoveredDays(input map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(input))
	for day := range input {
		result[day] = struct{}{}
	}
	return result
}
