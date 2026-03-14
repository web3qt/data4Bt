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
	"binance-data-loader/pkg/aggtradeparser"
	"binance-data-loader/pkg/binance"
	"binance-data-loader/pkg/clickhouse"
)

const (
	futuresUM1sRepairBaseTable       = "klines_1s"
	futuresUM1sRepairDefaultBaseURL  = "https://data.binance.vision"
	futuresUM1sRepairAggTradesPath   = "/data/futures/um/daily/aggTrades"
	futuresUM1sRepairMutationTimeout = 10 * time.Minute
	futuresUM1sRepairMutationPoll    = time.Second
)

type DailyCountMismatch struct {
	Symbol        string
	Day           time.Time
	StoredCount   int
	OfficialCount int
}

type dailyCountStats struct {
	RowCount    int
	UniqueCount int
}

type repairMismatchDetail struct {
	Symbol          string
	Day             time.Time
	StoredRowCount  int
	StoredUniqCount int
	OfficialCount   int
}

func DiffDailySecondCounts(symbols []string, startDay, endDay time.Time, stored, official map[string]map[string]int) []DailyCountMismatch {
	if len(symbols) == 0 || startDay.After(endDay) {
		return nil
	}

	sortedSymbols := append([]string(nil), symbols...)
	sort.Strings(sortedSymbols)

	var mismatches []DailyCountMismatch
	for _, symbol := range sortedSymbols {
		for currentDay := truncateToUTCDay(startDay); !currentDay.After(endDay); currentDay = currentDay.AddDate(0, 0, 1) {
			dayKey := currentDay.Format("2006-01-02")
			storedCount := 0
			if symbolCounts, exists := stored[symbol]; exists {
				storedCount = symbolCounts[dayKey]
			}

			officialCount := 0
			if symbolCounts, exists := official[symbol]; exists {
				officialCount = symbolCounts[dayKey]
			}

			if storedCount == officialCount {
				continue
			}

			mismatches = append(mismatches, DailyCountMismatch{
				Symbol:        symbol,
				Day:           currentDay,
				StoredCount:   storedCount,
				OfficialCount: officialCount,
			})
		}
	}

	return mismatches
}

func BuildRepairTasks(mismatches []DailyCountMismatch, baseURL, dataPath string) []domain.ConcurrentTask {
	if len(mismatches) == 0 {
		return nil
	}

	grouped := make(map[string][]time.Time)
	for _, mismatch := range mismatches {
		day := truncateToUTCDay(mismatch.Day)
		grouped[mismatch.Symbol] = append(grouped[mismatch.Symbol], day)
	}

	symbols := make([]string, 0, len(grouped))
	for symbol := range grouped {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols)

	result := make([]domain.ConcurrentTask, 0, len(symbols))
	for _, symbol := range symbols {
		days := grouped[symbol]
		sort.Slice(days, func(i, j int) bool {
			return days[i].Before(days[j])
		})

		tasks := make([]domain.DownloadTask, 0, len(days))
		for _, day := range days {
			tasks = append(tasks, domain.DownloadTask{
				Symbol:   symbol,
				Date:     day,
				Interval: "1s",
				URL:      BuildDailyArchiveURL(baseURL, dataPath, symbol, day),
			})
		}

		result = append(result, domain.ConcurrentTask{
			Symbol:   symbol,
			Tasks:    tasks,
			Priority: 0,
		})
	}

	return result
}

func RunFuturesUM1sRepairGaps(ctx context.Context, cfg *config.Config, dryRun bool) error {
	effectiveCfg := normalizeFuturesUM1sRepairConfig(cfg)
	log := logger.GetLogger("windowimport_futures_um_1s_repair")

	symbols := ResolveConfiguredSymbols(effectiveCfg)
	if len(symbols) == 0 {
		return fmt.Errorf("futures-um-1s-repair-gaps requires explicit symbols via -symbols or binance.explicit_symbols")
	}

	startDay, endDay, err := ResolveWindowRange(effectiveCfg, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to resolve repair window: %w", err)
	}

	repository, err := clickhouse.NewRepository(effectiveCfg.Database.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to create repository: %w", err)
	}
	defer repository.Close()

	downloader := binance.NewBinanceDownloader(effectiveCfg.Binance, effectiveCfg.Downloader)
	csvParser := aggtradeparser.NewAggTradeCSVParser()

	storedStats, err := queryStoredDailySecondStats(ctx, repository, effectiveCfg.Database.ClickHouse.BaseTable, symbols, startDay, endDay)
	if err != nil {
		return fmt.Errorf("failed to query stored daily second stats: %w", err)
	}

	officialCounts, err := queryOfficialDailySecondCounts(
		ctx,
		downloader,
		csvParser,
		BuildMissingWindowTasks(symbols, startDay, endDay, effectiveCfg.Binance.BaseURL, effectiveCfg.Binance.DataPath, nil),
	)
	if err != nil {
		return fmt.Errorf("failed to query official daily second counts: %w", err)
	}

	mismatches := buildRepairMismatchDetails(symbols, startDay, endDay, storedStats, officialCounts)
	printRepairSummary(mismatches, dryRun)
	if len(mismatches) == 0 {
		log.Info().
			Int("symbol_count", len(symbols)).
			Str("start_day", startDay.Format("2006-01-02")).
			Str("end_day", endDay.Format("2006-01-02")).
			Msg("No 1s gap mismatches detected")
		return nil
	}

	if dryRun {
		return nil
	}

	repairTasks := BuildRepairTasks(repairDetailsToMismatches(mismatches), effectiveCfg.Binance.BaseURL, effectiveCfg.Binance.DataPath)
	if err := deleteRepairTaskRanges(ctx, repository, repairTasks); err != nil {
		return fmt.Errorf("failed to delete mismatched ranges: %w", err)
	}
	if err := waitForPendingMutations(ctx, repository, effectiveCfg.Database.ClickHouse.BaseTable, futuresUM1sRepairMutationTimeout); err != nil {
		return fmt.Errorf("failed waiting for delete mutations: %w", err)
	}
	if err := reimportRepairTasks(ctx, repository, downloader, csvParser, repairTasks); err != nil {
		return fmt.Errorf("failed to reimport repaired ranges: %w", err)
	}

	verifiedStats, err := queryStoredDailySecondStats(ctx, repository, effectiveCfg.Database.ClickHouse.BaseTable, symbols, startDay, endDay)
	if err != nil {
		return fmt.Errorf("failed to verify repaired daily second stats: %w", err)
	}

	remaining := filterRemainingMismatchDetails(mismatches, verifiedStats, officialCounts)
	if len(remaining) > 0 {
		printRepairSummary(remaining, false)
		return fmt.Errorf("repair completed with %d remaining mismatched symbol-days", len(remaining))
	}

	fmt.Printf(
		"repaired %d mismatched symbol-days for %d symbols in %s -> %s\n",
		len(mismatches),
		len(symbols),
		startDay.Format("2006-01-02"),
		endDay.Format("2006-01-02"),
	)
	log.Info().
		Int("mismatch_days", len(mismatches)).
		Int("symbol_count", len(symbols)).
		Str("start_day", startDay.Format("2006-01-02")).
		Str("end_day", endDay.Format("2006-01-02")).
		Msg("Futures UM 1s gap repair completed")

	return nil
}

func normalizeFuturesUM1sRepairConfig(cfg *config.Config) *config.Config {
	if cfg == nil {
		cfg = &config.Config{}
	}

	effectiveCfg := *cfg
	effectiveCfg.Binance = cfg.Binance
	effectiveCfg.Downloader = cfg.Downloader
	effectiveCfg.Database = cfg.Database
	effectiveCfg.Scheduler = cfg.Scheduler

	effectiveCfg.Binance.MarketType = "futures_um"
	effectiveCfg.Binance.Interval = "1s"
	effectiveCfg.Binance.DataPath = futuresUM1sRepairAggTradesPath
	if strings.TrimSpace(effectiveCfg.Binance.BaseURL) == "" {
		effectiveCfg.Binance.BaseURL = futuresUM1sRepairDefaultBaseURL
	}
	effectiveCfg.Database.ClickHouse.BaseTable = futuresUM1sRepairBaseTable

	return &effectiveCfg
}

func queryStoredDailySecondStats(
	ctx context.Context,
	repository *clickhouse.Repository,
	baseTable string,
	symbols []string,
	startDay, endDay time.Time,
) (map[string]map[string]dailyCountStats, error) {
	results := make(map[string]map[string]dailyCountStats, len(symbols))
	for _, symbol := range symbols {
		results[symbol] = make(map[string]dailyCountStats)
	}
	if len(symbols) == 0 {
		return results, nil
	}

	start := truncateToUTCDay(startDay)
	endExclusive := truncateToUTCDay(endDay).AddDate(0, 0, 1)

	query := fmt.Sprintf(`
		SELECT
			symbol,
			toDate(open_time) AS trade_day,
			count() AS row_count,
			uniqExact(open_time) AS uniq_count
		FROM %s
		WHERE symbol IN (%s)
		  AND open_time >= ?
		  AND open_time < ?
		GROUP BY symbol, trade_day
		ORDER BY symbol ASC, trade_day ASC
	`, baseTable, buildInClause(len(symbols)))

	args := make([]interface{}, 0, len(symbols)+2)
	for _, symbol := range symbols {
		args = append(args, symbol)
	}
	args = append(args, start, endExclusive)

	rows, err := repository.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			symbol    string
			tradeDay  time.Time
			rowCount  uint64
			uniqCount uint64
		)
		if err := rows.Scan(&symbol, &tradeDay, &rowCount, &uniqCount); err != nil {
			return nil, fmt.Errorf("failed to scan stored daily second stats row: %w", err)
		}
		if _, exists := results[symbol]; !exists {
			results[symbol] = make(map[string]dailyCountStats)
		}
		results[symbol][tradeDay.UTC().Format("2006-01-02")] = dailyCountStats{
			RowCount:    int(rowCount),
			UniqueCount: int(uniqCount),
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate stored daily second stats: %w", err)
	}

	return results, nil
}

func queryOfficialDailySecondCounts(
	ctx context.Context,
	downloader domain.Downloader,
	csvParser domain.Parser,
	tasks []domain.ConcurrentTask,
) (map[string]map[string]int, error) {
	results := make(map[string]map[string]int, len(tasks))
	for _, concurrentTask := range tasks {
		if _, exists := results[concurrentTask.Symbol]; !exists {
			results[concurrentTask.Symbol] = make(map[string]int)
		}
		for _, task := range concurrentTask.Tasks {
			count, err := fetchOfficialDailySecondCount(ctx, downloader, csvParser, task)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch official daily count for %s %s: %w", task.Symbol, task.Date.Format("2006-01-02"), err)
			}
			results[task.Symbol][task.Date.UTC().Format("2006-01-02")] = count
		}
	}
	return results, nil
}

func fetchOfficialDailySecondCount(ctx context.Context, downloader domain.Downloader, csvParser domain.Parser, task domain.DownloadTask) (int, error) {
	klines, err := fetchDailyAggTradeKLines(ctx, downloader, csvParser, task)
	if err != nil {
		return 0, err
	}
	return len(klines), nil
}

func fetchDailyAggTradeKLines(ctx context.Context, downloader domain.Downloader, csvParser domain.Parser, task domain.DownloadTask) ([]domain.KLine, error) {
	data, err := downloader.Fetch(ctx, task)
	if err != nil {
		if domain.IsDataNotAvailableError(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	klines, _, err := csvParser.Parse(ctx, data, task.Symbol)
	if err != nil {
		if isEmptyAggTradeResult(err) {
			return nil, nil
		}
		return nil, err
	}

	return klines, nil
}

func buildRepairMismatchDetails(
	symbols []string,
	startDay, endDay time.Time,
	storedStats map[string]map[string]dailyCountStats,
	officialCounts map[string]map[string]int,
) []repairMismatchDetail {
	sortedSymbols := append([]string(nil), symbols...)
	sort.Strings(sortedSymbols)

	var mismatches []repairMismatchDetail
	for _, symbol := range sortedSymbols {
		for currentDay := truncateToUTCDay(startDay); !currentDay.After(endDay); currentDay = currentDay.AddDate(0, 0, 1) {
			dayKey := currentDay.Format("2006-01-02")
			stats := storedStats[symbol][dayKey]
			officialCount := officialCounts[symbol][dayKey]
			if stats.RowCount == officialCount && stats.UniqueCount == officialCount {
				continue
			}
			mismatches = append(mismatches, repairMismatchDetail{
				Symbol:          symbol,
				Day:             currentDay,
				StoredRowCount:  stats.RowCount,
				StoredUniqCount: stats.UniqueCount,
				OfficialCount:   officialCount,
			})
		}
	}

	return mismatches
}

func repairDetailsToMismatches(details []repairMismatchDetail) []DailyCountMismatch {
	result := make([]DailyCountMismatch, 0, len(details))
	for _, detail := range details {
		result = append(result, DailyCountMismatch{
			Symbol:        detail.Symbol,
			Day:           detail.Day,
			StoredCount:   detail.StoredRowCount,
			OfficialCount: detail.OfficialCount,
		})
	}
	return result
}

func deleteRepairTaskRanges(ctx context.Context, repository *clickhouse.Repository, tasks []domain.ConcurrentTask) error {
	for _, concurrentTask := range tasks {
		for _, task := range concurrentTask.Tasks {
			start := truncateToUTCDay(task.Date)
			end := start.AddDate(0, 0, 1).Add(-time.Second)
			if err := repository.DeleteDataInRange(ctx, task.Symbol, start, end); err != nil {
				return fmt.Errorf("failed to delete %s %s: %w", task.Symbol, task.Date.Format("2006-01-02"), err)
			}
		}
	}
	return nil
}

func reimportRepairTasks(
	ctx context.Context,
	repository *clickhouse.Repository,
	downloader domain.Downloader,
	csvParser domain.Parser,
	tasks []domain.ConcurrentTask,
) error {
	for _, concurrentTask := range tasks {
		for _, task := range concurrentTask.Tasks {
			klines, err := fetchDailyAggTradeKLines(ctx, downloader, csvParser, task)
			if err != nil {
				return fmt.Errorf("failed to fetch repair data for %s %s: %w", task.Symbol, task.Date.Format("2006-01-02"), err)
			}
			if len(klines) == 0 {
				continue
			}
			if err := repository.Save(ctx, klines); err != nil {
				return fmt.Errorf("failed to save repaired data for %s %s: %w", task.Symbol, task.Date.Format("2006-01-02"), err)
			}
		}
	}
	return nil
}

func filterRemainingMismatchDetails(
	expected []repairMismatchDetail,
	storedStats map[string]map[string]dailyCountStats,
	officialCounts map[string]map[string]int,
) []repairMismatchDetail {
	var remaining []repairMismatchDetail
	for _, detail := range expected {
		dayKey := detail.Day.UTC().Format("2006-01-02")
		stats := storedStats[detail.Symbol][dayKey]
		officialCount := officialCounts[detail.Symbol][dayKey]
		if stats.RowCount == officialCount && stats.UniqueCount == officialCount {
			continue
		}
		remaining = append(remaining, repairMismatchDetail{
			Symbol:          detail.Symbol,
			Day:             detail.Day,
			StoredRowCount:  stats.RowCount,
			StoredUniqCount: stats.UniqueCount,
			OfficialCount:   officialCount,
		})
	}
	return remaining
}

func waitForPendingMutations(ctx context.Context, repository *clickhouse.Repository, table string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	query := `
		SELECT count()
		FROM system.mutations
		WHERE database = currentDatabase()
		  AND table = ?
		  AND is_done = 0
	`

	ticker := time.NewTicker(futuresUM1sRepairMutationPoll)
	defer ticker.Stop()

	for {
		rows, err := repository.QueryContext(waitCtx, query, table)
		if err != nil {
			return fmt.Errorf("failed to query pending mutations: %w", err)
		}

		var pending uint64
		if rows.Next() {
			if err := rows.Scan(&pending); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan pending mutation count: %w", err)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("failed to iterate pending mutation rows: %w", err)
		}
		rows.Close()

		if pending == 0 {
			return nil
		}

		select {
		case <-waitCtx.Done():
			return waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func printRepairSummary(mismatches []repairMismatchDetail, dryRun bool) {
	mode := "apply"
	if dryRun {
		mode = "dry-run"
	}

	fmt.Printf("%s mismatch_days=%d\n", mode, len(mismatches))
	for _, mismatch := range mismatches {
		fmt.Printf(
			"%s %s stored_rows=%d stored_unique=%d official=%d\n",
			mismatch.Symbol,
			mismatch.Day.Format("2006-01-02"),
			mismatch.StoredRowCount,
			mismatch.StoredUniqCount,
			mismatch.OfficialCount,
		)
	}
}

func buildInClause(count int) string {
	if count <= 0 {
		return ""
	}

	placeholders := make([]string, count)
	for idx := 0; idx < count; idx++ {
		placeholders[idx] = "?"
	}
	return strings.Join(placeholders, ",")
}

func isEmptyAggTradeResult(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "no valid aggTrade rows parsed")
}
