package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
	"binance-data-loader/internal/state"
	"binance-data-loader/pkg/binance"
	"binance-data-loader/pkg/clickhouse"
	"binance-data-loader/pkg/importer"
	"binance-data-loader/pkg/monitor"
	"binance-data-loader/pkg/parser"
	"binance-data-loader/pkg/scheduler"
)

/*
# 查询所有交易对并保存到文件
./data_loader -cmd=range-query -output=ranges.txt

# 查询指定交易对并保存
./data_loader -cmd=range-query -symbols=BTCUSDT,ETHUSDT -output=my_ranges.txt

# 使用短参数名
./data_loader -cmd=range-query -symbols=BTC,ETH -o=top_coins.txt
*/

var (
	configFile = flag.String("config", "config.yml", "Configuration file path")
	command    = flag.String("cmd", "run", "Command to execute: run, validate, init-db, create-views, status, discover, concurrent, update-latest, range-query")
	symbols    = flag.String("symbols", "", "Comma-separated list of symbols to process (optional)")
	endDate    = flag.String("end", "", "End date (YYYY-MM-DD)")
	output     = flag.String("output", "", "Output file path for range-query results (optional)")
	verbose    = flag.Bool("verbose", false, "Enable verbose logging")
	version    = flag.Bool("version", false, "Show version information")
	detailed   = flag.Bool("detailed", false, "Show detailed status information")
	concurrent = flag.Bool("concurrent", false, "Enable concurrent mode")
)

const (
	appName    = "Binance Data Loader"
	appVersion = "1.0.0"
	buildDate  = "2025-07-18"
)

func main() {
	flag.Parse()

	if *version {
		fmt.Printf("%s v%s (built on %s)\n", appName, appVersion, buildDate)
		os.Exit(0)
	}

	// 加载配置
	cfg, err := config.Load(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 如果启用了详细日志，覆盖配置
	if *verbose {
		cfg.Log.Level = "debug"
	}

	// 初始化日志
	if err := logger.Init(cfg.Log); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	log := logger.GetLogger("main")
	log.Info().
		Str("app", appName).
		Str("version", appVersion).
		Str("command", *command).
		Msg("Starting application")

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置信号处理
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info().Str("signal", sig.String()).Msg("Received signal, shutting down...")
		cancel()
	}()

	// 执行命令
	if err := executeCommand(ctx, cfg, *command); err != nil {
		log.Error().Err(err).Str("command", *command).Msg("Command execution failed")
		os.Exit(1)
	}

	log.Info().Msg("Application completed successfully")
}

func executeCommand(ctx context.Context, cfg *config.Config, cmd string) error {
	switch cmd {
	case "run":
		if *concurrent {
			return runConcurrentDataLoader(ctx, cfg)
		} else {
			return runDataLoader(ctx, cfg)
		}
	case "concurrent":
		return runConcurrentDataLoader(ctx, cfg)
	case "update-latest":
		return updateToLatest(ctx, cfg)
	case "validate":
		return validateData(ctx, cfg)
	case "init-db":
		return initializeDatabase(ctx, cfg)
	case "create-views":
		return createMaterializedViews(ctx, cfg)
	case "status":
		return showStatus(ctx, cfg)
	case "discover":
		return discoverSymbols(ctx, cfg)
	case "range-query":
		return queryDataRanges(ctx, cfg)
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

func runDataLoader(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("data_loader")
	log.Info().Msg("Starting data loader")

	// 初始化组件
	components, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer components.cleanup()

	// 初始化数据库表
	if err := components.repository.CreateTables(ctx); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// 解析日期参数
	endDateTime, err := parseDateRange(cfg)
	if err != nil {
		return fmt.Errorf("failed to parse date range: %w", err)
	}

	// 更新调度器配置
	cfg.Scheduler.EndDate = endDateTime.Format("2006-01-02")

	// 创建调度器
	scheduler := scheduler.NewScheduler(
		cfg.Scheduler,
		components.downloader,
		components.importer,
		components.stateManager,
		components.progressReporter,
		components.repository,
	)

	// 运行调度器
	if err := scheduler.Run(ctx); err != nil {
		return fmt.Errorf("scheduler execution failed: %w", err)
	}

	// 停止调度器
	if err := scheduler.Stop(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to stop scheduler gracefully")
	}

	log.Info().Msg("Data loader completed successfully")
	return nil
}

func runConcurrentDataLoader(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("concurrent_data_loader")
	log.Info().Msg("Starting concurrent data loader")

	// 初始化组件
	components, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer components.cleanup()

	// 初始化数据库表
	if err := components.repository.CreateTables(ctx); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// 解析日期参数
	endDateTime, err := parseDateRange(cfg)
	if err != nil {
		return fmt.Errorf("failed to parse date range: %w", err)
	}

	// 更新调度器配置
	cfg.Scheduler.EndDate = endDateTime.Format("2006-01-02")

	// 创建调度器
	scheduler := scheduler.NewScheduler(
		cfg.Scheduler,
		components.downloader,
		components.importer,
		components.stateManager,
		components.progressReporter,
		components.repository,
	)

	// 运行并发调度器
	if err := scheduler.RunConcurrent(ctx); err != nil {
		return fmt.Errorf("concurrent scheduler execution failed: %w", err)
	}

	// 停止调度器
	if err := scheduler.Stop(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to stop scheduler gracefully")
	}

	log.Info().Msg("Concurrent data loader completed successfully")
	return nil
}

func updateToLatest(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("update_to_latest")
	log.Info().Msg("Starting update to latest")

	// 初始化组件
	components, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer components.cleanup()

	// 初始化数据库表
	if err := components.repository.CreateTables(ctx); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// 创建调度器
	scheduler := scheduler.NewScheduler(
		cfg.Scheduler,
		components.downloader,
		components.importer,
		components.stateManager,
		components.progressReporter,
		components.repository,
	)

	// 执行更新到最新
	if err := scheduler.UpdateToLatest(ctx); err != nil {
		return fmt.Errorf("update to latest failed: %w", err)
	}

	// 停止调度器
	if err := scheduler.Stop(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to stop scheduler gracefully")
	}

	log.Info().Msg("Update to latest completed successfully")
	return nil
}

func validateData(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("validator")
	log.Info().Msg("Starting data validation")

	// 初始化组件
	components, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer components.cleanup()

	// 解析日期参数
	endDateTime, err := parseDateRange(cfg)
	if err != nil {
		return fmt.Errorf("failed to parse date range: %w", err)
	}

	// 获取要验证的交易对
	symbolList, err := getSymbolList(ctx, components.downloader)
	if err != nil {
		return fmt.Errorf("failed to get symbol list: %w", err)
	}

	// 创建调度器并执行验证
	scheduler := scheduler.NewScheduler(
		cfg.Scheduler,
		components.downloader,
		components.importer,
		components.stateManager,
		components.progressReporter,
		components.repository,
	)
	// 验证数据
	if err := scheduler.ValidateData(ctx, symbolList, endDateTime); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	log.Info().Msg("Data validation completed")
	return nil
}

func initializeDatabase(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("db_init")
	log.Info().Msg("Initializing database")

	// 创建ClickHouse仓库
	repository, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to create repository: %w", err)
	}
	defer repository.Close()

	// 创建表
	if err := repository.CreateTables(ctx); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	log.Info().Msg("Database initialized successfully")
	return nil
}

func createMaterializedViews(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("mv_creator")
	log.Info().Msg("Creating materialized views")

	// 创建ClickHouse仓库
	repository, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to create repository: %w", err)
	}
	defer repository.Close()

	// 创建物化视图
	intervals := cfg.MaterializedViews.Intervals
	if len(intervals) == 0 {
		intervals = []string{"5m", "15m", "1h", "4h", "1d"}
	}

	if err := repository.CreateMaterializedViews(ctx, intervals); err != nil {
		return fmt.Errorf("failed to create materialized views: %w", err)
	}

	log.Info().Msg("Materialized views created successfully")
	return nil
}

type components struct {
	downloader       *binance.BinanceDownloader
	parser           *parser.CSVParser
	repository       *clickhouse.Repository
	stateManager     *state.FileStateManager
	progressReporter *monitor.ProgressReporter
	importer         *importer.Importer
}

func (c *components) cleanup() {
	if c.repository != nil {
		c.repository.Close()
	}
	if c.importer != nil {
		c.importer.Close()
	}
}

func initializeComponents(cfg *config.Config) (*components, error) {
	// 创建下载器
	downloader := binance.NewBinanceDownloader(cfg.Binance, cfg.Downloader)

	// 创建解析器
	parser := parser.NewCSVParser(cfg.Parser)

	// 创建ClickHouse仓库
	repository, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("failed to create repository: %w", err)
	}

	// 创建状态管理器
	stateManager, err := state.NewFileStateManager(cfg.State)
	if err != nil {
		return nil, fmt.Errorf("failed to create state manager: %w", err)
	}

	// 创建进度报告器
	var progressReporter *monitor.ProgressReporter
	if cfg.Monitoring.Enabled {
		progressReporter = monitor.NewProgressReporter(cfg.Monitoring)
	}

	// 创建导入器
	importer := importer.NewImporter(
		cfg.Importer,
		downloader,
		parser,
		repository,
		stateManager,
		progressReporter,
	)

	return &components{
		downloader:       downloader,
		parser:           parser,
		repository:       repository,
		stateManager:     stateManager,
		progressReporter: progressReporter,
		importer:         importer,
	}, nil
}

func parseDateRange(cfg *config.Config) (time.Time, error) {
	var endDateTime time.Time
	var err error

	// 解析结束日期
	if *endDate != "" {
		endDateTime, err = time.Parse("2006-01-02", *endDate)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid end date format: %w", err)
		}
	} else if cfg.Scheduler.EndDate != "" {
		endDateTime, err = time.Parse("2006-01-02", cfg.Scheduler.EndDate)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid config end date format: %w", err)
		}
	} else {
		// 默认使用昨天作为结束日期
		endDateTime = time.Now().AddDate(0, 0, -1)
	}

	// 如果结束日期是今天或未来，调整为昨天
	yesterday := time.Now().AddDate(0, 0, -1)
	if endDateTime.After(yesterday) {
		endDateTime = yesterday
		log := logger.GetLogger("main")
		log.Info().
			Str("adjusted_end_date", endDateTime.Format("2006-01-02")).
			Msg("Adjusted end date to yesterday (data may not be available for today)")
	}

	return endDateTime, nil
}

func getSymbolList(ctx context.Context, downloader *binance.BinanceDownloader) ([]string, error) {
	if *symbols != "" {
		// 使用命令行指定的交易对
		return strings.Split(*symbols, ","), nil
	}

	// 从下载器获取所有可用的交易对
	return downloader.GetSymbols(ctx)
}

// showStatus 显示下载状态
func showStatus(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("status")
	log.Info().Msg("Showing system status")

	// 初始化组件
	components, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer components.cleanup()

	// 获取所有状态
	allStates, err := components.stateManager.GetAllStates()
	if err != nil {
		return fmt.Errorf("failed to get states: %w", err)
	}

	if len(allStates) == 0 {
		fmt.Println("没有找到任何下载状态记录")
		fmt.Println("提示：请先运行 'go run cmd/main.go -cmd=run' 开始下载数据")
		return nil
	}

	// 过滤指定的symbols
	if *symbols != "" {
		requestedSymbols := strings.Split(*symbols, ",")
		filteredStates := make(map[string]*domain.ProcessingState)
		for _, symbol := range requestedSymbols {
			symbol = strings.TrimSpace(strings.ToUpper(symbol))
			if state, exists := allStates[symbol]; exists {
				filteredStates[symbol] = state
			} else {
				fmt.Printf("警告: 未找到代币 %s 的状态记录\n", symbol)
			}
		}
		allStates = filteredStates
	}

	// 获取符号时间线
	timelines, err := components.stateManager.GetAllTimelines()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get timelines")
	} else {
		log.Info().Int("count", len(timelines)).Msg("Symbol timelines")
		for symbol, timeline := range timelines {
			log.Info().
				Str("symbol", symbol).
				Time("historical_start", timeline.HistoricalStartDate).
				Time("current_import", timeline.CurrentImportDate).
				Time("latest_available", timeline.LatestAvailableDate).
				Int("total_months", timeline.TotalMonths).
				Int("imported_months", timeline.ImportedMonthsCount).
				Time("last_updated", timeline.LastUpdated).
				Msg("Timeline")
		}
	}

	// 获取worker状态
	workerStates, err := components.stateManager.GetAllWorkerStates()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get worker states")
	} else {
		log.Info().Int("count", len(workerStates)).Msg("Worker states")
		for workerID, state := range workerStates {
			log.Info().
				Int("worker_id", workerID).
				Str("status", state.Status).
				Str("current_symbol", state.CurrentSymbol).
				Int("tasks_count", state.TasksCount).
				Int("completed_tasks", state.CompletedTasks).
				Int("failed_tasks", state.FailedTasks).
				Str("error_message", state.ErrorMessage).
				Time("start_time", state.StartTime).
				Time("last_update", state.LastUpdate).
				Msg("Worker state")
		}
	}

	// 获取币种进度信息
	symbolProgresses, err := components.stateManager.GetAllSymbolProgress()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get symbol progress")
	} else {
		log.Info().Int("count", len(symbolProgresses)).Msg("Symbol progress information")
		for symbol, progress := range symbolProgresses {
			log.Info().
				Str("symbol", symbol).
				Str("status", progress.Status).
				Int("total_months", progress.TotalMonths).
				Int("completed_months", progress.CompletedMonths).
				Int("failed_months", progress.FailedMonths).
				Str("current_month", progress.CurrentMonth).
				Float64("progress", progress.Progress).
				Int("worker_id", progress.WorkerID).
				Time("last_update", progress.LastUpdate).
				Msg("Symbol progress")
		}
	}

	// 显示总体状态
	fmt.Printf("\n=== Binance 数据下载状态 ===\n\n")
	totalCompleted := 0
	totalFailed := 0
	for _, state := range allStates {
		totalCompleted += state.Processed
		totalFailed += state.Failed
	}
	fmt.Printf("已完成任务: %d\n", totalCompleted)
	if totalFailed > 0 {
		fmt.Printf("失败任务: %d\n", totalFailed)
	}
	fmt.Printf("代币数量: %d\n", len(allStates))
	fmt.Println()

	// 按符号排序
	var symbolList []string
	for symbol := range allStates {
		symbolList = append(symbolList, symbol)
	}
	sort.Strings(symbolList)

	// 显示详细状态
	if *detailed {
		fmt.Printf("%-12s %-12s %-8s %-8s %-20s %-10s\n",
			"代币", "最后日期", "已完成", "失败", "最后更新", "状态")
		fmt.Println(strings.Repeat("-", 80))

		for _, symbol := range symbolList {
			state := allStates[symbol]

			lastDateStr := "未开始"
			if !state.LastDate.IsZero() {
				lastDateStr = state.LastDate.Format("2006-01-02")
			}

			lastUpdatedStr := state.LastUpdated.Format("2006-01-02 15:04")

			status := "进行中"
			if state.Failed > 0 {
				status = "有错误"
			} else if state.Processed == 0 {
				status = "等待中"
			} else if state.Processed > 0 {
				status = "已处理"
			}

			fmt.Printf("%-12s %-12s %-8d %-8d %-20s %-10s\n",
				symbol, lastDateStr, state.Processed,
				state.Failed, lastUpdatedStr, status)
		}

		// 显示数据库中的数据统计
		if timelines != nil && len(timelines) > 0 {
			fmt.Printf("\n=== 数据库时间范围统计 ===\n")
			fmt.Printf("%-12s %-20s %-12s\n", "代币", "时间范围", "记录数")
			fmt.Println(strings.Repeat("-", 50))
			for _, symbol := range symbolList {
				if timeline, exists := timelines[symbol]; exists {
					dateRange := fmt.Sprintf("%s to %s",
						timeline.HistoricalStartDate.Format("2006-01-02"),
						timeline.CurrentImportDate.Format("2006-01-02"))
					fmt.Printf("%-12s %-20s %-12d\n",
						symbol, dateRange, timeline.ImportedMonthsCount)
				}
			}
		}
	} else {
		// 简化显示
		fmt.Printf("%-12s %-12s %-8s %-8s\n", "代币", "最后日期", "已完成", "状态")
		fmt.Println(strings.Repeat("-", 45))

		for _, symbol := range symbolList {
			state := allStates[symbol]

			lastDateStr := "未开始"
			if !state.LastDate.IsZero() {
				lastDateStr = state.LastDate.Format("2006-01-02")
			}

			status := "进行中"
			if state.Failed > 0 {
				status = "有错误"
			} else if state.Processed == 0 {
				status = "等待中"
			} else if state.Processed > 0 {
				status = "已处理"
			}

			fmt.Printf("%-12s %-12s %-8d %-8s\n",
				symbol, lastDateStr, state.Processed, status)
		}
	}

	fmt.Printf("\n提示：\n")
	fmt.Printf("- 使用 -detailed 参数查看详细信息\n")
	fmt.Printf("- 使用 -symbols=BTCUSDT,ETHUSDT 查看特定代币状态\n")
	fmt.Printf("- 数据存储位置: %s\n", cfg.State.FilePath)

	return nil
}

// discoverSymbols 发现并显示所有代币的时间线信息
func discoverSymbols(ctx context.Context, cfg *config.Config) error {
	fmt.Println("🔍 正在发现币安USDT交易对的完整时间线信息...")
	fmt.Println()

	// 初始化组件
	comps, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer comps.cleanup()

	// 获取所有USDT交易对
	fmt.Println("📡 从币安数据页面获取所有USDT交易对...")
	allSymbols, err := comps.downloader.GetAllSymbolsFromBinance(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbols from Binance: %w", err)
	}

	fmt.Printf("✅ 发现 %d 个USDT交易对\n\n", len(allSymbols))

	// 如果指定了特定符号，只处理这些符号
	var targetSymbols []string
	if *symbols != "" {
		targetSymbols = strings.Split(*symbols, ",")
		for i, symbol := range targetSymbols {
			targetSymbols[i] = strings.TrimSpace(strings.ToUpper(symbol))
		}
		fmt.Printf("🎯 只分析指定的 %d 个交易对: %s\n\n", len(targetSymbols), strings.Join(targetSymbols, ", "))
	} else {
		targetSymbols = allSymbols
	}

	// 分析每个交易对的时间线
	fmt.Println("📊 正在分析交易对时间线...")
	var timelines []*domain.SymbolTimeline

	for i, symbol := range targetSymbols {
		fmt.Printf("[%d/%d] 分析 %s...", i+1, len(targetSymbols), symbol)

		timeline, err := comps.downloader.GetSymbolTimeline(ctx, symbol)
		if err != nil {
			fmt.Printf(" ❌ 失败: %v\n", err)
			continue
		}

		// 保存时间线到状态管理器
		if err := comps.stateManager.SaveTimeline(timeline); err != nil {
			fmt.Printf(" ⚠️  保存失败: %v\n", err)
		} else {
			fmt.Printf(" ✅ 完成 (%d个月)\n", timeline.TotalMonths)
		}

		timelines = append(timelines, timeline)
	}

	fmt.Println()
	fmt.Printf("🎉 时间线分析完成！成功分析了 %d 个交易对\n\n", len(timelines))

	// 显示汇总信息
	displayTimelineSummary(timelines)

	// 显示详细信息（如果请求）
	if *detailed {
		fmt.Println()
		displayDetailedTimelines(timelines)
	}

	fmt.Println()
	fmt.Println("💡 提示:")
	fmt.Println("   - 使用 'go run cmd/main.go -cmd=discover -symbols=BTCUSDT,ETHUSDT' 分析特定交易对")
	fmt.Println("   - 使用 'go run cmd/main.go -cmd=discover -detailed' 查看详细信息")
	fmt.Println("   - 使用 'go run cmd/main.go -cmd=status' 查看导入状态")

	return nil
}

// displayTimelineSummary 显示时间线汇总信息
func displayTimelineSummary(timelines []*domain.SymbolTimeline) {
	fmt.Println("📈 时间线汇总:")
	fmt.Println(strings.Repeat("=", 80))

	if len(timelines) == 0 {
		fmt.Println("   没有找到任何时间线数据")
		return
	}

	// 统计信息
	totalMonths := 0
	earliestDate := time.Now()
	latestDate := time.Time{}

	for _, timeline := range timelines {
		totalMonths += timeline.TotalMonths
		if timeline.HistoricalStartDate.Before(earliestDate) {
			earliestDate = timeline.HistoricalStartDate
		}
		if timeline.LatestAvailableDate.After(latestDate) {
			latestDate = timeline.LatestAvailableDate
		}
	}

	fmt.Printf("   交易对数量: %d\n", len(timelines))
	fmt.Printf("   总月份数据: %d\n", totalMonths)
	fmt.Printf("   最早数据: %s\n", earliestDate.Format("2006-01"))
	fmt.Printf("   最新数据: %s\n", latestDate.Format("2006-01"))

	// 按月份数排序显示前10
	sort.Slice(timelines, func(i, j int) bool {
		return timelines[i].TotalMonths > timelines[j].TotalMonths
	})

	fmt.Println()
	fmt.Println("🏆 数据最丰富的交易对 (前10):")
	fmt.Printf("%-12s %-8s %-12s %-12s\n", "交易对", "月份数", "开始时间", "结束时间")
	fmt.Println(strings.Repeat("-", 50))

	for i, timeline := range timelines {
		if i >= 10 {
			break
		}
		fmt.Printf("%-12s %-8d %-12s %-12s\n",
			timeline.Symbol,
			timeline.TotalMonths,
			timeline.HistoricalStartDate.Format("2006-01"),
			timeline.LatestAvailableDate.Format("2006-01"))
	}
}

// displayDetailedTimelines 显示详细的时间线信息
func displayDetailedTimelines(timelines []*domain.SymbolTimeline) {
	fmt.Println("📋 详细时间线信息:")
	fmt.Println(strings.Repeat("=", 80))

	for _, timeline := range timelines {
		fmt.Printf("\n🪙 %s:\n", timeline.Symbol)
		fmt.Printf("   状态: %s\n", timeline.Status)
		fmt.Printf("   总月份: %d\n", timeline.TotalMonths)
		fmt.Printf("   时间范围: %s 至 %s\n",
			timeline.HistoricalStartDate.Format("2006-01"),
			timeline.LatestAvailableDate.Format("2006-01"))

		if len(timeline.AvailableMonths) > 0 {
			fmt.Printf("   可用月份: ")
			if len(timeline.AvailableMonths) <= 12 {
				// 如果月份不多，显示全部
				fmt.Printf("%s\n", strings.Join(timeline.AvailableMonths, ", "))
			} else {
				// 如果月份很多，只显示前几个和后几个
				first := timeline.AvailableMonths[:3]
				last := timeline.AvailableMonths[len(timeline.AvailableMonths)-3:]
				fmt.Printf("%s ... %s\n", strings.Join(first, ", "), strings.Join(last, ", "))
			}
		}
	}
}

// queryDataRanges 查询所有 USDT 交易对的历史数据范围
func queryDataRanges(ctx context.Context, cfg *config.Config) error {
	fmt.Println("🔍 正在查询币安USDT交易对的历史数据范围...")
	fmt.Println()

	// 检查是否需要输出到文件
	var outputFile *os.File
	var fileWriter *bufio.Writer
	if *output != "" {
		var err error
		outputFile, err = os.Create(*output)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer outputFile.Close()
		fileWriter = bufio.NewWriter(outputFile)
		defer fileWriter.Flush()
		fmt.Printf("📁 结果将保存到文件: %s\n\n", *output)
	}

	// 初始化组件
	comps, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer comps.cleanup()

	// 获取所有USDT交易对
	fmt.Println("📡 从币安数据页面获取所有USDT交易对...")
	allSymbols, err := comps.downloader.GetAllSymbolsFromBinance(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbols from Binance: %w", err)
	}

	// 如果指定了特定符号，只处理这些符号
	var targetSymbols []string
	if *symbols != "" {
		targetSymbols = strings.Split(*symbols, ",")
		for i, symbol := range targetSymbols {
			targetSymbols[i] = strings.TrimSpace(strings.ToUpper(symbol))
			// 如果没有USDT后缀，自动添加
			if !strings.HasSuffix(targetSymbols[i], "USDT") {
				targetSymbols[i] += "USDT"
			}
		}
		fmt.Printf("🎯 查询指定的 %d 个交易对\n\n", len(targetSymbols))
	} else {
		targetSymbols = allSymbols
		fmt.Printf("✅ 发现 %d 个USDT交易对\n\n", len(allSymbols))
	}

	// 查询每个交易对的数据范围
	fmt.Println("📊 正在查询数据范围...")
	fmt.Println()

	type rangeResult struct {
		symbol    string
		startDate string
		endDate   string
		days      int
		err       error
	}

	results := make([]rangeResult, 0, len(targetSymbols))

	for i, symbol := range targetSymbols {
		// 显示进度
		fmt.Printf("\r[%d/%d] 查询 %s...", i+1, len(targetSymbols), symbol)

		timeline, err := comps.downloader.GetSymbolTimeline(ctx, symbol)
		if err != nil {
			results = append(results, rangeResult{
				symbol: symbol,
				err:    err,
			})
			continue
		}

		startDate := timeline.HistoricalStartDate.Format("2006-01-02")
		endDate := timeline.LatestAvailableDate.Format("2006-01-02")

		// 计算天数差
		days := int(timeline.LatestAvailableDate.Sub(timeline.HistoricalStartDate).Hours() / 24)

		results = append(results, rangeResult{
			symbol:    symbol,
			startDate: startDate,
			endDate:   endDate,
			days:      days,
		})
	}

	// 清除进度行
	fmt.Printf("\r%s\r", strings.Repeat(" ", 50))

	// 输出结果
	fmt.Println("📈 数据范围查询结果:")
	fmt.Println(strings.Repeat("=", 60))

	// 如果有文件输出，写入文件头部
	if fileWriter != nil {
		fileWriter.WriteString("# 币安USDT交易对历史数据范围查询结果\n")
		fileWriter.WriteString(fmt.Sprintf("# 查询时间: %s\n", time.Now().Format("2006-01-02 15:04:05")))
		fileWriter.WriteString("# 格式: 交易对: 开始时间 to 结束时间 (天数)\n")
		fileWriter.WriteString(strings.Repeat("=", 60) + "\n")
	}

	successCount := 0
	for _, result := range results {
		if result.err != nil {
			outputLine := fmt.Sprintf("%-12s: ❌ 查询失败 - %v", result.symbol, result.err)
			fmt.Println(outputLine)
			if fileWriter != nil {
				fileWriter.WriteString(fmt.Sprintf("%-12s: 查询失败 - %v\n", result.symbol, result.err))
			}
		} else {
			outputLine := fmt.Sprintf("%-12s: %s to %s (%d days)",
				result.symbol, result.startDate, result.endDate, result.days)
			fmt.Println(outputLine)
			if fileWriter != nil {
				fileWriter.WriteString(outputLine + "\n")
			}
			successCount++
		}
	}

	// 输出统计信息
	summaryLine := fmt.Sprintf("\n✅ 查询完成！成功查询 %d/%d 个交易对", successCount, len(results))
	fmt.Println(summaryLine)
	if fileWriter != nil {
		fileWriter.WriteString(fmt.Sprintf("\n# 查询完成！成功查询 %d/%d 个交易对\n", successCount, len(results)))
		fileWriter.Flush()
		fmt.Printf("💾 结果已保存到文件: %s\n", *output)
	}

	return nil
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\n%s - Download and process Binance K-line data\n\n", appName)
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  run        - Run the data loader (default)\n")
		fmt.Fprintf(os.Stderr, "  validate   - Validate existing data\n")
		fmt.Fprintf(os.Stderr, "  init-db    - Initialize database tables\n")
		fmt.Fprintf(os.Stderr, "  create-views - Create materialized views\n")
		fmt.Fprintf(os.Stderr, "  status     - Show download status\n")
		fmt.Fprintf(os.Stderr, "  discover   - Discover symbol timelines\n")
		fmt.Fprintf(os.Stderr, "  range-query - Query historical data ranges\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  %s -cmd=run -start=2024-01-01 -end=2024-01-31\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=status -detailed\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=validate -symbols=BTCUSDT,ETHUSDT\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=discover -symbols=BTCUSDT -detailed\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=range-query\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=range-query -symbols=BTCUSDT,ETHUSDT\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=range-query -symbols=BTC\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=range-query -output=ranges.txt\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=range-query -symbols=BTCUSDT -output=btc_range.txt\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=init-db\n", os.Args[0])
	}
}
