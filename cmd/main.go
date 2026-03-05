package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
	"binance-data-loader/internal/signal"
	"binance-data-loader/internal/state"
	"binance-data-loader/pkg/binance"
	"binance-data-loader/pkg/clickhouse"
	"binance-data-loader/pkg/csvexport"
	"binance-data-loader/pkg/gaps"
	"binance-data-loader/pkg/importer"
	"binance-data-loader/pkg/monitor"
	"binance-data-loader/pkg/parser"
	"binance-data-loader/pkg/quality"
	"binance-data-loader/pkg/reports"
	"binance-data-loader/pkg/scheduler"
	"binance-data-loader/pkg/verification"
	"binance-data-loader/pkg/webmonitor"
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
    command    = flag.String("cmd", "run", "Command to execute: run, validate, init-db, create-views, populate-views, status, discover, update-latest, range-query, list-symbols, update-ranges, check-quality, export-csv, backfill-gaps")
    symbols    = flag.String("symbols", "", "Comma-separated list of symbols to process (optional)")
    endDate    = flag.String("end", "", "End date (YYYY-MM-DD)")
    output     = flag.String("output", "", "Output file path for range-query results (optional)")
    verbose    = flag.Bool("verbose", false, "Enable verbose logging")
    version    = flag.Bool("version", false, "Show version information")
    detailed   = flag.Bool("detailed", false, "Show detailed status information")
    startDate  = flag.String("start", "", "Start date for quality check (YYYY-MM-DD)")
    format     = flag.String("format", "console", "Output format for quality check: console, json, csv, markdown")
    interval   = flag.String("interval", "1m", "Time interval for CSV export: 1m, 5m, 15m, 1h, 4h, 1d")
    stream     = flag.Bool("stream", false, "Stream per-symbol verification output as each completes")
    dryRun     = flag.Bool("dry-run", false, "Preview gaps without downloading data")
    force      = flag.Bool("force", false, "Force redownload of existing data")
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

	// 创建信号处理器
	signalHandler := signal.NewSignalHandler(10*time.Second, log)
	
	// 启动信号处理
	if err := signalHandler.Start(context.Background()); err != nil {
		log.Error().Err(err).Msg("Failed to start signal handler")
		fmt.Fprintf(os.Stderr, "Failed to start signal handler: %v\n", err)
		os.Exit(1)
	}
	defer signalHandler.Stop()

	// 获取信号处理器的上下文
	ctx := signalHandler.GetContext()

	// 执行命令
	if err := executeCommand(ctx, cfg, *command); err != nil {
		// 检查是否是因为上下文取消导致的错误
		if err == context.Canceled {
			log.Info().Msg("Application was cancelled by user")
			fmt.Println("✅ 系统已成功停止")
			os.Exit(0)
		} else if err == context.DeadlineExceeded {
			log.Warn().Msg("Application was cancelled due to timeout")
			fmt.Println("⏰ 系统因超时被停止")
			os.Exit(1)
		} else {
			log.Error().Err(err).Str("command", *command).Msg("Command execution failed")
			fmt.Printf("❌ 系统执行失败: %v\n", err)
			os.Exit(1)
		}
	}

	log.Info().Msg("Application completed successfully")
	fmt.Println("🎉 系统正常完成所有任务")
}

func executeCommand(ctx context.Context, cfg *config.Config, cmd string) error {
	switch cmd {
	case "run":
		return runDataLoader(ctx, cfg)
	case "update-latest":
		return updateToLatest(ctx, cfg)
	case "validate":
		return validateData(ctx, cfg)
	case "verify-data":
		return verifyData(ctx, cfg)
	case "init-db":
		return initializeDatabase(ctx, cfg)
	case "create-views":
		return createMaterializedViews(ctx, cfg)
	case "populate-views":
		return populateMaterializedViews(ctx, cfg)
	case "status":
		return showStatus(ctx, cfg)
	case "discover":
		return discoverSymbols(ctx, cfg)
	case "range-query":
		return queryDataRanges(ctx, cfg)
	case "list-symbols":
		return listSymbols(ctx, cfg)
	case "update-ranges":
		return updateTimelineRanges(ctx, cfg)
	case "check-quality":
		return checkDataQuality(ctx, cfg)
	case "export-csv":
		return exportCSV(ctx, cfg)
	case "backfill-gaps":
		return backfillGaps(ctx, cfg)
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

func runDataLoader(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("data_loader")
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

	// 获取符号列表和时间线
	symbols, timelines, err := getSymbolList(ctx, components.downloader, components.stateManager, cfg)
	if err != nil {
		return fmt.Errorf("failed to get symbols and timelines: %w", err)
	}

	// 启动Web监控服务
	if components.webMonitor != nil && cfg.Monitoring.WebDashboard.AutoStart {
		if err := components.webMonitor.Start(ctx); err != nil {
			log.Warn().Err(err).Msg("Failed to start web monitor")
		}
	}

	// 显示启动概览
	if cfg.Scheduler.ShowStartupOverview {
		showStartupOverview(symbols, timelines)
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

	// 运行并发调度器（如果有RunConcurrent方法）
	// 这里我们使用RunWithSymbols方法来保持一致
	if err := scheduler.RunWithSymbols(ctx, symbols, endDateTime); err != nil {
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
	symbolList, _, err := getSymbolList(ctx, components.downloader, components.stateManager, cfg)
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

func populateMaterializedViews(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("mv_populator")
	log.Info().Msg("Populating materialized views with historical data")

	// 创建ClickHouse仓库
	repository, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to create repository: %w", err)
	}
	defer repository.Close()

	// 获取要填充的时间间隔
	intervals := cfg.MaterializedViews.Intervals
	if len(intervals) == 0 {
		intervals = []string{"5m", "15m", "1h", "4h", "1d"}
	}

	// 解析symbols参数
	var symbolList []string
	if *symbols != "" {
		symbolList = strings.Split(strings.TrimSpace(*symbols), ",")
		// 清理空白字符
		for i, symbol := range symbolList {
			symbolList[i] = strings.TrimSpace(symbol)
		}
		log.Info().Strs("symbols", symbolList).Msg("Filtering specific symbols")
	}

	log.Info().Strs("intervals", intervals).Msg("Starting to populate materialized views")

	// 调用repository方法填充历史数据
	if err := repository.PopulateMaterializedViews(ctx, intervals, symbolList); err != nil {
		return fmt.Errorf("failed to populate materialized views: %w", err)
	}

	log.Info().Msg("Materialized views populated with historical data successfully")
	fmt.Println("🎉 物化视图已成功填充历史数据")
	return nil
}

type components struct {
	downloader       *binance.BinanceDownloader
	parser           *parser.CSVParser
	repository       *clickhouse.Repository
	stateManager     *state.FileStateManager
	progressReporter domain.ProgressReporter
	importer         *importer.Importer
	webMonitor       *webmonitor.WebMonitor
}

func (c *components) cleanup() {
	log := logger.GetLogger("cleanup")
	log.Info().Msg("Starting component cleanup")
	
	// 停止Web监控器（优先级最高，需要最长时间）
	if c.webMonitor != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.webMonitor.Stop(ctx); err != nil {
			log.Warn().Err(err).Msg("Failed to stop web monitor gracefully")
		}
	}
	
	// 关闭导入器
	if c.importer != nil {
		if err := c.importer.Close(); err != nil {
			log.Warn().Err(err).Msg("Failed to close importer")
		}
	}
	
	// 关闭数据库连接（最后关闭）
	if c.repository != nil {
		if err := c.repository.Close(); err != nil {
			log.Warn().Err(err).Msg("Failed to close repository")
		}
	}
	
	log.Info().Msg("Component cleanup completed")
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
	var progressReporter domain.ProgressReporter
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

	// 创建Web监控器
	var webMon *webmonitor.WebMonitor
	if cfg.Monitoring.WebDashboard.Enabled {
		var err error
		webMon, err = webmonitor.NewWebMonitor(cfg.Monitoring.WebDashboard, cfg.Database.ClickHouse, stateManager)
		if err != nil {
			return nil, fmt.Errorf("failed to create web monitor: %w", err)
		}
	}

	return &components{
		downloader:       downloader,
		parser:           parser,
		repository:       repository,
		stateManager:     stateManager,
		progressReporter: progressReporter,
		importer:         importer,
		webMonitor:       webMon,
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

func getSymbolList(ctx context.Context, downloader *binance.BinanceDownloader, stateManager *state.FileStateManager, cfg *config.Config) ([]string, []domain.SymbolTimeline, error) {
	log := logger.GetLogger("symbol_list")
	
	if *symbols != "" {
		// 使用命令行指定的交易对
		symbolList := strings.Split(*symbols, ",")
		var timelines []domain.SymbolTimeline
		
		// 为指定的交易对获取或创建时间线
		for _, symbol := range symbolList {
			symbol = strings.TrimSpace(strings.ToUpper(symbol))
			timeline, err := getOrCreateTimeline(ctx, symbol, downloader, stateManager, cfg)
			if err != nil {
				log.Warn().Str("symbol", symbol).Err(err).Msg("Failed to get timeline for specified symbol")
				continue
			}
			timelines = append(timelines, *timeline)
		}
		
		return symbolList, timelines, nil
	}

	// 如果启用了自动发现，优先从本地缓存获取
	if cfg.Scheduler.AutoDiscoverSymbols {
		existingTimelines, err := stateManager.GetAllTimelines()
		if err == nil && len(existingTimelines) > 0 {
			// 检查缓存是否仍然有效
			cacheValid := true
			var timelineList []domain.SymbolTimeline
			
			for _, timeline := range existingTimelines {
				if time.Since(timeline.LastUpdated) > cfg.Scheduler.TimelineCacheDuration {
					cacheValid = false
					break
				}
				timelineList = append(timelineList, *timeline)
			}
			
			if cacheValid {
				symbols := make([]string, 0, len(existingTimelines))
				for symbol := range existingTimelines {
					symbols = append(symbols, symbol)
				}
				log.Info().
					Int("cached_symbols", len(symbols)).
					Dur("cache_age", time.Since(timelineList[0].LastUpdated)).
					Msg("Using cached symbol list and timelines")
				
				return symbols, timelineList, nil
			}
		}
	}

	// 缓存不存在或已过期，从币安获取新的交易对列表
	log.Info().Msg("Fetching fresh symbol list from Binance...")
	symbols, err := downloader.GetSymbols(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get symbols from Binance: %w", err)
	}

	// 获取每个交易对的时间线并缓存
	var timelines []domain.SymbolTimeline
	for _, symbol := range symbols {
		timeline, err := getOrCreateTimeline(ctx, symbol, downloader, stateManager, cfg)
		if err != nil {
			log.Warn().Str("symbol", symbol).Err(err).Msg("Failed to get timeline")
			continue
		}
		timelines = append(timelines, *timeline)
	}

	log.Info().
		Int("fresh_symbols", len(symbols)).
		Int("timelines_fetched", len(timelines)).
		Msg("Fetched fresh symbol list and timelines from Binance")

	return symbols, timelines, nil
}

// getOrCreateTimeline 获取或创建交易对的时间线
func getOrCreateTimeline(ctx context.Context, symbol string, downloader *binance.BinanceDownloader, stateManager *state.FileStateManager, cfg *config.Config) (*domain.SymbolTimeline, error) {
	// 先尝试从本地获取
	if timeline, err := stateManager.GetTimeline(symbol); err == nil && timeline != nil {
		// 检查时间线是否需要更新
		if time.Since(timeline.LastUpdated) <= cfg.Scheduler.TimelineCacheDuration {
			return timeline, nil
		}
	}

	// 从币安获取最新时间线
	timeline, err := downloader.GetSymbolTimeline(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get timeline for %s: %w", symbol, err)
	}

	// 保存到状态管理器
	if err := stateManager.SaveTimeline(timeline); err != nil {
		// 不要因为保存失败而终止，只记录警告
		timelineLog := logger.GetLogger("timeline")
		timelineLog.Warn().
			Str("symbol", symbol).
			Err(err).
			Msg("Failed to save timeline to state manager")
	}

	return timeline, nil
}

// showStartupOverview 显示启动概览信息
func showStartupOverview(symbols []string, timelines []domain.SymbolTimeline) {
	fmt.Printf("\n")
	fmt.Printf("🚀 === Binance 数据加载器启动概览 ===\n")
	fmt.Printf("\n")
	
	if len(timelines) == 0 {
		fmt.Printf("📋 将处理的交易对: %d 个\n", len(symbols))
		for i, symbol := range symbols {
			if i < 10 { // 只显示前10个
				fmt.Printf("   • %s\n", symbol)
			} else if i == 10 {
				fmt.Printf("   • ... 还有 %d 个交易对\n", len(symbols)-10)
				break
			}
		}
		fmt.Printf("\n")
		return
	}

	// 按总月份数排序时间线
	sort.Slice(timelines, func(i, j int) bool {
		return timelines[i].TotalMonths > timelines[j].TotalMonths
	})

	// 统计信息
	totalMonths := 0
	completedMonths := 0
	earliestDate := time.Now()
	latestDate := time.Time{}
	
	for _, timeline := range timelines {
		totalMonths += timeline.TotalMonths
		completedMonths += timeline.ImportedMonthsCount
		if timeline.HistoricalStartDate.Before(earliestDate) {
			earliestDate = timeline.HistoricalStartDate
		}
		if timeline.LatestAvailableDate.After(latestDate) {
			latestDate = timeline.LatestAvailableDate
		}
	}

	fmt.Printf("📊 数据概览:\n")
	fmt.Printf("   • 交易对数量: %d\n", len(timelines))
	fmt.Printf("   • 总数据月份: %d 个月\n", totalMonths)
	fmt.Printf("   • 已完成月份: %d 个月 (%.1f%%)\n", completedMonths, float64(completedMonths)*100/float64(totalMonths))
	fmt.Printf("   • 时间范围: %s 至 %s\n", earliestDate.Format("2006-01"), latestDate.Format("2006-01"))
	fmt.Printf("\n")

	fmt.Printf("🏆 主要交易对 (按数据量排序):\n")
	displayCount := min(len(timelines), 10)
	for i := 0; i < displayCount; i++ {
		timeline := timelines[i]
		progress := float64(timeline.ImportedMonthsCount) * 100 / float64(timeline.TotalMonths)
		progressBar := generateProgressBar(progress, 20)
		
		fmt.Printf("   %-12s %s %3.0f%% (%2d/%2d月) %s-%s\n",
			timeline.Symbol,
			progressBar,
			progress,
			timeline.ImportedMonthsCount,
			timeline.TotalMonths,
			timeline.HistoricalStartDate.Format("2006-01"),
			timeline.LatestAvailableDate.Format("2006-01"))
	}
	
	if len(timelines) > 10 {
		fmt.Printf("   ... 还有 %d 个交易对\n", len(timelines)-10)
	}
	
	fmt.Printf("\n")
	fmt.Printf("💡 提示: 使用 'go run cmd/main.go -cmd=status -detailed' 查看详细状态\n")
	fmt.Printf("\n")
}

// generateProgressBar 生成进度条
func generateProgressBar(progress float64, width int) string {
	filled := int(progress * float64(width) / 100)
	bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	return fmt.Sprintf("[%s]", bar)
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
			fmt.Printf(" ⚠️  状态保存失败: %v\n", err)
		}
		
		// 保存交易对信息到数据库
		symbolInfo := &domain.SymbolInfo{
			Symbol:       timeline.Symbol,
			Status:       "TRADING",
			EarliestDate: timeline.HistoricalStartDate,
			LatestDate:   timeline.LatestAvailableDate,
			TotalMonths:  int32(timeline.TotalMonths),
			DataStatus:   "discovered",
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}
		
		if err := comps.repository.SaveSymbolInfo(ctx, symbolInfo); err != nil {
			fmt.Printf(" ⚠️  数据库保存失败: %v", err)
		}
		
		fmt.Printf(" ✅ 完成 (%d个月)\n", timeline.TotalMonths)
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
		fmt.Fprintf(os.Stderr, "  run        - Run the concurrent data loader (default)\n")
		fmt.Fprintf(os.Stderr, "  validate   - Validate existing data\n")
		fmt.Fprintf(os.Stderr, "  init-db    - Initialize database tables\n")
		fmt.Fprintf(os.Stderr, "  create-views - Create materialized views\n")
		fmt.Fprintf(os.Stderr, "  populate-views - Populate materialized views with historical data\n")
		fmt.Fprintf(os.Stderr, "  status     - Show download status\n")
		fmt.Fprintf(os.Stderr, "  discover   - Discover symbol timelines\n")
		fmt.Fprintf(os.Stderr, "  update-latest - Update to latest data\n")
		fmt.Fprintf(os.Stderr, "  range-query - Query historical data ranges\n")
		fmt.Fprintf(os.Stderr, "  list-symbols - List symbols in database\n")
		fmt.Fprintf(os.Stderr, "  update-ranges - Update timeline ranges for symbols\n")
		fmt.Fprintf(os.Stderr, "  export-csv - Export K-line data to CSV file\n")
		fmt.Fprintf(os.Stderr, "  backfill-gaps - Detect and fill historical data gaps\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s                                      # Run with defaults\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=run -end=2024-01-31            # Run until specific date\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=status -detailed               # Show detailed status\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=validate -symbols=BTCUSDT,ETHUSDT\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=discover -symbols=BTCUSDT -detailed\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=range-query -symbols=BTCUSDT -output=ranges.txt\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=list-symbols                   # List symbols in database\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=update-ranges                  # Update all symbol ranges\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=update-ranges -symbols=BTCUSDT # Update specific symbol ranges\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=check-quality                  # Check data quality for all symbols\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=check-quality BTCUSDT ETHUSDT  # Check specific symbols\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=check-quality -format=json     # Export quality report as JSON\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=check-quality -start=2023-01-01 -end=2023-12-31\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=export-csv -symbols=BTCUSDT -interval=1m # Export BTCUSDT 1m data\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=export-csv -interval=5m -output=data.csv # Export all symbols 5m data\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=export-csv -symbols=ETHUSDT -start=2023-01-01 -end=2023-12-31\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=backfill-gaps                  # Detect and fill all historical gaps\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=backfill-gaps -symbols=BTCUSDT # Fill gaps for specific symbol\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=backfill-gaps -dry-run         # Preview gaps without downloading\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=backfill-gaps -start=2020-01 -end=2024-12 # Fill gaps in range\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=backfill-gaps -force           # Force redownload existing data\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=init-db                        # Initialize database\n", os.Args[0])
	}
}

// listSymbols 列出数据库中的所有交易对信息
func listSymbols(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("list_symbols")
	log.Info().Msg("Listing symbols from database")
	
	// 初始化组件
	comps, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer comps.cleanup()
	
	// 获取所有交易对信息
	symbolInfos, err := comps.repository.GetAllSymbolInfos(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbol infos: %w", err)
	}
	
	if len(symbolInfos) == 0 {
		fmt.Println("🔍 数据库中没有找到交易对信息")
		fmt.Println("💡 提示: 请先使用 'go run cmd/main.go -cmd=discover' 发现交易对")
		return nil
	}
	
	fmt.Println("📊 数据库中的交易对信息:")
	fmt.Println("================================================================================")
	fmt.Printf("%-12s %-8s %-6s %-6s %-12s %-12s %-6s %-10s\n", 
		"交易对", "状态", "基础", "报价", "最早日期", "最新日期", "月数", "数据状态")
	fmt.Println("--------------------------------------------------------------------------------")
	
	for _, info := range symbolInfos {
		fmt.Printf("%-12s %-8s %-6s %-6s %-12s %-12s %-6d %-10s\n",
			info.Symbol,
			info.Status,
			info.BaseAsset,
			info.QuoteAsset,
			info.EarliestDate.Format("2006-01-02"),
			info.LatestDate.Format("2006-01-02"),
			int(info.TotalMonths),
			info.DataStatus,
		)
	}
	
	fmt.Println("================================================================================")
	fmt.Printf("🎉 总计: %d 个交易对\n", len(symbolInfos))
	
	return nil
}

// updateTimelineRanges 更新所有交易对的时间范围信息
func updateTimelineRanges(ctx context.Context, cfg *config.Config) error {
	fmt.Println("📅 更新交易对时间范围信息...")
	fmt.Println()
	
	log := logger.GetLogger("update_ranges")
	log.Info().Msg("Starting timeline ranges update")
	
	// 初始化组件
	comps, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer comps.cleanup()
	
	// 获取数据库中所有已存储的交易对
	fmt.Println("🔍 获取数据库中的所有交易对信息...")
	symbolInfos, err := comps.repository.GetAllSymbolInfos(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbol infos: %w", err)
	}
	
	if len(symbolInfos) == 0 {
		fmt.Println("⚠️  数据库中没有找到交易对信息")
		fmt.Println("💡 提示: 请先使用 'go run cmd/main.go -cmd=discover' 发现交易对")
		return nil
	}
	
	fmt.Printf("✅ 发现 %d 个交易对需要更新时间范围\n\n", len(symbolInfos))
	
	// 如果指定了特定符号，只处理这些符号
	var targetSymbols []*domain.SymbolInfo
	if *symbols != "" {
		requestedSymbols := strings.Split(*symbols, ",")
		symbolMap := make(map[string]bool)
		for _, symbol := range requestedSymbols {
			symbolMap[strings.TrimSpace(strings.ToUpper(symbol))] = true
		}
		
		for _, info := range symbolInfos {
			if symbolMap[info.Symbol] {
				targetSymbols = append(targetSymbols, info)
			}
		}
		
		if len(targetSymbols) == 0 {
			return fmt.Errorf("未找到指定的交易对: %s", *symbols)
		}
		
		fmt.Printf("🎯 只更新指定的 %d 个交易对\n\n", len(targetSymbols))
	} else {
		targetSymbols = symbolInfos
	}
	
	// 更新每个交易对的时间范围
	fmt.Println("📊 正在更新交易对时间范围...")
	updatedCount := 0
	errorCount := 0
	
	for i, oldInfo := range targetSymbols {
		fmt.Printf("[%d/%d] 更新 %s...", i+1, len(targetSymbols), oldInfo.Symbol)
		
		// 获取最新的时间线信息
		timeline, err := comps.downloader.GetSymbolTimeline(ctx, oldInfo.Symbol)
		if err != nil {
			fmt.Printf(" ❌ 失败: %v\n", err)
			log.Error().Err(err).Str("symbol", oldInfo.Symbol).Msg("Failed to get timeline")
			errorCount++
			continue
		}
		
		// 检查是否有更新
		hasUpdates := false
		newInfo := *oldInfo // 复制原有信息
		
		// 更新最新日期
		if !timeline.LatestAvailableDate.Equal(oldInfo.LatestDate) {
			hasUpdates = true
			newInfo.LatestDate = timeline.LatestAvailableDate
		}
		
		// 更新总月数
		if int32(timeline.TotalMonths) != oldInfo.TotalMonths {
			hasUpdates = true
			newInfo.TotalMonths = int32(timeline.TotalMonths)
		}
		
		// 更新最早日期（虽然通常不会变，但为了保险起见）
		if !timeline.HistoricalStartDate.Equal(oldInfo.EarliestDate) {
			hasUpdates = true
			newInfo.EarliestDate = timeline.HistoricalStartDate
		}
		
		if hasUpdates {
			// 设置更新时间
			newInfo.UpdatedAt = time.Now()
			
			// 保存到数据库
			if err := comps.repository.UpdateSymbolInfo(ctx, &newInfo); err != nil {
				fmt.Printf(" ❌ 数据库更新失败: %v\n", err)
				log.Error().Err(err).Str("symbol", oldInfo.Symbol).Msg("Failed to update symbol info")
				errorCount++
				continue
			}
			
			// 保存时间线到状态管理器
			if err := comps.stateManager.SaveTimeline(timeline); err != nil {
				log.Warn().Err(err).Str("symbol", oldInfo.Symbol).Msg("Failed to save timeline to state manager")
			}
			
			fmt.Printf(" ✅ 已更新 (%d个月 -> %d个月)\n", 
				oldInfo.TotalMonths, timeline.TotalMonths)
			updatedCount++
		} else {
			fmt.Printf(" ⏭️  无需更新 (%d个月)\n", timeline.TotalMonths)
		}
		
		// 检查上下文是否被取消
		select {
		case <-ctx.Done():
			fmt.Printf("\n⚠️  操作被用户取消\n")
			return ctx.Err()
		default:
		}
	}
	
	fmt.Println()
	fmt.Println("🎉 时间范围更新完成！")
	fmt.Printf("   ✅ 成功更新: %d 个交易对\n", updatedCount)
	fmt.Printf("   ⏭️  无需更新: %d 个交易对\n", len(targetSymbols)-updatedCount-errorCount)
	if errorCount > 0 {
		fmt.Printf("   ❌ 更新失败: %d 个交易对\n", errorCount)
	}
	fmt.Println()
	
	log.Info().
		Int("total", len(targetSymbols)).
		Int("updated", updatedCount).
		Int("errors", errorCount).
		Msg("Timeline ranges update completed")
		
	return nil
}

// checkDataQuality 执行数据质量检查
func checkDataQuality(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("main")
	
	// 初始化组件
	comps, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer comps.cleanup()
	
	// 解析命令行参数
	symbols := []string{}
	if len(flag.Args()) > 1 {
		// 如果指定了交易对，使用指定的交易对
		symbols = flag.Args()[1:]
	}
	
	// 解析开始日期（支持YYYY-MM-DD或YYYY-MM格式）
	var startDatePtr *time.Time
	if *startDate != "" {
		parsed, err := parseFlexibleDate(*startDate, true)
		if err != nil {
			return fmt.Errorf("invalid start date format (expected YYYY-MM-DD or YYYY-MM): %w", err)
		}
		startDatePtr = &parsed
	}
	
	// 解析结束日期（支持YYYY-MM-DD或YYYY-MM格式）
	var endDatePtr *time.Time
	if *endDate != "" {
		parsed, err := parseFlexibleDate(*endDate, false)
		if err != nil {
			return fmt.Errorf("invalid end date format (expected YYYY-MM-DD or YYYY-MM): %w", err)
		}
		endDatePtr = &parsed
	}
	
	// 创建质量检查器
	checker := quality.NewQualityChecker(comps.repository, comps.downloader)
	reporter := quality.NewReporter()
	
	fmt.Println("🔍 开始数据质量检查...")
	fmt.Println()
	
	// 如果没有指定交易对，获取所有交易对
	if len(symbols) == 0 {
		log.Info().Msg("No symbols specified, getting all available symbols")
		allSymbols, err := checker.GetAllSymbols(ctx)
		if err != nil {
			return fmt.Errorf("failed to get all symbols: %w", err)
		}
		symbols = allSymbols
		fmt.Printf("📊 将检查 %d 个交易对的数据质量\n\n", len(symbols))
	} else {
		fmt.Printf("📊 将检查指定的 %d 个交易对: %s\n\n", len(symbols), strings.Join(symbols, ", "))
	}
	
	if len(symbols) == 0 {
		fmt.Println("⚠️  没有找到可检查的交易对")
		return nil
	}
	
	// 创建质量检查请求
	request := &quality.QualityCheckRequest{
		Symbols:   symbols,
		StartDate: startDatePtr,
		EndDate:   endDatePtr,
		CheckMode: quality.CheckModeStandard,
	}
	
	// 验证请求
	if err := checker.ValidateRequest(request); err != nil {
		return fmt.Errorf("invalid quality check request: %w", err)
	}
	
	// 执行批量质量检查
	batchReport, err := checker.CheckBatchQuality(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to check batch quality: %w", err)
	}
	
	// 根据输出格式生成报告
	switch *format {
	case "json":
		if err := reporter.WriteJSONReport(os.Stdout, batchReport); err != nil {
			return fmt.Errorf("failed to write JSON report: %w", err)
		}
	case "csv":
		csvReport := reporter.GenerateCSVReport(batchReport.Reports)
		fmt.Print(csvReport)
	case "markdown":
		markdownReport := reporter.GenerateMarkdownReport(batchReport)
		fmt.Print(markdownReport)
	case "table":
		tableReport := reporter.GenerateSummaryTable(batchReport.Reports)
		fmt.Print(tableReport)
	default: // console
		consoleReport := reporter.GenerateBatchConsoleReport(batchReport)
		fmt.Print(consoleReport)
	}
	
	log.Info().
		Int("total_symbols", len(symbols)).
		Int("checked_symbols", batchReport.CheckedSymbols).
		Float64("average_score", batchReport.Summary.AverageScore).
		Str("format", *format).
		Msg("Data quality check completed")
	
	return nil
}

// verifyData 执行数据验证（基于数据库的完整性检查）
func verifyData(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("verify_data")
	log.Info().Msg("Starting data verification")
	
	// 记录开始时间用于报告
	verificationStartTime := time.Now()
	
	// 初始化组件
	comps, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer comps.cleanup()
	
	// 解析交易对列表
	var symbolList []string
	if *symbols == "" {
		// 如果没有指定symbols，获取数据库中所有交易对
		log.Info().Msg("No symbols specified, fetching all symbols from database")
		symbolInfos, err := comps.repository.GetAllSymbolInfos(ctx)
		if err != nil {
			return fmt.Errorf("failed to get all symbols from database: %w", err)
		}
		
		if len(symbolInfos) == 0 {
			return fmt.Errorf("no symbols found in database, please run data import first")
		}
		
		// 提取symbol名称
		symbolList = make([]string, len(symbolInfos))
		for i, info := range symbolInfos {
			symbolList[i] = info.Symbol
		}
		
		log.Info().Int("symbols_count", len(symbolList)).Msg("Found symbols in database for verification")
	} else {
		// 解析用户指定的交易对列表
		symbolList = parseSymbolsParameter(*symbols)
		if len(symbolList) == 0 {
			return fmt.Errorf("no valid symbols provided")
		}
	}
	
	// 解析开始日期（支持YYYY-MM-DD或YYYY-MM格式）
	var startDatePtr *time.Time
	if *startDate != "" {
		parsed, err := parseFlexibleDate(*startDate, true)
		if err != nil {
			return fmt.Errorf("invalid start date format (expected YYYY-MM-DD or YYYY-MM): %w", err)
		}
		startDatePtr = &parsed
	}
	
	// 解析结束日期（支持YYYY-MM-DD或YYYY-MM格式）
	var endDatePtr *time.Time
	if *endDate != "" {
		parsed, err := parseFlexibleDate(*endDate, false)
		if err != nil {
			return fmt.Errorf("invalid end date format (expected YYYY-MM-DD or YYYY-MM): %w", err)
		}
		endDatePtr = &parsed
	}
	
	// 注意：不再设置默认时间范围，而是基于数据库实际数据范围
	
	fmt.Printf("🔍 开始验证数据质量...\n")
	if *symbols == "" {
		fmt.Printf("📊 交易对: 所有交易对 (共%d个)\n", len(symbolList))
	} else {
		fmt.Printf("📊 交易对: %s\n", strings.Join(symbolList, ", "))
	}
	if startDatePtr != nil && endDatePtr != nil {
		fmt.Printf("📅 时间范围: %s 到 %s\n", startDatePtr.Format("2006-01-02"), endDatePtr.Format("2006-01-02"))
	} else {
		fmt.Printf("📅 时间范围: 基于数据库实际数据范围\n")
	}
	fmt.Println()
	
	// 创建数据库验证检查器（新的基于数据库的验证逻辑）
	databaseChecker := verification.NewDatabaseVerificationChecker(comps.repository)
	
    var batchReport *verification.BatchDatabaseVerificationReport

    if *stream {
        fmt.Println("⏱️ 启用流式输出：每个交易对完成后立即显示结果\n")
        reporter := verification.NewDatabaseReporter()

        // 逐个验证并即时输出
        symbolReports := make([]*verification.DatabaseVerificationReport, 0, len(symbolList))
        validReports := 0
        var totalScore float64

        for idx, sym := range symbolList {
            fmt.Printf("(%d/%d) 验证 %s ...\n", idx+1, len(symbolList), sym)
            rep, rerr := databaseChecker.VerifySymbolData(ctx, sym, startDatePtr, endDatePtr)
            if rerr != nil {
                fmt.Printf("❌ %s 验证失败: %v\n\n", sym, rerr)
                // 即使失败也附加一个空报告以占位
                rep = &verification.DatabaseVerificationReport{
                    Symbol:     sym,
                    DataRange:  &verification.SymbolDataRange{Symbol: sym, HasData: false},
                    GeneratedAt: time.Now(),
                }
            }

            // 即时输出该交易对的报告
            fmt.Print(reporter.GenerateSymbolConsoleReport(rep))

            // 汇总统计
            if rep != nil {
                symbolReports = append(symbolReports, rep)
                if rep.DataRange != nil && rep.DataRange.HasData {
                    validReports++
                    totalScore += rep.QualityScore
                }
            }
        }

        avg := 0.0
        if validReports > 0 {
            avg = totalScore / float64(validReports)
        }

        batchReport = &verification.BatchDatabaseVerificationReport{
            Reports:             symbolReports,
            TotalSymbols:        len(symbolList),
            VerifiedSymbols:     validReports,
            AverageCompleteness: avg,
            GeneratedAt:         time.Now(),
        }
    } else {
        // 执行批量数据验证
        batchReport, err = databaseChecker.VerifyBatchSymbols(ctx, symbolList, startDatePtr, endDatePtr)
        if err != nil {
            return fmt.Errorf("failed to verify data: %w", err)
        }
    }
	
	// 根据详细模式输出结果
    if *detailed {
        // 详细模式：输出JSON格式
        reporter := verification.NewDatabaseReporter()
        if err := reporter.WriteJSONReport(os.Stdout, batchReport); err != nil {
            return fmt.Errorf("failed to write detailed report: %w", err)
        }
    } else {
        reporter := verification.NewDatabaseReporter()
        if *stream {
            // 流式输出模式下已逐个输出，这里给出简要汇总表
            fmt.Println("=== 验证汇总表 ===")
            fmt.Print(reporter.GenerateSummaryTable(batchReport.Reports))
        } else {
            // 简洁模式：输出用户友好的摘要
            consoleReport := reporter.GenerateBatchConsoleReport(batchReport)
            fmt.Print(consoleReport)
        }

        // 添加验证结论
        fmt.Println("\n📋 验证结论:")
        if batchReport.AverageCompleteness >= 95.0 {
            fmt.Println("✅ 数据完整性优秀，无需特别关注")
		} else if batchReport.AverageCompleteness >= 85.0 {
			fmt.Println("⚠️  数据完整性良好，建议关注部分问题")
		} else if batchReport.AverageCompleteness >= 70.0 {
			fmt.Println("🔶 数据完整性一般，需要改进")
		} else {
			fmt.Println("❌ 数据完整性较差，需要立即处理")
		}
	}
	
	log.Info().
		Int("total_symbols", len(symbolList)).
		Int("verified_symbols", batchReport.VerifiedSymbols).
		Float64("average_score", batchReport.AverageCompleteness).
		Bool("detailed", *detailed).
		Msg("Data verification completed")
	
	// 生成详细报告文档
	if err := generateVerificationReport(batchReport, verificationStartTime); err != nil {
		log.Warn().Err(err).Msg("Failed to generate verification report, but verification completed successfully")
		fmt.Printf("\n⚠️  报告生成失败: %v\n", err)
	}
	
	return nil
}

// generateVerificationReport 生成验证报告文档
func generateVerificationReport(batchReport *verification.BatchDatabaseVerificationReport, startTime time.Time) error {
	// 创建报告生成器和适配器
	reportGenerator := reports.NewReportGenerator()
	adapter := reports.NewVerificationResultAdapter()
	
	// 转换验证结果为报告格式
	resultsForReport := adapter.ConvertBatchReport(batchReport)
	executionTime := adapter.ExtractExecutionTime(batchReport, startTime)
	
	// 生成报告文件路径
	reportPath := reports.GenerateDefaultReportPath()
	
	// 生成报告
	actualPath, err := reportGenerator.GenerateVerificationReport(resultsForReport, executionTime, reportPath)
	if err != nil {
		return fmt.Errorf("failed to generate verification report: %w", err)
	}
	
	// 分析报告问题
	criticalSymbols, attentionSymbols := adapter.AnalyzeReportIssues(batchReport)
	
	// 显示报告生成结果
	fmt.Printf("\n📄 详细报告已生成: %s\n", actualPath)
	
	// 生成摘要信息
	reportSummary := adapter.GenerateReportSummary(batchReport, executionTime)
	fmt.Printf("📊 %s\n", reportSummary)
	
	// 显示关键问题提示
	if len(criticalSymbols) > 0 {
		fmt.Printf("\n🚨 严重问题交易对 (%d个): ", len(criticalSymbols))
		if len(criticalSymbols) <= 5 {
			fmt.Printf("%s\n", strings.Join(criticalSymbols, ", "))
		} else {
			fmt.Printf("%s ... 等%d个\n", strings.Join(criticalSymbols[:5], ", "), len(criticalSymbols))
		}
	}
	
	if len(attentionSymbols) > 0 {
		fmt.Printf("⚠️  需要关注交易对 (%d个): ", len(attentionSymbols))
		if len(attentionSymbols) <= 5 {
			fmt.Printf("%s\n", strings.Join(attentionSymbols, ", "))
		} else {
			fmt.Printf("%s ... 等%d个\n", strings.Join(attentionSymbols[:5], ", "), len(attentionSymbols))
		}
	}
	
	if len(criticalSymbols) > 0 || len(attentionSymbols) > 0 {
		fmt.Printf("\n🔍 查看详细报告了解具体问题和修复建议\n")
	} else {
		fmt.Printf("\n🎉 所有交易对数据质量良好！\n")
	}
	
	return nil
}

// parseSymbolsParameter 解析交易对参数
func parseSymbolsParameter(symbolsStr string) []string {
	if symbolsStr == "" {
		return nil
	}
	
	// 按逗号分割并清理空白字符
	parts := strings.Split(symbolsStr, ",")
	var result []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			// 转换为大写（Binance交易对通常是大写）
			result = append(result, strings.ToUpper(part))
		}
	}
	return result
}

// parseFlexibleDate 解析灵活的日期格式（支持YYYY-MM-DD或YYYY-MM）
func parseFlexibleDate(dateStr string, isStartDate bool) (time.Time, error) {
	// 首先尝试完整日期格式 YYYY-MM-DD
	if parsed, err := time.Parse("2006-01-02", dateStr); err == nil {
		return parsed, nil
	}
	
	// 然后尝试月份格式 YYYY-MM
	if parsed, err := time.Parse("2006-01", dateStr); err == nil {
		if isStartDate {
			// 开始日期：使用月份的第一天
			return time.Date(parsed.Year(), parsed.Month(), 1, 0, 0, 0, 0, time.UTC), nil
		} else {
			// 结束日期：使用月份的最后一天
			nextMonth := parsed.AddDate(0, 1, 0)
			lastDay := nextMonth.Add(-24 * time.Hour)
			return time.Date(lastDay.Year(), lastDay.Month(), lastDay.Day(), 23, 59, 59, 0, time.UTC), nil
		}
	}
	
	return time.Time{}, fmt.Errorf("invalid date format, expected YYYY-MM-DD or YYYY-MM")
}

// exportCSV 导出CSV数据
func exportCSV(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("export_csv")
	log.Info().Msg("Starting CSV export")

	// 初始化组件
	components, err := initializeComponents(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}
	defer components.cleanup()

	// 创建CSV导出器
	exporter := csvexport.NewCSVExporter(components.repository)

	// 解析时间范围参数
	var startTimePtr *time.Time
	if *startDate != "" {
		parsed, err := parseFlexibleDate(*startDate, true)
		if err != nil {
			return fmt.Errorf("invalid start date format (expected YYYY-MM-DD or YYYY-MM): %w", err)
		}
		startTimePtr = &parsed
	}

	var endTimePtr *time.Time
	if *endDate != "" {
		parsed, err := parseFlexibleDate(*endDate, false)
		if err != nil {
			return fmt.Errorf("invalid end date format (expected YYYY-MM-DD or YYYY-MM): %w", err)
		}
		endTimePtr = &parsed
	}

	// 构建导出参数
	params := csvexport.ExportParams{
		Symbol:     *symbols,
		Interval:   *interval,
		StartTime:  startTimePtr,
		EndTime:    endTimePtr,
		OutputPath: *output,
	}

	// 执行导出
	if err := exporter.Export(ctx, params); err != nil {
		return fmt.Errorf("CSV export failed: %w", err)
	}

	log.Info().Msg("CSV export completed successfully")
	return nil
}

// backfillGaps 补全历史数据缺口
func backfillGaps(ctx context.Context, cfg *config.Config) error {
	log := logger.GetLogger("backfill_gaps")
	log.Info().Msg("Starting historical data gaps backfill")

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

	// 解析交易对列表
	var symbolList []string
	if *symbols == "" {
		// 如果没有指定symbols，获取数据库中所有交易对
		log.Info().Msg("No symbols specified, fetching all symbols from database")
		symbolInfos, err := components.repository.GetAllSymbolInfos(ctx)
		if err != nil {
			return fmt.Errorf("failed to get all symbols from database: %w", err)
		}

		if len(symbolInfos) == 0 {
			return fmt.Errorf("no symbols found in database, please run data import first")
		}

		// 提取symbol名称
		symbolList = make([]string, len(symbolInfos))
		for i, info := range symbolInfos {
			symbolList[i] = info.Symbol
		}

		log.Info().Int("symbols_count", len(symbolList)).Msg("Found symbols in database for gap analysis")
	} else {
		// 解析用户指定的交易对列表
		symbolList = parseSymbolsParameter(*symbols)
		if len(symbolList) == 0 {
			return fmt.Errorf("no valid symbols provided")
		}
	}

	// 解析时间范围
	var startDatePtr *time.Time
	if *startDate != "" {
		parsed, err := parseFlexibleDate(*startDate, true)
		if err != nil {
			return fmt.Errorf("invalid start date format (expected YYYY-MM-DD or YYYY-MM): %w", err)
		}
		startDatePtr = &parsed
	}

	var endDatePtr *time.Time
	if *endDate != "" {
		parsed, err := parseFlexibleDate(*endDate, false)
		if err != nil {
			return fmt.Errorf("invalid end date format (expected YYYY-MM-DD or YYYY-MM): %w", err)
		}
		endDatePtr = &parsed
	}

	// 创建gap检测器
	gapDetector := gaps.NewGapDetector(components.repository, components.downloader)

	fmt.Printf("🔍 开始检测历史数据缺口...\n")
	if *symbols == "" {
		fmt.Printf("📊 交易对: 所有交易对 (共%d个)\n", len(symbolList))
	} else {
		fmt.Printf("📊 交易对: %s\n", strings.Join(symbolList, ", "))
	}
	if startDatePtr != nil && endDatePtr != nil {
		fmt.Printf("📅 时间范围: %s 到 %s\n", startDatePtr.Format("2006-01-02"), endDatePtr.Format("2006-01-02"))
	} else {
		fmt.Printf("📅 时间范围: 基于币安可用数据范围\n")
	}
	fmt.Println()

	// 检测所有缺口
	allGaps, err := gapDetector.DetectAllGaps(ctx, symbolList, startDatePtr, endDatePtr)
	if err != nil {
		return fmt.Errorf("failed to detect gaps: %w", err)
	}

	if len(allGaps) == 0 {
		fmt.Println("🎉 未发现数据缺口，所有数据完整！")
		return nil
	}

	// 显示检测到的缺口
	fmt.Printf("🚨 发现 %d 个数据缺口:\n\n", len(allGaps))
	totalMonths := 0
	for _, gap := range allGaps {
		fmt.Printf("📍 %s: %s (共%d个月)\n", gap.Symbol, gap.Description, len(gap.MissingMonths))
		totalMonths += len(gap.MissingMonths)
		if *detailed {
			fmt.Printf("   缺失月份: %s\n", strings.Join(gap.MissingMonths, ", "))
		}
	}
	fmt.Printf("\n📊 总计需要补全: %d 个月的数据\n\n", totalMonths)

	// 如果是预览模式，不执行下载
	if *dryRun {
		fmt.Println("🔍 预览模式：仅显示缺口，不执行下载")
		return nil
	}

	// 询问用户确认
	fmt.Print("⚠️  是否继续执行数据补全？ (y/N): ")
	var response string
	fmt.Scanln(&response)
	if response != "y" && response != "Y" {
		fmt.Println("❌ 用户取消操作")
		return nil
	}

	// 生成补全任务
	backfillTasks := gapDetector.GenerateBackfillTasks(allGaps)
	
	fmt.Printf("🚀 开始补全 %d 个任务...\n\n", len(backfillTasks))

	// 创建调度器并执行补全
	scheduler := scheduler.NewScheduler(
		cfg.Scheduler,
		components.downloader,
		components.importer,
		components.stateManager,
		components.progressReporter,
		components.repository,
	)

	// 启动进度报告器
	if components.progressReporter != nil {
		if err := components.progressReporter.Start(len(backfillTasks)); err != nil {
			log.Warn().Err(err).Msg("Failed to start progress reporter")
		}
	}

	// 执行补全任务
	if err := scheduler.BackfillGaps(ctx, backfillTasks, *force); err != nil {
		return fmt.Errorf("backfill failed: %w", err)
	}

	// 停止调度器
	if err := scheduler.Stop(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to stop scheduler gracefully")
	}

	fmt.Println("\n🎉 历史数据缺口补全完成！")
	fmt.Println("💡 建议运行以下命令验证数据完整性:")
	fmt.Println("   go run cmd/main.go -cmd=verify-data")
	fmt.Println("   go run cmd/main.go -cmd=populate-views")

	log.Info().Msg("Historical data gaps backfill completed successfully")
	return nil
}
