package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/logger"
	"binance-data-loader/internal/state"
	"binance-data-loader/pkg/binance"
	"binance-data-loader/pkg/clickhouse"
	"binance-data-loader/pkg/importer"
	"binance-data-loader/pkg/monitor"
	"binance-data-loader/pkg/parser"
	"binance-data-loader/pkg/scheduler"
)

var (
	configFile = flag.String("config", "config.yml", "Configuration file path")
	command    = flag.String("cmd", "run", "Command to execute: run, validate, init-db, create-views")
	symbols    = flag.String("symbols", "", "Comma-separated list of symbols to process (optional)")
	endDate    = flag.String("end", "", "End date (YYYY-MM-DD)")
	verbose    = flag.Bool("verbose", false, "Enable verbose logging")
	version    = flag.Bool("version", false, "Show version information")
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
		return runDataLoader(ctx, cfg)
	case "validate":
		return validateData(ctx, cfg)
	case "init-db":
		return initializeDatabase(ctx, cfg)
	case "create-views":
		return createMaterializedViews(ctx, cfg)
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

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\n%s - Download and process Binance K-line data\n\n", appName)
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  run        - Run the data loader (default)\n")
		fmt.Fprintf(os.Stderr, "  validate   - Validate existing data\n")
		fmt.Fprintf(os.Stderr, "  init-db    - Initialize database tables\n")
		fmt.Fprintf(os.Stderr, "  create-views - Create materialized views\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -cmd=run -start=2024-01-01 -end=2024-01-31\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=validate -symbols=BTCUSDT,ETHUSDT\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -cmd=init-db\n", os.Args[0])
	}
}
