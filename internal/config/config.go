package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config 应用配置结构
type Config struct {
	Log                 LogConfig                 `mapstructure:"log"`
	Binance             BinanceConfig             `mapstructure:"binance"`
	Downloader          DownloaderConfig          `mapstructure:"downloader"`
	Parser              ParserConfig              `mapstructure:"parser"`
	Database            DatabaseConfig            `mapstructure:"database"`
	Importer            ImporterConfig            `mapstructure:"importer"`
	State               StateConfig               `mapstructure:"state"`
	MaterializedViews   MaterializedViewsConfig   `mapstructure:"materialized_views"`
	Monitoring          MonitoringConfig          `mapstructure:"monitoring"`
	Scheduler           SchedulerConfig           `mapstructure:"scheduler"`
}

// LogConfig 日志配置
type LogConfig struct {
	Level    string `mapstructure:"level"`
	Format   string `mapstructure:"format"`
	Output   string `mapstructure:"output"`
	FilePath string `mapstructure:"file_path"`
}

// BinanceConfig 币安配置
type BinanceConfig struct {
	BaseURL       string        `mapstructure:"base_url"`
	DataPath      string        `mapstructure:"data_path"`
	SymbolsFilter string        `mapstructure:"symbols_filter"`
	Interval      string        `mapstructure:"interval"`
	Timeout       time.Duration `mapstructure:"timeout"`
	RetryCount    int           `mapstructure:"retry_count"`
	RetryDelay    time.Duration `mapstructure:"retry_delay"`
	ProxyURL      string        `mapstructure:"proxy_url"`
}

// DownloaderConfig 下载器配置
type DownloaderConfig struct {
	Concurrency        int    `mapstructure:"concurrency"`
	BufferSize         int    `mapstructure:"buffer_size"`
	UserAgent          string `mapstructure:"user_agent"`
	EnableCompression  bool   `mapstructure:"enable_compression"`
	MaxFileSize        string `mapstructure:"max_file_size"`
}

// ParserConfig 解析器配置
type ParserConfig struct {
	Concurrency      int  `mapstructure:"concurrency"`
	BufferSize       int  `mapstructure:"buffer_size"`
	ValidateData     bool `mapstructure:"validate_data"`
	SkipInvalidRows  bool `mapstructure:"skip_invalid_rows"`
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	ClickHouse ClickHouseConfig `mapstructure:"clickhouse"`
}

// ClickHouseConfig ClickHouse配置
type ClickHouseConfig struct {
	Hosts             []string          `mapstructure:"hosts"`
	Database          string            `mapstructure:"database"`
	Username          string            `mapstructure:"username"`
	Password          string            `mapstructure:"password"`
	Settings          map[string]int    `mapstructure:"settings"`
	Compression       string            `mapstructure:"compression"`
	DialTimeout       time.Duration     `mapstructure:"dial_timeout"`
	MaxOpenConns      int               `mapstructure:"max_open_conns"`
	MaxIdleConns      int               `mapstructure:"max_idle_conns"`
	ConnMaxLifetime   time.Duration     `mapstructure:"conn_max_lifetime"`
}

// ImporterConfig 导入器配置
type ImporterConfig struct {
	BatchSize            int           `mapstructure:"batch_size"`
	BufferSize           int           `mapstructure:"buffer_size"`
	FlushInterval        time.Duration `mapstructure:"flush_interval"`
	EnableDeduplication  bool          `mapstructure:"enable_deduplication"`
}

// StateConfig 状态管理配置
type StateConfig struct {
	StorageType   string        `mapstructure:"storage_type"`
	FilePath      string        `mapstructure:"file_path"`
	BackupCount   int           `mapstructure:"backup_count"`
	SaveInterval  time.Duration `mapstructure:"save_interval"`
}

// MaterializedViewsConfig 物化视图配置
type MaterializedViewsConfig struct {
	Enabled          bool          `mapstructure:"enabled"`
	Intervals        []string      `mapstructure:"intervals"`
	RefreshInterval  time.Duration `mapstructure:"refresh_interval"`
	ParallelRefresh  bool          `mapstructure:"parallel_refresh"`
}

// MonitoringConfig 监控配置
type MonitoringConfig struct {
	Enabled                  bool          `mapstructure:"enabled"`
	MetricsPort              int           `mapstructure:"metrics_port"`
	HealthCheckPort          int           `mapstructure:"health_check_port"`
	ProgressReportInterval   time.Duration `mapstructure:"progress_report_interval"`
}

// SchedulerConfig 调度器配置
type SchedulerConfig struct {
	EndDate               string `mapstructure:"end_date"`
	BatchDays             int    `mapstructure:"batch_days"`
	MaxConcurrentSymbols  int    `mapstructure:"max_concurrent_symbols"`
}

// Load 加载配置文件
func Load(configPath string) (*Config, error) {
	v := viper.New()
	
	// 设置配置文件路径和名称
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yml")
		v.AddConfigPath("./configs")
		v.AddConfigPath("../configs")
		v.AddConfigPath(".")
	}
	
	// 设置环境变量前缀
	v.SetEnvPrefix("BDL")
	v.AutomaticEnv()
	
	// 设置默认值
	setDefaults(v)
	
	// 读取配置文件
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	
	// 解析配置
	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	
	// 验证配置
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}
	
	return &config, nil
}

// setDefaults 设置默认配置值
func setDefaults(v *viper.Viper) {
	// 日志默认配置
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("log.output", "stdout")
	
	// 币安默认配置
	v.SetDefault("binance.base_url", "https://data.binance.vision")
	v.SetDefault("binance.data_path", "/data/spot/daily/klines")
	v.SetDefault("binance.symbols_filter", "USDT")
	v.SetDefault("binance.interval", "1m")
	v.SetDefault("binance.timeout", "30s")
	v.SetDefault("binance.retry_count", 3)
	v.SetDefault("binance.retry_delay", "5s")
	
	// 下载器默认配置
	v.SetDefault("downloader.concurrency", 10)
	v.SetDefault("downloader.buffer_size", 100)
	v.SetDefault("downloader.user_agent", "BinanceDataLoader/1.0")
	v.SetDefault("downloader.enable_compression", true)
	v.SetDefault("downloader.max_file_size", "100MB")
	
	// 解析器默认配置
	v.SetDefault("parser.concurrency", 5)
	v.SetDefault("parser.buffer_size", 50)
	v.SetDefault("parser.validate_data", true)
	v.SetDefault("parser.skip_invalid_rows", true)
	
	// 数据库默认配置
	v.SetDefault("database.clickhouse.hosts", []string{"localhost:9000"})
	v.SetDefault("database.clickhouse.database", "data4BT")
	v.SetDefault("database.clickhouse.username", "default")
	v.SetDefault("database.clickhouse.password", "123456")
	v.SetDefault("database.clickhouse.compression", "lz4")
	v.SetDefault("database.clickhouse.dial_timeout", "30s")
	v.SetDefault("database.clickhouse.max_open_conns", 10)
	v.SetDefault("database.clickhouse.max_idle_conns", 5)
	v.SetDefault("database.clickhouse.conn_max_lifetime", "10m")
	
	// 导入器默认配置
	v.SetDefault("importer.batch_size", 2)
	v.SetDefault("importer.buffer_size", 20)
	v.SetDefault("importer.flush_interval", "30s")
	v.SetDefault("importer.enable_deduplication", true)
	
	// 状态管理默认配置
	v.SetDefault("state.storage_type", "file")
	v.SetDefault("state.file_path", "state/progress.json")
	v.SetDefault("state.backup_count", 5)
	v.SetDefault("state.save_interval", "10s")
	
	// 物化视图默认配置
	v.SetDefault("materialized_views.enabled", true)
	v.SetDefault("materialized_views.intervals", []string{"5m", "15m", "1h", "4h", "1d"})
	v.SetDefault("materialized_views.refresh_interval", "1m")
	v.SetDefault("materialized_views.parallel_refresh", true)
	
	// 监控默认配置
	v.SetDefault("monitoring.enabled", true)
	v.SetDefault("monitoring.metrics_port", 8080)
	v.SetDefault("monitoring.health_check_port", 8081)
	v.SetDefault("monitoring.progress_report_interval", "30s")
	
	// 调度器默认配置

	v.SetDefault("scheduler.end_date", "")
	v.SetDefault("scheduler.batch_days", 7)
	v.SetDefault("scheduler.max_concurrent_symbols", 5)
}

// validateConfig 验证配置
func validateConfig(config *Config) error {
	if config.Binance.BaseURL == "" {
		return fmt.Errorf("binance.base_url is required")
	}
	
	if config.Database.ClickHouse.Database == "" {
		return fmt.Errorf("database.clickhouse.database is required")
	}
	
	if len(config.Database.ClickHouse.Hosts) == 0 {
		return fmt.Errorf("database.clickhouse.hosts is required")
	}
	
	if config.Downloader.Concurrency <= 0 {
		return fmt.Errorf("downloader.concurrency must be greater than 0")
	}
	
	if config.Parser.Concurrency <= 0 {
		return fmt.Errorf("parser.concurrency must be greater than 0")
	}
	
	if config.Importer.BatchSize <= 0 {
		return fmt.Errorf("importer.batch_size must be greater than 0")
	}
	
	return nil
}