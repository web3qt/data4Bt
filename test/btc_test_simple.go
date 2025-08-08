package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/pkg/binance"
	"binance-data-loader/pkg/parser"
)

// 简化的配置结构，只包含必要的字段
type SimpleConfig struct {
	Logging struct {
		Level  string `yaml:"level"`
		Format string `yaml:"format"`
	} `yaml:"logging"`
	Binance struct {
		BaseURL  string `yaml:"base_url"`
		DataPath string `yaml:"data_path"`
		Interval string `yaml:"interval"`
		Timeout  int    `yaml:"timeout"`
		Retries  int    `yaml:"retries"`
	} `yaml:"binance"`
	Parser struct {
		Validation struct {
			Enabled bool `yaml:"enabled"`
		} `yaml:"validation"`
		MaxFileSize int64 `yaml:"max_file_size"`
	} `yaml:"parser"`
}

func mainTest() {
	ctx := context.Background()
	
	// 加载配置
	cfg, err := loadSimpleConfig("test/config_test.yml")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}
	
	// 初始化日志
	logger := initLogger(cfg.Logging.Level, cfg.Logging.Format)
	logger.Info().Msg("Starting BTC USDT simple test (download and parse only)")
	
	// 初始化下载器
	downloader := binance.NewBinanceDownloader(config.BinanceConfig{
		BaseURL:    cfg.Binance.BaseURL,
		DataPath:   cfg.Binance.DataPath,
		Interval:   cfg.Binance.Interval,
		Timeout:    time.Duration(cfg.Binance.Timeout) * time.Second,
		RetryCount: cfg.Binance.Retries,
		RetryDelay: 1 * time.Second,
	}, config.DownloaderConfig{
		UserAgent: "BTC-Test/1.0",
	})
	
	// 初始化解析器
	parser := parser.NewCSVParser(config.ParserConfig{
		ValidateData: cfg.Parser.Validation.Enabled,
	})
	
	// 测试符号
	symbol := "BTCUSDT"
	
	// 使用一个确定存在的历史日期进行测试
	testDay := time.Date(2023, 7, 15, 0, 0, 0, 0, time.UTC)
	testDate := testDay.Format("2006-01-02")
	
	logger.Info().Str("symbol", symbol).Str("date", testDate).Msg("Testing data download and parsing")
	
	// 创建下载任务
	task := domain.DownloadTask{
		Symbol:   symbol,
		Date:     testDay,
		Interval: "1m",
		URL:      downloader.BuildDownloadURL(symbol, testDay),
		Retries:  0,
	}
	
	// 下载数据
	logger.Info().Str("url", task.URL).Msg("Downloading data")
	data, err := downloader.Fetch(ctx, task)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to download data")
	}
	logger.Info().Int("data_size", len(data)).Msg("Data downloaded successfully")
	
	// 解析数据
	logger.Info().Msg("Parsing data")
	klines, validationResult, err := parser.Parse(ctx, data, symbol)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to parse data")
	}
	
	logger.Info().
		Int("total_klines", len(klines)).
		Bool("validation_valid", validationResult.Valid).
		Int("validation_errors", len(validationResult.Errors)).
		Msg("Data parsed successfully")
	
	if len(validationResult.Errors) > 0 {
		logger.Warn().Strs("errors", validationResult.Errors).Msg("Validation errors found")
	}
	
	// 显示一些示例数据
	if len(klines) > 0 {
		logger.Info().Msg("Sample data (first 3 records):")
		for i := 0; i < 3 && i < len(klines); i++ {
			kline := klines[i]
			logger.Info().
				Str("symbol", kline.Symbol).
				Time("open_time", kline.OpenTime).
				Float64("open_price", kline.OpenPrice).
				Float64("high_price", kline.HighPrice).
				Float64("low_price", kline.LowPrice).
				Float64("close_price", kline.ClosePrice).
				Float64("volume", kline.Volume).
				Msgf("Record %d", i+1)
		}
		
		// 显示时间范围
		firstTime := klines[0].OpenTime
		lastTime := klines[len(klines)-1].OpenTime
		logger.Info().
			Time("first_time", firstTime).
			Time("last_time", lastTime).
			Dur("time_span", lastTime.Sub(firstTime)).
			Msg("Data time range")
	} else {
		logger.Warn().Msg("No data parsed")
	}
	
	logger.Info().Msg("BTC USDT simple test completed successfully")
	logger.Info().Msg("Note: This test only validates download and parsing. To test database storage, please install Docker and run the full test.")
}

func loadSimpleConfig(filename string) (*SimpleConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	
	var cfg SimpleConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	
	return &cfg, nil
}

func initLogger(level, format string) zerolog.Logger {
	// 设置日志级别
	logLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		logLevel = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(logLevel)
	
	// 设置日志格式
	if format == "pretty" {
		return zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Str("component", "btc-simple-test").Logger()
	}
	
	// 默认JSON格式
	return zerolog.New(os.Stdout).With().Timestamp().Str("component", "btc-simple-test").Logger()
}
