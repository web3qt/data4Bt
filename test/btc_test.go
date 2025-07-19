package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"../internal/config"
	"../internal/domain"
	"../internal/logger"
	"../pkg/binance"
	"../pkg/clickhouse"
	"../pkg/parser"
)

func main() {
	ctx := context.Background()
	
	// 获取当前目录
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatal("Failed to get current directory:", err)
	}
	
	// 加载配置文件
	configPath := filepath.Join(currentDir, "config_test.yml")
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}
	
	// 初始化日志
	if err := logger.Init(cfg.Log); err != nil {
		log.Fatal("Failed to initialize logger:", err)
	}
	
	logger := logger.GetLogger("btc-test")
	logger.Info().Msg("Starting BTC USDT test")
	
	// 创建ClickHouse repository
	repo, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create ClickHouse repository")
	}
	defer repo.Close()
	
	// 创建数据表
	logger.Info().Msg("Creating database tables")
	if err := repo.CreateTables(ctx); err != nil {
		logger.Fatal().Err(err).Msg("Failed to create tables")
	}
	logger.Info().Msg("Tables created successfully")
	
	// 创建下载器
	downloader := binance.NewDownloader(cfg.Binance)
	
	// 创建解析器
	parser := parser.NewCSVParser(cfg.Parser)
	
	// 测试符号
	symbol := "BTCUSDT"
	
	// 获取昨天的日期进行测试
	yesterday := time.Now().AddDate(0, 0, -1)
	testDate := yesterday.Format("2006-01-02")
	
	logger.Info().Str("symbol", symbol).Str("date", testDate).Msg("Testing data download and parsing")
	
	// 构建下载任务
	task := domain.DownloadTask{
		Symbol:   symbol,
		Date:     yesterday,
		Interval: "1m",
		URL:      fmt.Sprintf("%s%s/%s/%s-1m-%s.zip", cfg.Binance.BaseURL, cfg.Binance.DataPath, symbol, symbol, testDate),
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
	
	// 保存到ClickHouse
	if len(klines) > 0 {
		logger.Info().Msg("Saving data to ClickHouse")
		if err := repo.Save(ctx, klines); err != nil {
			logger.Fatal().Err(err).Msg("Failed to save data to ClickHouse")
		}
		logger.Info().Int("saved_records", len(klines)).Msg("Data saved successfully")
		
		// 验证保存的数据
		lastDate, err := repo.GetLastDate(ctx, symbol)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to get last date")
		} else {
			logger.Info().Str("last_date", lastDate.Format("2006-01-02")).Msg("Last date in database")
		}
		
		// 验证数据完整性
		validationResult, err := repo.ValidateData(ctx, symbol, yesterday)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to validate saved data")
		} else {
			logger.Info().
				Int("total_rows", validationResult.TotalRows).
				Int("valid_rows", validationResult.ValidRows).
				Bool("is_valid", validationResult.Valid).
				Msg("Data validation completed")
		}
	} else {
		logger.Warn().Msg("No data to save")
	}
	
	logger.Info().Msg("BTC USDT test completed successfully")
}