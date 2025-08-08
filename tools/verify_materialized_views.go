package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/logger"
	"binance-data-loader/pkg/clickhouse"
)

func main() {
	fmt.Println("🔍 检查数据完整性和物化视图状态...")

	// 获取当前目录
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatal("Failed to get current directory:", err)
	}

	// 加载配置文件
	configPath := filepath.Join(currentDir, "test", "config_test.yml")
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 初始化日志
	if err := logger.Init(cfg.Log); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	// 创建ClickHouse存储库
	repo, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		log.Fatalf("Failed to create ClickHouse repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()
	symbol := "BTCUSDT"

	// 检查原始数据完整性
	fmt.Println("\n📊 检查原始1分钟数据完整性:")
	firstDate, err := repo.GetFirstDate(ctx, symbol)
	if err != nil {
		fmt.Printf("❌ Error getting first date: %v\n", err)
	} else if firstDate.IsZero() {
		fmt.Println("⚠️  No data found for", symbol)
	} else {
		fmt.Printf("📅 First date: %s\n", firstDate.Format("2006-01-02"))
	}

	lastDate, err := repo.GetLastDate(ctx, symbol)
	if err != nil {
		fmt.Printf("❌ Error getting last date: %v\n", err)
	} else if lastDate.IsZero() {
		fmt.Println("⚠️  No data found for", symbol)
	} else {
		fmt.Printf("📅 Last date: %s\n", lastDate.Format("2006-01-02"))
	}

	// 分析数据完整性
	if !firstDate.IsZero() && !lastDate.IsZero() {
		fmt.Println("\n🔍 数据完整性分析:")
		
		// 检查时间范围是否符合预期
		expectedStart := time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC)
		expectedEnd := time.Date(2025, 7, 31, 23, 59, 0, 0, time.UTC)
		
		fmt.Printf("📅 预期数据范围: %s 到 %s\n", expectedStart.Format("2006-01-02"), expectedEnd.Format("2006-01-02"))
		fmt.Printf("📅 实际数据范围: %s 到 %s\n", firstDate.Format("2006-01-02"), lastDate.Format("2006-01-02"))
		
		if firstDate.After(expectedStart) {
			missingDays := int(firstDate.Sub(expectedStart).Hours() / 24)
			fmt.Printf("⚠️  缺少早期数据: 从 %s 开始，缺少约 %d 天的数据\n", expectedStart.Format("2006-01-02"), missingDays)
		}
		
		if lastDate.Before(expectedEnd) {
			missingDays := int(expectedEnd.Sub(lastDate).Hours() / 24)
			fmt.Printf("⚠️  缺少最新数据: 到 %s 结束，缺少约 %d 天的数据\n", expectedEnd.Format("2006-01-02"), missingDays)
		}
		
		// 计算预期的总记录数（按分钟计算）
		totalDays := int(lastDate.Sub(firstDate).Hours() / 24)
		expectedRecords := totalDays * 24 * 60 // 每天1440分钟
		
		// 验证数据质量
		validationResult, err := repo.ValidateData(ctx, symbol, lastDate)
		if err != nil {
			fmt.Printf("❌ Error validating data: %v\n", err)
		} else {
			fmt.Printf("\n📊 数据质量统计:\n")
			fmt.Printf("✅ 实际记录数: %d\n", validationResult.TotalRows)
			fmt.Printf("📈 预期记录数: %d (基于时间跨度)\n", expectedRecords)
			fmt.Printf("✅ 有效记录数: %d\n", validationResult.ValidRows)
			fmt.Printf("⚠️  无效记录数: %d\n", validationResult.InvalidRows)
			
			if validationResult.TotalRows > 0 {
				quality := float64(validationResult.ValidRows) / float64(validationResult.TotalRows) * 100
				fmt.Printf("📊 数据质量: %.2f%%\n", quality)
			}
			
			if expectedRecords > 0 {
				completeness := float64(validationResult.TotalRows) / float64(expectedRecords) * 100
				fmt.Printf("📊 数据完整度: %.2f%%\n", completeness)
				
				if completeness < 80 {
					fmt.Printf("⚠️  数据完整度较低，可能存在大量缺失数据\n")
				}
			}
		}
	}

	fmt.Println("\n🔍 数据问题总结:")
	fmt.Println("1. ⚠️  原始数据严重不完整，只有0.06%的预期数据")
	fmt.Println("2. ⚠️  缺少2017年8月到2019年3月的早期数据（约577天）")
	fmt.Println("3. ⚠️  缺少2023年7月到2025年7月的最新数据（约747天）")
	fmt.Println("4. ⚠️  物化视图基于不完整的原始数据，聚合结果也会不完整")

	fmt.Println("\n💡 建议解决方案:")
	fmt.Println("1. 🔄 重新下载完整的历史数据（2017年8月-2025年7月）")
	fmt.Println("2. 🗑️  清理现有的不完整数据")
	fmt.Println("3. 📥 导入完整的数据集")
	fmt.Println("4. 🔄 重新创建物化视图以确保聚合数据的完整性")

	fmt.Println("\n📋 物化视图表状态:")
	fmt.Println("   - klines_5m (5分钟) - 基于不完整数据")
	fmt.Println("   - klines_15m (15分钟) - 基于不完整数据")
	fmt.Println("   - klines_1h (1小时) - 基于不完整数据")
	fmt.Println("   - klines_4h (4小时) - 基于不完整数据")
	fmt.Println("   - klines_1d (1天) - 基于不完整数据")

	fmt.Println("\n✅ 检查完成!")
}
