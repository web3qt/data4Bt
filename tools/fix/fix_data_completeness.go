package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/logger"
	"binance-data-loader/pkg/clickhouse"
)

func main() {
	fmt.Println("🔧 修复数据完整性问题...")

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

	fmt.Println("\n⚠️  开始清除所有现有数据并重建结构...")
	fmt.Println("执行操作:")
	fmt.Println("1. 清除所有现有的K线数据")
	fmt.Println("2. 清除所有物化视图")
	fmt.Println("3. 重新创建表结构")
	fmt.Println("4. 重新创建物化视图")

	fmt.Println("\n🗑️  清理现有数据...")
	if err := repo.ClearAllData(ctx); err != nil {
		fmt.Printf("❌ Error clearing data: %v\n", err)
		return
	}
	fmt.Println("✅ 数据清理完成")

	fmt.Println("\n🏗️  重新创建表结构...")
	if err := repo.CreateTables(ctx); err != nil {
		fmt.Printf("❌ Error creating tables: %v\n", err)
		return
	}
	fmt.Println("✅ 表结构创建完成")

	fmt.Println("\n📊 重新创建物化视图...")
	intervals := []string{"5m", "15m", "1h", "4h", "1d"}
	if err := repo.CreateMaterializedViews(ctx, intervals); err != nil {
		fmt.Printf("❌ Error creating materialized views: %v\n", err)
		return
	}
	fmt.Println("✅ 物化视图创建完成")

	fmt.Println("\n✅ 数据库重置完成!")
	fmt.Println("\n📋 下一步操作建议:")
	fmt.Println("1. 🔄 运行数据下载程序获取完整的历史数据")
	fmt.Println("   时间范围: 2017年8月 - 2025年7月")
	fmt.Println("2. 📊 使用以下命令启动数据下载:")
	fmt.Println("   go run cmd/main.go")
	fmt.Println("3. 🔍 下载完成后运行验证脚本:")
	fmt.Println("   go run tools/verify_materialized_views.go")

	fmt.Println("\n💡 提示: 完整的BTC历史数据下载可能需要较长时间")
	fmt.Println("建议在网络稳定的环境下进行，并监控下载进度。")
}