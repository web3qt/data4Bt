// +build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"binance-data-loader/internal/config"
	"binance-data-loader/pkg/clickhouse"
)

func main() {
	// 加载配置
	cfg, err := config.Load("configs/config.yml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 创建ClickHouse仓库连接
	repository, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer repository.Close()

	ctx := context.Background()

	// 确认清理操作
	fmt.Print("⚠️  警告：此操作将清空数据库中的所有K线数据！\n")
	fmt.Print("确认要继续吗？(输入 'yes' 确认): ")
	
	var confirmation string
	fmt.Scanln(&confirmation)
	
	if confirmation != "yes" {
		fmt.Println("操作已取消")
		os.Exit(0)
	}

	// 执行清理
	fmt.Println("🗑️  正在清理数据库...")
	if err := repository.ClearAllData(ctx); err != nil {
		log.Fatalf("Failed to clear database: %v", err)
	}

	fmt.Println("✅ 数据库清理完成！")
}