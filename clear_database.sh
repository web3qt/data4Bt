#!/bin/bash

# 数据库清理脚本
# 用于清空ClickHouse数据库中的所有K线数据

echo "🗑️  数据库清理工具"
echo "==================="
echo ""
echo "此脚本将清空数据库中的所有K线数据表："
echo "- klines_1m (1分钟K线数据)"
echo "- klines_5m (5分钟K线数据)"
echo "- klines_15m (15分钟K线数据)"
echo "- klines_1h (1小时K线数据)"
echo "- klines_4h (4小时K线数据)"
echo "- klines_1d (1天K线数据)"
echo ""

# 检查Go环境
if ! command -v go &> /dev/null; then
    echo "❌ 错误：未找到Go环境，请先安装Go"
    exit 1
fi

# 检查配置文件
if [ ! -f "configs/config.yml" ]; then
    echo "❌ 错误：未找到配置文件 configs/config.yml"
    exit 1
fi

# 执行清理程序
go run clear_database.go

echo ""
echo "📝 提示：如需重新加载数据，请运行："
echo "   go run cmd/main.go -cmd=run"