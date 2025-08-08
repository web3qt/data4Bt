#!/bin/bash

# Binance Data Loader 状态查看脚本
# 使用方法:
#   ./status.sh              - 查看所有代币状态
#   ./status.sh -d           - 查看详细状态
#   ./status.sh BTCUSDT      - 查看特定代币状态
#   ./status.sh BTCUSDT,ETHUSDT -d  - 查看多个代币的详细状态

set -e

# 默认参数
DETAILED=""
SYMBOLS=""

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--detailed)
            DETAILED="-detailed"
            shift
            ;;
        -h|--help)
            echo "Binance Data Loader 状态查看工具"
            echo ""
            echo "使用方法:"
            echo "  $0                    查看所有代币状态"
            echo "  $0 -d                查看详细状态"
            echo "  $0 BTCUSDT           查看特定代币状态"
            echo "  $0 BTCUSDT,ETHUSDT   查看多个代币状态"
            echo "  $0 BTCUSDT -d        查看特定代币详细状态"
            echo ""
            echo "选项:"
            echo "  -d, --detailed       显示详细信息"
            echo "  -h, --help          显示此帮助信息"
            exit 0
            ;;
        *)
            if [[ -z "$SYMBOLS" ]]; then
                SYMBOLS="$1"
            else
                echo "错误: 只能指定一个代币列表参数"
                exit 1
            fi
            shift
            ;;
    esac
done

# 构建命令
CMD="go run cmd/main.go -cmd=status"

if [[ -n "$SYMBOLS" ]]; then
    CMD="$CMD -symbols=$SYMBOLS"
fi

if [[ -n "$DETAILED" ]]; then
    CMD="$CMD $DETAILED"
fi

# 执行命令
echo "正在查询数据下载状态..."
echo ""
eval $CMD

echo ""
echo "=== 数据库实时统计 ==="
echo ""

# 检查数据加载器进程状态
if [ -f ".data_loader_pid" ]; then
    DATA_LOADER_PID=$(cat .data_loader_pid)
    if kill -0 $DATA_LOADER_PID 2>/dev/null; then
        echo "🟢 数据加载器状态: 运行中 (PID: $DATA_LOADER_PID)"
    else
        echo "🔴 数据加载器状态: 已停止"
    fi
else
    echo "🔴 数据加载器状态: 未启动"
fi

echo ""
echo "📊 数据库表统计:"
echo "---------------------------------------------"

# 检查ClickHouse连接并查询数据
if docker exec shared-clickhouse clickhouse-client --database=data4BT --query="SELECT 1" >/dev/null 2>&1; then
    # 查询1分钟K线数据统计
    echo "1分钟K线数据:"
    docker exec shared-clickhouse clickhouse-client --database=data4BT --query='SELECT symbol, COUNT(*) as records, MIN(open_time) as earliest, MAX(close_time) as latest FROM klines_1m GROUP BY symbol ORDER BY COUNT(*) DESC FORMAT PrettyCompact' || echo "  暂无数据"
    
    echo ""
    echo "5分钟K线数据:"
    docker exec shared-clickhouse clickhouse-client --database=data4BT --query='SELECT symbol, COUNT(*) as records, MIN(open_time) as earliest, MAX(close_time) as latest FROM klines_5m GROUP BY symbol ORDER BY COUNT(*) DESC FORMAT PrettyCompact' || echo "  暂无数据"
    
    echo ""
    echo "💡 提示: 使用 'tail -f logs/data_loader.log' 查看实时日志"
else
    echo "❌ 无法连接到ClickHouse数据库"
fi