#!/bin/bash

# 币安数据加载器状态检查脚本
echo "=== 币安数据加载器状态检查 ==="
echo "检查时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 检查ClickHouse数据库连接
echo "📡 检查数据库连接..."
if docker exec shared-clickhouse clickhouse-client --query "SELECT 1" > /dev/null 2>&1; then
    echo "✅ ClickHouse数据库连接正常"
else
    echo "❌ ClickHouse数据库连接失败"
    exit 1
fi

# 检查数据库和表
echo ""
echo "🗄️ 检查数据库状态..."
DB_EXISTS=$(docker exec shared-clickhouse clickhouse-client --query "SELECT count() FROM system.databases WHERE name='data4BT'")
if [ "$DB_EXISTS" -eq 1 ]; then
    echo "✅ data4BT数据库存在"
    
    # 检查表结构
    TABLE_COUNT=$(docker exec shared-clickhouse clickhouse-client --database=data4BT --query "SELECT count() FROM system.tables WHERE database='data4BT'")
    echo "📋 数据库中有 $TABLE_COUNT 个表"
    
    # 检查klines_1m表的数据
    RECORD_COUNT=$(docker exec shared-clickhouse clickhouse-client --database=data4BT --query "SELECT COUNT(*) FROM klines_1m")
    echo "📈 klines_1m表中有 $RECORD_COUNT 条记录"
    
    if [ "$RECORD_COUNT" -gt 0 ]; then
        echo ""
        echo "📊 数据统计 (前10个交易对):"
        echo "交易对        记录数      最早时间        最新时间"
        echo "--------------------------------------------------------"
        docker exec shared-clickhouse clickhouse-client --database=data4BT --query "
        SELECT 
            symbol,
            formatReadableQuantity(COUNT(*)) as records,
            toString(MIN(open_time)) as earliest,
            toString(MAX(open_time)) as latest
        FROM klines_1m 
        GROUP BY symbol 
        ORDER BY COUNT(*) DESC 
        LIMIT 10
        FORMAT TSV" | while IFS=$'\t' read -r symbol records earliest latest; do
            printf "%-12s %-10s %-15s %s\n" "$symbol" "$records" "${earliest:0:10}" "${latest:0:10}"
        done
    else
        echo "⚠️  数据库表已创建但暂无数据"
    fi
else
    echo "❌ data4BT数据库不存在"
fi

echo ""
echo "🔄 检查数据加载器进程..."
if [ -f ".data_loader_pid" ]; then
    PID=$(cat .data_loader_pid)
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "✅ 数据加载器正在运行 (PID: $PID)"
        
        # 检查最近的日志
        if [ -f "logs/data_loader.log" ]; then
            echo ""
            echo "📝 最近的日志 (最后5行):"
            tail -5 logs/data_loader.log | while IFS= read -r line; do
                echo "   $line"
            done
        fi
    else
        echo "❌ 数据加载器进程不存在 (PID文件显示: $PID)"
    fi
else
    echo "❌ 找不到数据加载器PID文件"
fi

# 检查监控服务器
echo ""
echo "🌐 检查监控服务器..."
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "✅ 监控服务器运行正常 (http://localhost:8080)"
    echo "   📊 监控面板: http://localhost:8080"
    echo "   📡 进度API: http://localhost:8080/progress"
else
    echo "❌ 监控服务器未运行或不可访问"
fi

# 检查状态文件
echo ""
echo "📁 检查状态文件..."
if [ -f "state/progress.json" ]; then
    SYMBOL_COUNT=$(cat state/progress.json | grep -o '"[A-Z0-9]*USDT"' | wc -l)
    echo "✅ 进度状态文件存在，包含 $SYMBOL_COUNT 个交易对的状态"
    
    echo ""
    echo "🎯 处理进度最多的前5个交易对:"
    echo "交易对        已处理月数   最后处理时间"
    echo "----------------------------------------"
    cat state/progress.json | jq -r 'to_entries | map({symbol: .key, processed: .value.processed, last_date: .value.last_date}) | sort_by(.processed) | reverse | limit(5; .[]) | "\(.symbol)\t\(.processed)\t\(.last_date)"' 2>/dev/null | while IFS=$'\t' read -r symbol processed last_date; do
        printf "%-12s %-10s %s\n" "$symbol" "$processed" "${last_date:0:10}"
    done 2>/dev/null || echo "   (需要安装jq命令来显示详细信息)"
else
    echo "❌ 状态文件不存在"
fi

# 网络连接检查
echo ""
echo "🌍 检查网络连接..."
if curl -s --connect-timeout 5 https://api.binance.com/api/v3/ping > /dev/null 2>&1; then
    echo "✅ 币安API连接正常"
else
    echo "❌ 币安API连接失败，可能影响数据下载"
fi

if curl -s --connect-timeout 5 https://data.binance.vision/ > /dev/null 2>&1; then
    echo "✅ 币安数据源连接正常"
else
    echo "❌ 币安数据源连接失败，这是数据下载的主要问题"
    echo "   💡 建议检查网络连接或稍后重试"
fi

echo ""
echo "🎯 快速操作指南:"
echo "   启动数据加载器: ./start.sh"
echo "   停止数据加载器: ./stop.sh"  
echo "   查看实时日志: tail -f logs/data_loader.log"
echo "   清理数据库: ./clear_database.sh"
echo "   状态检查工具: go run tools/status_checker.go"
echo "   监控面板: open http://localhost:8080"
echo ""