#!/bin/bash

# 完全重置数据加载系统的脚本
# 包括清理数据库和所有状态文件
# 支持多种环境：本地ClickHouse、Docker、docker-compose

set -e  # 遇到错误时退出

echo "🧹 币安数据加载器 - 完全重置脚本"
echo "========================================"
echo

# 1. 停止可能运行的程序
echo "🛑 检查并停止正在运行的程序..."

# 查找并终止相关进程
pids=$(ps aux | grep -E "(go run.*main\.go|data-loader)" | grep -v grep | awk '{print $2}' || true)
if [ -n "$pids" ]; then
    echo "发现正在运行的程序，正在终止..."
    echo "$pids" | xargs kill -9 2>/dev/null || true
    echo "✅ 程序已停止"
else
    echo "✅ 没有发现运行的程序"
fi

# 2. 清理状态文件
echo
echo "📁 清理状态文件..."

# 备份现有状态文件（如果存在）
if [ -f "state/progress.json" ] && [ "$(cat state/progress.json 2>/dev/null)" != "{}" ]; then
    timestamp=$(date +"%Y%m%d_%H%M%S")
    cp state/progress.json "state/progress_backup_${timestamp}.json" 2>/dev/null || true
    echo "📦 已备份现有状态到 state/progress_backup_${timestamp}.json"
fi

# 清理所有状态文件
rm -f state/progress.json
rm -f state/progress.json.backup
rm -f state/timelines.json

# 确保state目录存在并创建空的状态文件
mkdir -p state
echo "{}" > state/progress.json
echo "{}" > state/timelines.json

echo "✅ 状态文件已清理并重置"

# 3. 清理数据库
echo
echo "🗄️  清理ClickHouse数据库..."

# 数据库清理函数
cleanup_database() {
    local client_cmd="$1"
    echo "正在清理数据库表和视图..."
    
    # 清理各个时间周期的K线数据表
    TABLES=("klines_1m" "klines_5m" "klines_15m" "klines_1h" "klines_4h" "klines_1d" "symbol_infos" "symbol_progress")
    
    for table in "${TABLES[@]}"; do
        echo "   清理表: $table"
        if [[ "$client_cmd" == *"curl"* ]]; then
            # HTTP API 方式
            curl -s -X POST "http://localhost:8123/" \
                 --data "TRUNCATE TABLE IF EXISTS data4BT.$table" \
                 --user "default:123456" > /dev/null 2>&1 || true
        else
            # ClickHouse客户端方式
            $client_cmd "TRUNCATE TABLE IF EXISTS data4BT.$table" 2>/dev/null || true
        fi
        echo "   ✅ $table 清理完成"
    done
    
    echo "✅ 数据库清理完成"
    
    # 验证清理结果
    echo "📊 验证清理结果..."
    for table in "klines_1m" "symbol_infos"; do
        if [[ "$client_cmd" == *"curl"* ]]; then
            # HTTP API 方式验证
            count=$(curl -s -X POST "http://localhost:8123/" \
                         --data "SELECT COUNT(*) FROM data4BT.$table" \
                         --user "default:123456" 2>/dev/null || echo "error")
        else
            # ClickHouse客户端方式验证
            count=$($client_cmd "SELECT COUNT(*) FROM data4BT.$table" 2>/dev/null || echo "error")
        fi
        
        if [ "$count" = "0" ]; then
            echo "✅ $table: 已清空"
        elif [ "$count" = "error" ]; then
            echo "⚠️  $table: 检查失败（表可能不存在）"
        else
            echo "❌ $table: 仍有 $count 条记录"
        fi
    done
}

# 尝试多种方式连接ClickHouse
db_cleaned=false

# 方式1: 本地ClickHouse客户端
if command -v clickhouse-client &> /dev/null; then
    if clickhouse-client --query "SELECT 1" &>/dev/null; then
        echo "使用本地ClickHouse客户端..."
        cleanup_database "clickhouse-client --query"
        db_cleaned=true
    fi
fi

# 方式2: docker-compose中的ClickHouse
if [ "$db_cleaned" = false ] && command -v docker-compose &> /dev/null; then
    if docker-compose ps | grep -q clickhouse && docker-compose exec -T clickhouse clickhouse-client --query "SELECT 1" &>/dev/null; then
        echo "使用docker-compose中的ClickHouse..."
        cleanup_database "docker-compose exec -T clickhouse clickhouse-client --query"
        db_cleaned=true
    fi
fi

# 方式3: Docker容器中的ClickHouse (多种可能的容器名)
if [ "$db_cleaned" = false ]; then
    for container_name in "clickhouse" "data4bt-clickhouse-1" "data4bt_clickhouse_1" "data4bt-clickhouse" "web3qt-clickhouse-1"; do
        if docker ps --format "table {{.Names}}" | grep -q "^${container_name}$" 2>/dev/null; then
            if docker exec "$container_name" clickhouse-client --query "SELECT 1" &>/dev/null; then
                echo "使用Docker容器: $container_name"
                cleanup_database "docker exec $container_name clickhouse-client --query"
                db_cleaned=true
                break
            fi
        fi
    done
fi

# 方式4: HTTP API直接连接
if [ "$db_cleaned" = false ]; then
    if curl -s http://localhost:8123/ping | grep -q "Ok" 2>/dev/null; then
        echo "使用HTTP API连接ClickHouse..."
        cleanup_database "curl_http_api"
        db_cleaned=true
    fi
fi

# 如果所有方式都失败
if [ "$db_cleaned" = false ]; then
    echo "⚠️  未找到可访问的ClickHouse实例"
    echo "请确保ClickHouse正在运行，然后重新执行此脚本"
    echo "或者手动清理数据库后继续"
fi

# 4. 清理临时文件和日志
echo
echo "🧽 清理临时文件..."

# 清理Go构建缓存中的临时文件
rm -rf /tmp/go-build* 2>/dev/null || true
rm -rf /tmp/binance-data-loader* 2>/dev/null || true

# 清理项目中的日志文件
rm -f *.log 2>/dev/null || true
rm -rf logs/*.log 2>/dev/null || true

# 清理可能的Docker volume（如果用户明确要求）
echo "检查Docker volumes..."
if docker volume ls | grep -q data4bt; then
    echo "发现data4bt相关的Docker volumes"
    echo "注意：脚本不会删除Docker volumes，只清理数据库表"
    echo "如需删除volumes，请手动执行："
    echo "  docker volume ls | grep data4bt | awk '{print \$2}' | xargs docker volume rm"
    echo "⏭️  保留Docker volumes和容器"
fi

echo "✅ 临时文件清理完成"

# 5. 总结和下一步指导
echo
echo "="*50
echo "🎉 完全重置完成！"
echo "="*50
echo
echo "📋 已完成的操作："
echo "  ✅ 停止了所有运行的程序"
echo "  ✅ 清理并重置了状态文件"
if [ "$db_cleaned" = true ]; then
    echo "  ✅ 清理了ClickHouse数据库"
else
    echo "  ⚠️  ClickHouse数据库需要手动清理"
fi
echo "  ✅ 清理了临时文件和日志"
echo
echo "🚀 下一步操作（推荐顺序）："
echo "  1️⃣  启动服务: ./start.sh"
echo "  2️⃣  测试新的Ctrl+C功能: go run cmd/main.go -cmd=run"
echo
echo "💡 或者分步执行："
echo "  • 启动ClickHouse: docker-compose up -d"
echo "  • 初始化数据库: go run cmd/main.go -cmd=init-db"
echo "  • 发现交易对: go run cmd/main.go -cmd=discover"
echo "  • 开始数据加载: go run cmd/main.go -cmd=run"
echo
echo "⚡ 现在支持改进的Ctrl+C信号处理:"
echo "  • 第一次 Ctrl+C: 优雅关闭（10秒超时）"
echo "  • 第二次 Ctrl+C: 立即强制退出"
echo
echo "========================================"