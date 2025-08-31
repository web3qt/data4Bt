#!/bin/bash

# 优化版数据加载器启动脚本
# 针对网络问题和稳定性进行优化

set -e

echo "🚀 启动币安数据加载器 (优化版)"
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 检查前置条件
echo "📋 检查前置条件..."

# 检查ClickHouse (支持新旧两种容器)
CLICKHOUSE_CONTAINER=""
if docker ps | grep -q data4bt-clickhouse; then
    CLICKHOUSE_CONTAINER="data4bt-clickhouse"
    echo "✅ 发现 data4bt-clickhouse 容器"
elif docker ps | grep -q shared-clickhouse; then
    CLICKHOUSE_CONTAINER="shared-clickhouse"
    echo "✅ 发现 shared-clickhouse 容器"
else
    echo "❌ ClickHouse容器未运行"
    echo "正在启动ClickHouse..."
    
    # 首先尝试使用智能启动脚本
    if [ -f "./start_clickhouse.sh" ]; then
        echo "🚀 使用智能启动脚本..."
        ./start_clickhouse.sh auto
        # 重新检测容器
        if docker ps | grep -q data4bt-clickhouse; then
            CLICKHOUSE_CONTAINER="data4bt-clickhouse"
        elif docker ps | grep -q shared-clickhouse; then
            CLICKHOUSE_CONTAINER="shared-clickhouse"
        fi
    else
        # 回退到传统方式
        docker compose up -d clickhouse
        echo "⏱️  等待ClickHouse启动..."
        sleep 30
        CLICKHOUSE_CONTAINER="data4bt-clickhouse"
    fi
fi

# 测试ClickHouse连接 (支持密码认证)
echo "🔍 测试ClickHouse连接..."
if [ "$CLICKHOUSE_CONTAINER" = "data4bt-clickhouse" ]; then
    # 新容器使用密码认证
    if docker exec $CLICKHOUSE_CONTAINER clickhouse-client --user=default --password=123456 --query "SELECT 1" > /dev/null 2>&1; then
        echo "✅ ClickHouse连接正常 (新容器，密码认证)"
    else
        echo "❌ ClickHouse连接失败 (新容器)"
        exit 1
    fi
elif [ "$CLICKHOUSE_CONTAINER" = "shared-clickhouse" ]; then
    # 共享容器可能不需要密码
    if docker exec $CLICKHOUSE_CONTAINER clickhouse-client --query "SELECT 1" > /dev/null 2>&1; then
        echo "✅ ClickHouse连接正常 (共享容器)"
    else
        echo "❌ ClickHouse连接失败 (共享容器)"
        exit 1
    fi
else
    echo "❌ 未找到有效的ClickHouse容器"
    exit 1
fi

# 检查网络连接
echo ""
echo "🌐 检查网络连接..."
MAX_RETRIES=3
RETRY_COUNT=0

check_network() {
    if curl -s --connect-timeout 10 --max-time 30 https://data.binance.vision/ > /dev/null 2>&1; then
        echo "✅ 币安数据源连接正常"
        return 0
    else
        echo "⚠️  币安数据源连接失败"
        return 1
    fi
}

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if check_network; then
        break
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            echo "🔄 网络重试 $RETRY_COUNT/$MAX_RETRIES，等待10秒..."
            sleep 10
        fi
    fi
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "❌ 网络连接检查失败，但程序将继续运行（可能使用缓存或降级模式）"
fi

# 创建必要的目录
echo ""
echo "📁 创建必要目录..."
mkdir -p logs state tools

# 设置资源限制
echo ""
echo "⚙️  配置系统资源..."

# 设置内存限制（如果支持）
if command -v ulimit > /dev/null; then
    # macOS上某些ulimit选项可能不支持，忽略错误
    ulimit -v 4194304 2>/dev/null || echo "⚠️  虚拟内存限制设置失败（忽略）"
    ulimit -m 4194304 2>/dev/null || echo "⚠️  物理内存限制设置失败（忽略）"
    echo "✅ 内存限制设置完成"
fi

# 停止已有进程
echo ""
echo "🔄 检查并停止现有进程..."
if [ -f .data_loader_pid ]; then
    OLD_PID=$(cat .data_loader_pid)
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "停止现有进程 (PID: $OLD_PID)..."
        kill "$OLD_PID" || true
        sleep 3
        if ps -p "$OLD_PID" > /dev/null 2>&1; then
            echo "强制终止进程..."
            kill -9 "$OLD_PID" || true
        fi
    fi
    rm -f .data_loader_pid
fi

# 检查配置文件
echo ""
if [ ! -f config.yml ]; then
    echo "❌ 配置文件 config.yml 不存在"
    exit 1
fi
echo "📝 使用配置文件: config.yml"

# 设置环境变量
export GOMAXPROCS=4  # 限制Go程序使用的CPU核心数
export GOMEMLIMIT=4GiB  # Go内存限制(使用GiB格式)
export GODEBUG=gctrace=0  # 关闭GC跟踪减少输出

# 启动程序
echo ""
echo "🎯 启动数据加载器..."
echo "配置: 并发处理模式"
echo "监控地址: http://localhost:8890"
echo "按 Ctrl+C 停止程序"
echo ""

# 启动方式选择
if [ "$1" = "--background" ] || [ "$1" = "-bg" ]; then
    # 后台运行模式
    echo "🔧 后台模式启动..."
    nohup go run cmd/main.go -cmd=run -config=config.yml \
        >> logs/data_loader.log 2>&1 &
    
    # 保存PID
    echo $! > .data_loader_pid
    echo "✅ 程序已在后台启动 (PID: $!)"
    echo "📊 监控面板: http://localhost:8890"
    echo "📝 查看日志: tail -f logs/data_loader.log"
    echo "🛑 停止程序: ./stop.sh"
    
elif [ "$1" = "--test" ] || [ "$1" = "-t" ]; then
    # 测试模式 - 只处理一个交易对
    echo "🧪 测试模式启动..."
    go run cmd/main.go -cmd=run -config=config.yml -symbols=BTCUSDT
    
else
    # 前台运行模式（默认）
    echo "🖥️  前台模式启动..."
    echo "💡 提示: 使用 './start.sh --background' 可在后台运行"
    echo ""
    
    # 捕获中断信号以便优雅关闭
    trap 'echo ""; echo "🛑 接收到停止信号，正在优雅关闭..."; kill -TERM $PID 2>/dev/null; wait $PID 2>/dev/null; exit 0' INT TERM
    
    go run cmd/main.go -cmd=run -config=config.yml &
    PID=$!
    echo $PID > .data_loader_pid
    
    echo "✅ 程序已启动 (PID: $PID)"
    echo "📊 监控面板: http://localhost:8890"
    echo ""
    
    # 等待进程结束
    wait $PID
    rm -f .data_loader_pid
fi

echo ""
echo "✅ 数据加载器启动完成"