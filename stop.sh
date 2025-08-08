#!/bin/bash

# Data4BT 一键停止脚本
# 停止ClickHouse服务和相关进程

set -e

echo "🛑 Data4BT 一键停止脚本"
echo "========================"
echo ""

# 检查Docker环境
if ! command -v docker &> /dev/null; then
    echo "❌ 错误：未找到Docker，无法停止容器"
    exit 1
fi

echo "🔍 查找运行中的Data4BT相关进程..."

# 首先检查并停止保存的数据加载器进程
if [ -f ".data_loader_pid" ]; then
    DATA_LOADER_PID=$(cat .data_loader_pid)
    echo "📋 检查保存的数据加载器进程 (PID: $DATA_LOADER_PID)..."
    if kill -0 $DATA_LOADER_PID 2>/dev/null; then
        echo "🔄 停止数据加载器进程: $DATA_LOADER_PID"
        kill -TERM $DATA_LOADER_PID 2>/dev/null || true
        sleep 3
        # 检查进程是否还在运行
        if kill -0 $DATA_LOADER_PID 2>/dev/null; then
            echo "🔨 强制停止数据加载器进程: $DATA_LOADER_PID"
            kill -KILL $DATA_LOADER_PID 2>/dev/null || true
        fi
        echo "✅ 数据加载器进程已停止"
    else
        echo "ℹ️  数据加载器进程已不存在"
    fi
    rm -f .data_loader_pid
else
    echo "ℹ️  未找到保存的数据加载器进程ID文件"
fi

echo ""
# 查找并停止其他Go进程
echo "📋 检查其他Go进程..."
GO_PIDS=$(pgrep -f "go run.*cmd/main.go" 2>/dev/null || true)
if [ -n "$GO_PIDS" ]; then
    echo "🔄 停止Go进程: $GO_PIDS"
    echo $GO_PIDS | xargs kill -TERM 2>/dev/null || true
    sleep 2
    # 如果进程仍在运行，强制杀死
    REMAINING_PIDS=$(pgrep -f "go run.*cmd/main.go" 2>/dev/null || true)
    if [ -n "$REMAINING_PIDS" ]; then
        echo "🔨 强制停止残留进程: $REMAINING_PIDS"
        echo $REMAINING_PIDS | xargs kill -KILL 2>/dev/null || true
    fi
    echo "✅ Go进程已停止"
else
    echo "ℹ️  未发现运行中的Go进程"
fi

echo ""
echo "🐳 检查ClickHouse容器..."

# 检查ClickHouse容器状态
CONTAINER_ID=$(docker ps -q --filter "name=clickhouse" --filter "status=running" | head -1)
if [ -n "$CONTAINER_ID" ]; then
    echo "🔄 停止ClickHouse容器: $CONTAINER_ID"
    
    # 询问用户是否要停止ClickHouse容器
    echo "⚠️  注意：检测到运行中的ClickHouse容器"
    echo "   根据配置，这可能是共享的ClickHouse容器，也被其他项目使用"
    echo ""
    read -p "是否要停止ClickHouse容器？(y/N): " -n 1 -r
    echo ""
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker stop $CONTAINER_ID
        echo "✅ ClickHouse容器已停止"
    else
        echo "ℹ️  保持ClickHouse容器运行"
    fi
else
    echo "ℹ️  未发现运行中的ClickHouse容器"
fi

echo ""
echo "🧹 清理临时文件..."

# 清理可能的临时文件
if [ -d "/tmp/data4bt" ]; then
    rm -rf /tmp/data4bt
    echo "✅ 清理临时目录"
fi

# 清理Go模块缓存中的临时文件（可选）
if [ -d "./tmp" ]; then
    rm -rf ./tmp
    echo "✅ 清理项目临时目录"
fi

echo ""
echo "📊 检查剩余进程..."

# 检查是否还有相关进程
REMAINING_PROCESSES=$(pgrep -f "data4bt\|binance-data-loader" 2>/dev/null || true)
if [ -n "$REMAINING_PROCESSES" ]; then
    echo "⚠️  发现剩余相关进程:"
    ps -p $REMAINING_PROCESSES -o pid,ppid,cmd 2>/dev/null || true
    echo ""
    echo "如需手动清理，请运行:"
    echo "   kill $REMAINING_PROCESSES"
else
    echo "✅ 未发现剩余相关进程"
fi

echo ""
echo "🎉 Data4BT 停止完成!"
echo "=================="
echo ""
echo "📝 提示:"
echo "   重新启动:     ./start.sh"
echo "   查看状态:     docker ps"
echo "   清空数据库:   ./clear_database.sh"
echo ""
echo "💡 如果需要完全重置环境，请运行清空数据库脚本"
echo ""