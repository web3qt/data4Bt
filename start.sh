#!/bin/bash

# Data4BT 一键启动脚本
# 启动ClickHouse服务并初始化数据库

set -e

echo "🚀 Data4BT 一键启动脚本"
echo "========================"
echo ""

# 检查Docker环境
if ! command -v docker &> /dev/null; then
    echo "❌ 错误：未找到Docker，请先安装Docker"
    exit 1
fi

# 检查Go环境
if ! command -v go &> /dev/null; then
    echo "❌ 错误：未找到Go环境，请先安装Go"
    exit 1
fi

# 检查配置文件
if [ ! -f "configs/config.yml" ]; then
    echo "❌ 错误：未找到配置文件 configs/config.yml"
    echo "请先创建配置文件"
    exit 1
fi

echo "📋 检查现有ClickHouse容器..."
# 检查是否有现有的ClickHouse容器在运行
CONTAINER_ID=$(docker ps -q --filter "name=clickhouse" --filter "status=running" | head -1)
if [ -n "$CONTAINER_ID" ]; then
    echo "✅ 发现运行中的ClickHouse容器: $CONTAINER_ID"
else
    echo "🔍 未发现运行中的ClickHouse容器，检查已停止的容器..."
    STOPPED_CONTAINER=$(docker ps -aq --filter "name=clickhouse" | head -1)
    if [ -n "$STOPPED_CONTAINER" ]; then
        echo "🔄 启动已存在的ClickHouse容器: $STOPPED_CONTAINER"
        docker start $STOPPED_CONTAINER
        sleep 5
    else
        echo "❌ 未找到ClickHouse容器"
        echo "请确保ClickHouse容器已创建并配置正确"
        echo "根据docker-compose.yml，应该使用现有的共享ClickHouse容器"
        exit 1
    fi
fi

echo ""
echo "⏳ 等待ClickHouse服务启动..."
sleep 10

# 测试ClickHouse连接
echo "🔗 测试ClickHouse连接..."
for i in {1..10}; do
    echo "尝试连接 ($i/10)..."
    if curl -s http://localhost:8123/ping | grep -q "Ok"; then
        echo "✅ ClickHouse HTTP接口连接成功!"
        break
    else
        if [ $i -eq 10 ]; then
            echo "❌ ClickHouse连接失败，请检查服务状态"
            echo "可以手动运行: docker ps 查看容器状态"
            exit 1
        fi
        echo "等待5秒后重试..."
        sleep 5
    fi
done

echo ""
echo "🗄️  初始化数据库..."
go run cmd/main.go -cmd=init-db
if [ $? -eq 0 ]; then
    echo "✅ 数据库初始化成功!"
else
    echo "❌ 数据库初始化失败"
    exit 1
fi

echo ""
echo "📊 检查系统状态..."
go run cmd/main.go -cmd=status

echo ""
echo "🚀 启动并发数据加载器..."
echo "========================"
echo "正在后台启动数据下载和处理服务..."
echo ""

# 启动并发数据加载器（后台运行）
nohup go run cmd/main.go -cmd=concurrent > logs/data_loader.log 2>&1 &
DATA_LOADER_PID=$!
echo "✅ 数据加载器已启动 (PID: $DATA_LOADER_PID)"
echo "📝 日志文件: logs/data_loader.log"

# 等待几秒让服务启动
sleep 5

echo ""
echo "🎉 Data4BT 系统启动完成!"
echo "========================"
echo ""
echo "📊 当前状态:"
go run cmd/main.go -cmd=status
echo ""
echo "📝 管理命令:"
echo "   查看状态:     ./status.sh"
echo "   查看日志:     tail -f logs/data_loader.log"
echo "   更新到最新:   go run cmd/main.go -cmd=update-latest"
echo "   停止服务:     ./stop.sh"
echo "   清空数据库:   ./clear_database.sh"
echo ""
echo "🌐 ClickHouse Web界面: http://localhost:8123/play"
echo "📊 数据库: data4BT"
echo "📁 进程ID已保存到: .data_loader_pid"

# 保存PID到文件，方便stop.sh使用
echo $DATA_LOADER_PID > .data_loader_pid
echo ""