#!/bin/bash

# ClickHouse重启脚本
echo "正在重启ClickHouse容器..."

# 停止容器
echo "停止ClickHouse容器..."
docker compose down clickhouse

# 等待2秒
sleep 2

# 启动容器
echo "启动ClickHouse容器..."
docker compose up -d clickhouse

# 等待容器启动
echo "等待ClickHouse启动..."
sleep 10

# 检查容器状态
echo "检查容器状态:"
docker compose ps clickhouse

# 测试连接
echo "\n测试ClickHouse连接:"
for i in {1..5}; do
    echo "尝试连接 ($i/5)..."
    if curl -s http://localhost:8123/ping | grep -q "Ok"; then
        echo "✅ ClickHouse HTTP接口连接成功!"
        break
    else
        echo "❌ 连接失败，等待5秒后重试..."
        sleep 5
    fi
done

# 测试数据库查询
echo "\n测试数据库查询:"
response=$(curl -s -X POST http://localhost:8123/ --data "SHOW DATABASES" 2>&1)
if echo "$response" | grep -q "AUTHENTICATION_FAILED\|Authentication failed"; then
    echo "❌ 认证仍然失败，请检查用户配置"
    echo "错误信息: $response"
else
    echo "✅ 数据库查询成功!"
    echo "可用数据库: $response"
fi

echo "\n现在可以尝试运行主程序:"
echo "go run cmd/main.go -cmd=init-db"
echo "go run cmd/main.go -cmd=run"