#!/bin/bash

# Data4BT 数据库清理脚本
# 用于清空ClickHouse数据库中的所有K线数据

set -e

echo "🗑️  Data4BT 数据库清理工具"
echo "==========================="
echo ""
echo "⚠️  警告：此操作将永久删除以下数据表中的所有数据："
echo "- klines_1m (1分钟K线数据)"
echo "- klines_5m (5分钟K线数据)"
echo "- klines_15m (15分钟K线数据)"
echo "- klines_1h (1小时K线数据)"
echo "- klines_4h (4小时K线数据)"
echo "- klines_1d (1天K线数据)"
echo "- symbol_progress (进度状态数据)"
echo ""
echo "💾 数据库：data4BT"
echo ""

# 安全确认
read -p "❓ 确定要清空所有数据吗？此操作不可恢复！(输入 'YES' 确认): " -r
echo ""
if [ "$REPLY" != "YES" ]; then
    echo "❌ 操作已取消"
    exit 0
fi

echo "🔍 检查环境..."

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

# 检查ClickHouse连接
echo "🔗 检查ClickHouse连接..."
if ! curl -s http://localhost:8123/ping | grep -q "Ok"; then
    echo "❌ 错误：无法连接到ClickHouse，请确保服务正在运行"
    echo "可以运行 ./start.sh 启动服务"
    exit 1
fi

echo "✅ ClickHouse连接正常"
echo ""
echo "🗑️  开始清理数据库..."

# 检查clear_database.go文件是否存在
if [ -f "clear_database.go" ]; then
    echo "📋 使用专用清理程序..."
    go run clear_database.go
else
    echo "📋 使用主程序清理功能..."
    # 如果没有专用清理程序，可以使用主程序的清理功能
    # 这里可以添加直接的SQL清理命令
    echo "🔄 清理数据表..."
    
    # 清理各个时间周期的K线数据表
    TABLES=("klines_1m" "klines_5m" "klines_15m" "klines_1h" "klines_4h" "klines_1d" "symbol_progress")
    
    for table in "${TABLES[@]}"; do
        echo "   清理表: $table"
        curl -s -X POST "http://localhost:8123/" \
             --data "TRUNCATE TABLE IF EXISTS data4BT.$table" \
             --user "default:123456" > /dev/null
        if [ $? -eq 0 ]; then
            echo "   ✅ $table 清理完成"
        else
            echo "   ⚠️  $table 清理可能失败"
        fi
    done
fi

echo ""
echo "📊 验证清理结果..."
# 检查表是否为空
for table in "klines_1m" "klines_5m" "klines_15m" "klines_1h" "klines_4h" "klines_1d"; do
    count=$(curl -s -X POST "http://localhost:8123/" \
                 --data "SELECT COUNT(*) FROM data4BT.$table" \
                 --user "default:123456" 2>/dev/null || echo "error")
    if [ "$count" = "0" ]; then
        echo "✅ $table: 已清空"
    elif [ "$count" = "error" ]; then
        echo "⚠️  $table: 检查失败（表可能不存在）"
    else
        echo "❌ $table: 仍有 $count 条记录"
    fi
done

echo ""
echo "🎉 数据库清理完成！"
echo "=================="
echo ""
echo "📝 后续操作："
echo "   重新初始化数据库: go run cmd/main.go -cmd=init-db"
echo "   开始数据加载:     go run cmd/main.go -cmd=run"
echo "   并发模式加载:     go run cmd/main.go -cmd=concurrent"
echo "   查看状态:         ./status.sh"
echo "   启动完整服务:     ./start.sh"
echo ""