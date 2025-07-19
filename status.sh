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