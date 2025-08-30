#!/bin/bash

# ClickHouse 智能启动脚本
# 支持 Docker Compose 和传统方式
# 用法: ./start_clickhouse.sh [docker|legacy|auto]

set -e

echo "🐘 ClickHouse 智能启动脚本"
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 检查参数
MODE=${1:-auto}
echo "📋 启动模式: $MODE"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# 检查 Docker 环境
check_docker() {
    if command -v docker >/dev/null 2>&1 && command -v docker compose >/dev/null 2>&1; then
        if docker info >/dev/null 2>&1; then
            return 0
        else
            log_warning "Docker daemon 未运行"
            return 1
        fi
    else
        log_warning "Docker 或 Docker Compose 未安装"
        return 1
    fi
}

# 检查现有容器
check_existing_containers() {
    # 检查新容器
    if docker ps -q -f name=data4bt-clickhouse | grep -q .; then
        log_info "发现新的 data4bt-clickhouse 容器正在运行"
        return 0
    fi
    
    # 检查旧的共享容器
    if docker ps -q -f name=shared-clickhouse | grep -q .; then
        log_info "发现共享的 shared-clickhouse 容器正在运行"
        return 1
    fi
    
    return 2
}

# Docker 方式启动
start_with_docker() {
    log_info "使用 Docker Compose 启动 ClickHouse"
    
    # 检查配置文件
    if [ ! -f "docker-compose.yml" ]; then
        log_error "docker-compose.yml 文件不存在"
        return 1
    fi
    
    # 停止可能存在的旧容器
    log_info "清理可能的旧容器..."
    docker compose down -v 2>/dev/null || true
    docker stop data4bt-clickhouse 2>/dev/null || true
    docker rm data4bt-clickhouse 2>/dev/null || true
    
    # 启动服务
    log_info "启动 ClickHouse 服务..."
    docker compose up -d clickhouse
    
    # 等待启动
    log_info "等待 ClickHouse 启动..."
    local retries=12
    local count=0
    
    while [ $count -lt $retries ]; do
        if docker exec data4bt-clickhouse clickhouse-client --user=default --password=123456 --query "SELECT 1" >/dev/null 2>&1; then
            log_success "ClickHouse 启动成功！"
            return 0
        fi
        
        count=$((count + 1))
        echo -n "."
        sleep 5
    done
    
    log_error "ClickHouse 启动超时"
    return 1
}

# 传统方式启动 (使用现有容器)
start_with_legacy() {
    log_info "使用传统方式连接 ClickHouse"
    
    # 检查是否有共享容器
    if ! docker ps -q -f name=shared-clickhouse | grep -q .; then
        log_warning "未找到共享的 ClickHouse 容器"
        log_info "请确保 ClickHouse 服务在 localhost:9000 上运行"
        return 1
    fi
    
    # 测试连接
    log_info "测试连接到共享 ClickHouse..."
    if docker exec shared-clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
        log_success "成功连接到共享 ClickHouse 容器"
        return 0
    else
        log_error "无法连接到共享 ClickHouse 容器"
        return 1
    fi
}

# 测试连接并初始化数据库
test_and_init() {
    local use_password=""
    local container_name=""
    
    if docker ps -q -f name=data4bt-clickhouse | grep -q .; then
        container_name="data4bt-clickhouse"
        use_password="--password=123456"
    elif docker ps -q -f name=shared-clickhouse | grep -q .; then
        container_name="shared-clickhouse"
        # 共享容器可能没有密码
        use_password=""
    else
        log_error "未找到运行中的 ClickHouse 容器"
        return 1
    fi
    
    log_info "测试数据库连接..."
    
    # 测试基本连接
    if ! docker exec $container_name clickhouse-client --user=default $use_password --query "SELECT 1" >/dev/null 2>&1; then
        log_error "数据库连接失败"
        return 1
    fi
    
    log_success "数据库连接正常"
    
    # 检查数据库是否存在
    log_info "检查 data4BT 数据库..."
    if docker exec $container_name clickhouse-client --user=default $use_password --query "SHOW DATABASES" 2>/dev/null | grep -q "data4BT"; then
        log_success "data4BT 数据库已存在"
    else
        log_info "创建 data4BT 数据库..."
        if docker exec $container_name clickhouse-client --user=default $use_password --query "CREATE DATABASE IF NOT EXISTS data4BT" 2>/dev/null; then
            log_success "data4BT 数据库创建成功"
        else
            log_error "data4BT 数据库创建失败"
            return 1
        fi
    fi
    
    # 显示连接信息
    echo ""
    log_success "ClickHouse 已准备就绪！"
    echo "📋 连接信息:"
    echo "   容器: $container_name"
    echo "   地址: localhost:9000"
    echo "   HTTP: localhost:8123"
    echo "   用户: default"
    if [ -n "$use_password" ]; then
        echo "   密码: 123456"
    else
        echo "   密码: 无"
    fi
    echo "   数据库: data4BT"
    echo ""
}

# 主逻辑
main() {
    case "$MODE" in
        docker)
            if check_docker; then
                start_with_docker && test_and_init
            else
                log_error "Docker 环境不可用"
                exit 1
            fi
            ;;
        legacy)
            start_with_legacy && test_and_init
            ;;
        auto)
            # 自动检测最佳启动方式
            check_existing_containers
            local container_status=$?
            
            if [ $container_status -eq 0 ]; then
                log_info "发现现有新容器，使用现有服务"
                test_and_init
            elif [ $container_status -eq 1 ]; then
                log_info "发现共享容器，使用传统方式"
                start_with_legacy && test_and_init
            else
                if check_docker; then
                    log_info "未发现现有容器，使用 Docker Compose 启动"
                    start_with_docker && test_and_init
                else
                    log_error "无法启动 ClickHouse: Docker 不可用且无现有容器"
                    exit 1
                fi
            fi
            ;;
        *)
            echo "用法: $0 [docker|legacy|auto]"
            echo ""
            echo "模式说明:"
            echo "  docker  - 强制使用 Docker Compose 启动新容器"
            echo "  legacy  - 使用现有的共享容器"
            echo "  auto    - 自动检测并选择最佳方式 (默认)"
            exit 1
            ;;
    esac
}

# 设置脚本权限并运行
main "$@"

echo ""
echo "🚀 现在可以运行应用程序:"
echo "   go run cmd/main.go -cmd=init-db    # 初始化数据库表"
echo "   go run cmd/main.go -cmd=run        # 运行数据加载器"
echo "   ./start_optimized.sh               # 使用优化启动脚本"
echo ""