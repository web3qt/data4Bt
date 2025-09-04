#!/bin/bash

# start_functions.sh - 启动脚本公共函数库
# 提供环境检查、进程管理、信号处理等核心功能

set -euo pipefail

# =============================================================================
# 全局变量
# =============================================================================

# 脚本配置
SCRIPT_NAME="data4bt-starter"
PID_FILE=".data_loader_pid"
LOG_DIR="logs"
STATE_DIR="state"
CONFIG_FILE="config.yml"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志级别
LOG_LEVEL_INFO="INFO"
LOG_LEVEL_WARN="WARN"
LOG_LEVEL_ERROR="ERROR"
LOG_LEVEL_DEBUG="DEBUG"

# 进程管理
GRACEFUL_SHUTDOWN_TIMEOUT=30
FORCE_KILL_TIMEOUT=10
MAX_STARTUP_WAIT=60

# =============================================================================
# 日志和输出函数
# =============================================================================

# 格式化日志输出
log_message() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case "$level" in
        "$LOG_LEVEL_INFO")
            echo -e "${GREEN}[INFO]${NC} ${timestamp} - $message"
            ;;
        "$LOG_LEVEL_WARN")
            echo -e "${YELLOW}[WARN]${NC} ${timestamp} - $message"
            ;;
        "$LOG_LEVEL_ERROR")
            echo -e "${RED}[ERROR]${NC} ${timestamp} - $message" >&2
            ;;
        "$LOG_LEVEL_DEBUG")
            if [ "${DEBUG:-false}" = "true" ]; then
                echo -e "${BLUE}[DEBUG]${NC} ${timestamp} - $message"
            fi
            ;;
    esac
}

# 便捷日志函数
log_info() { log_message "$LOG_LEVEL_INFO" "$1"; }
log_warn() { log_message "$LOG_LEVEL_WARN" "$1"; }
log_error() { log_message "$LOG_LEVEL_ERROR" "$1"; }
log_debug() { log_message "$LOG_LEVEL_DEBUG" "$1"; }

# 显示带图标的状态信息
show_status() {
    local icon="$1"
    local message="$2"
    echo -e "$icon $message"
}

# =============================================================================
# 环境检查函数
# =============================================================================

# 检查必要的命令是否存在（简化版）
check_required_commands_simple() {
    log_info "检查必要的命令..."
    
    local core_commands=("go" "curl")
    local missing_commands=()
    
    # 检查核心命令
    for cmd in "${core_commands[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing_commands+=("$cmd")
        fi
    done
    
    if [ ${#missing_commands[@]} -gt 0 ]; then
        log_error "缺少必要的命令: ${missing_commands[*]}"
        return 1
    fi
    
    show_status "✅" "所有必要命令已安装"
    return 0
}

# 检查必要的命令是否存在（保持向后兼容）
check_required_commands() {
    check_required_commands_simple
}

# 检查Docker环境
check_docker_environment() {
    log_info "检查Docker环境..."
    
    # 检查Docker是否安装
    if ! command -v docker >/dev/null 2>&1; then
        # 在生产环境模式下，Docker不是必需的
        if [ "${ENV_MODE:-}" = "prod" ]; then
            log_info "生产环境模式：跳过Docker检查"
            return 0
        fi
        log_warn "Docker未安装"
        offer_docker_installation
        return 1
    fi
    
    # 检查Docker服务是否运行
    if ! docker info >/dev/null 2>&1; then
        # 在生产环境模式下，尝试更宽松的检测
        if [ "${ENV_MODE:-}" = "prod" ]; then
            log_info "生产环境模式：Docker服务检测失败，但继续执行"
            log_debug "Docker info命令失败，可能是权限问题或服务配置问题"
            return 0
        fi
        log_warn "Docker服务未运行"
        log_info "请启动Docker服务:"
        echo "  - macOS: 启动Docker Desktop"
        echo "  - Linux: sudo systemctl start docker"
        echo "  - 或运行: sudo dockerd"
        return 1
    fi
    
    # 检查Docker Compose（仅在非生产环境）
    if [ "${ENV_MODE:-}" != "prod" ]; then
        if ! command -v docker-compose >/dev/null 2>&1 && ! docker compose version >/dev/null 2>&1; then
            log_warn "Docker Compose未安装"
            offer_docker_compose_installation
            return 1
        fi
    fi
    
    show_status "✅" "Docker环境正常"
    return 0
}

# 提供Docker安装指导
offer_docker_installation() {
    log_info "Docker安装指导:"
    echo ""
    echo "请选择你的操作系统安装Docker:"
    echo ""
    echo "🖥️  macOS:"
    echo "   - 下载Docker Desktop: https://www.docker.com/products/docker-desktop/"
    echo "   - 或使用Homebrew: brew install --cask docker"
    echo ""
    echo "🐧 Ubuntu/Debian:"
    echo "   curl -fsSL https://get.docker.com -o get-docker.sh"
    echo "   sudo sh get-docker.sh"
    echo ""
    echo "🔴 CentOS/RHEL:"
    echo "   sudo yum install -y docker"
    echo "   sudo systemctl start docker"
    echo ""
    echo "📦 或者使用官方一键安装脚本:"
    echo "   curl -fsSL https://get.docker.com | sh"
    echo ""
    
    # 询问是否尝试自动安装
    if command -v curl >/dev/null 2>&1; then
        echo -n "是否尝试自动安装Docker? (y/N): "
        read -r response
        if [[ "$response" =~ ^[Yy]$ ]]; then
            attempt_docker_installation
        fi
    fi
}

# 尝试自动安装Docker
attempt_docker_installation() {
    log_info "尝试自动安装Docker..."
    
    # 检测操作系统
    if [[ "$OSTYPE" == "darwin"* ]]; then
        log_info "检测到macOS，请手动安装Docker Desktop"
        return 1
    elif command -v apt-get >/dev/null 2>&1; then
        # Ubuntu/Debian
        log_info "检测到Ubuntu/Debian，尝试安装Docker..."
        if curl -fsSL https://get.docker.com -o get-docker.sh && sudo sh get-docker.sh; then
            sudo systemctl start docker
            sudo usermod -aG docker "$USER"
            log_success "Docker安装完成，请重新登录或运行: newgrp docker"
            return 0
        fi
    elif command -v yum >/dev/null 2>&1; then
        # CentOS/RHEL
        log_info "检测到CentOS/RHEL，尝试安装Docker..."
        if sudo yum install -y docker; then
            sudo systemctl start docker
            sudo systemctl enable docker
            sudo usermod -aG docker "$USER"
            log_success "Docker安装完成"
            return 0
        fi
    fi
    
    log_error "自动安装失败，请手动安装Docker"
    return 1
}

# 提供Docker Compose安装指导
offer_docker_compose_installation() {
    log_info "Docker Compose安装指导:"
    echo ""
    echo "📦 安装Docker Compose:"
    echo "   sudo curl -L \"https://github.com/docker/compose/releases/latest/download/docker-compose-\$(uname -s)-\$(uname -m)\" -o /usr/local/bin/docker-compose"
    echo "   sudo chmod +x /usr/local/bin/docker-compose"
    echo ""
    echo "或者使用pip安装:"
    echo "   pip install docker-compose"
    echo ""
}

# 检查Go环境
check_go_environment() {
    log_info "检查Go环境..."
    
    if ! command -v go >/dev/null 2>&1; then
        log_error "Go未安装或不在PATH中"
        return 1
    fi
    
    local go_version
    go_version=$(go version | awk '{print $3}' | sed 's/go//')
    log_debug "Go版本: $go_version"
    
    # 检查Go模块
    if [ ! -f "go.mod" ]; then
        log_error "go.mod文件不存在"
        return 1
    fi
    
    show_status "✅" "Go环境检查通过 (版本: $go_version)"
    return 0
}

# 检查ClickHouse连接（简化版，不管理容器）
check_clickhouse_connection() {
    log_info "检查ClickHouse连接..."
    
    # 尝试通过HTTP接口测试连接
    if test_clickhouse_direct_connection; then
        show_status "✅" "ClickHouse服务连接正常"
        return 0
    fi
    
    log_warn "ClickHouse连接失败"
    return 1
}

# 检查ClickHouse容器（保持向后兼容，但简化逻辑）
check_clickhouse_container() {
    log_info "检查ClickHouse服务..."
    
    # 直接尝试连接测试
    if test_clickhouse_direct_connection; then
        show_status "✅" "ClickHouse服务连接正常（直接连接）"
        echo "direct-connection"
        return 0
    fi
    
    log_warn "ClickHouse连接失败，程序启动后将重试"
    return 1
}

# 测试ClickHouse连接
test_clickhouse_connection() {
    local container_name="$1"
    
    log_debug "测试ClickHouse连接: $container_name"
    
    # 根据容器类型选择认证方式
    if [ "$container_name" = "data4bt-clickhouse" ]; then
        # 新容器使用密码认证
        if docker exec "$container_name" clickhouse-client --user=default --password=123456 --query "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
    elif [ "$container_name" = "shared-clickhouse" ]; then
        # 共享容器可能不需要密码
        if docker exec "$container_name" clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
    else
        # 其他容器，尝试多种认证方式
        if docker exec "$container_name" clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
            return 0
        elif docker exec "$container_name" clickhouse-client --user=default --password=123456 --query "SELECT 1" >/dev/null 2>&1; then
            return 0
        elif docker exec "$container_name" clickhouse-client --user=default --password="" --query "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
    fi
    
    return 1
}

# 测试ClickHouse直接连接（用于生产环境）
test_clickhouse_direct_connection() {
    log_debug "测试ClickHouse直接连接"
    
    # 尝试通过HTTP接口测试连接
    if command -v curl >/dev/null 2>&1; then
        # 尝试连接8123端口（HTTP接口）
        if curl -s --connect-timeout 5 "http://localhost:8123/ping" >/dev/null 2>&1; then
            log_debug "ClickHouse HTTP接口连接成功 (localhost:8123)"
            return 0
        fi
        
        # 尝试连接外部地址
        if curl -s --connect-timeout 5 "http://www.linklayer.ai:8123/ping" >/dev/null 2>&1; then
            log_debug "ClickHouse HTTP接口连接成功 (www.linklayer.ai:8123)"
            return 0
        fi
    fi
    
    # 尝试使用clickhouse-client（如果可用）
    if command -v clickhouse-client >/dev/null 2>&1; then
        if clickhouse-client --host=localhost --port=9000 --query="SELECT 1" >/dev/null 2>&1; then
            log_debug "ClickHouse客户端连接成功 (localhost:9000)"
            return 0
        fi
    fi
    
    log_debug "ClickHouse直接连接失败"
    return 1
}

# 启动ClickHouse容器（已禁用 - 生产环境安全）
start_clickhouse_container() {
    log_warn "容器管理功能已禁用，请确保ClickHouse服务已手动启动"
    log_info "程序将尝试连接配置文件中指定的ClickHouse服务"
    return 1
}

# 等待ClickHouse启动完成（已禁用 - 生产环境安全）
wait_for_clickhouse_startup() {
    log_warn "容器管理功能已禁用"
    return 1
}

# 直接启动ClickHouse容器（已禁用 - 生产环境安全）
start_clickhouse_directly() {
    log_warn "容器创建功能已禁用，请手动确保ClickHouse服务运行"
    return 1
}

# 检查网络连接
check_network_connectivity() {
    log_info "检查网络连接..."
    
    local max_retries=3
    local retry_count=0
    local test_url="https://data.binance.vision/"
    
    while [ $retry_count -lt $max_retries ]; do
        if curl -s --connect-timeout 10 --max-time 30 "$test_url" >/dev/null 2>&1; then
            show_status "✅" "网络连接正常"
            return 0
        fi
        
        retry_count=$((retry_count + 1))
        if [ $retry_count -lt $max_retries ]; then
            log_warn "网络连接失败，重试 $retry_count/$max_retries"
            sleep 5
        fi
    done
    
    log_warn "网络连接检查失败，程序将继续运行（可能使用缓存或降级模式）"
    return 1
}

# 检查配置文件
check_config_file() {
    log_info "检查配置文件..."
    
    # 检查配置文件路径，支持configs目录
    local config_path="$CONFIG_FILE"
    if [[ "$CONFIG_FILE" == config-*.yml ]] && [ ! -f "$CONFIG_FILE" ]; then
        config_path="configs/$CONFIG_FILE"
    fi
    
    if [ ! -f "$config_path" ]; then
        log_error "配置文件 $config_path 不存在"
        if [[ "$CONFIG_FILE" == config-*.yml ]]; then
            log_info "提示: 请确保配置文件存在于 configs/ 目录中"
            log_info "或者从示例文件复制: cp configs/$CONFIG_FILE.example configs/$CONFIG_FILE"
        fi
        return 1
    fi
    
    # 基本的文件可读性检查
    if [ ! -r "$config_path" ]; then
        log_error "配置文件 $config_path 不可读"
        return 1
    fi
    
    # 简单的YAML格式检查（检查是否包含基本的YAML结构）
    if ! grep -q ":" "$config_path"; then
        log_error "配置文件 $config_path 不是有效的YAML格式"
        return 1
    fi
    
    # 更新CONFIG_FILE为实际路径
    CONFIG_FILE="$config_path"
    show_status "✅" "配置文件检查通过: $CONFIG_FILE"
    return 0
}

# =============================================================================
# 进程管理函数
# =============================================================================

# 检查进程是否存在
is_process_running() {
    local pid="$1"
    
    if [ -z "$pid" ]; then
        return 1
    fi
    
    if kill -0 "$pid" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# 从PID文件读取进程ID
get_pid_from_file() {
    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE" 2>/dev/null)
        if [ -n "$pid" ] && is_process_running "$pid"; then
            echo "$pid"
            return 0
        fi
    fi
    return 1
}

# 查找运行中的相关进程
find_running_processes() {
    local pids_list=""
    
    # 方法1: 从PID文件读取
    local saved_pid
    if saved_pid=$(get_pid_from_file); then
        pids_list="$saved_pid"
    fi
    
    # 方法2: 通过进程名查找
    local found_pids
    found_pids=$(pgrep -f "go run.*cmd/main.go\\|data4bt\\|binance-data-loader" 2>/dev/null || true)
    if [ -n "$found_pids" ]; then
        # 合并PID列表，去重
        for pid in $found_pids; do
            if [[ ! " $pids_list " =~ " $pid " ]]; then
                pids_list="$pids_list $pid"
            fi
        done
    fi
    
    # 输出去重后的PID列表，每行一个
    if [ -n "$pids_list" ]; then
        echo "$pids_list" | tr ' ' '\n' | sort -u | grep -v '^$'
    fi
}

# 执行启动前检查
perform_startup_checks() {
    log_info "执行启动前检查..."
    
    # 检查是否已有实例在运行
    local running_pids
    running_pids=$(find_running_processes)
    
    if [ -n "$running_pids" ]; then
        log_warn "发现运行中的实例: $running_pids"
        
        if [ "${RUN_MODE:-foreground}" != "test" ]; then
            log_info "停止现有实例以避免冲突..."
            stop_existing_processes
        else
            log_error "测试模式不允许与现有实例并行运行"
            return 1
        fi
    fi
    
    return 0
}

# 停止现有进程
stop_existing_processes() {
    log_info "检查并停止现有进程..."
    
    # 使用兼容的方式获取PID列表
    local pids_list
    pids_list=$(find_running_processes)
    
    if [ -z "$pids_list" ]; then
        log_debug "未发现运行中的进程"
        return 0
    fi
    
    log_info "发现运行中的进程: $pids_list"
    
    # 发送TERM信号
    for pid in $pids_list; do
        if is_process_running "$pid"; then
            log_debug "向进程 $pid 发送TERM信号"
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    
    # 等待进程退出
    local wait_count=0
    while [ $wait_count -lt $GRACEFUL_SHUTDOWN_TIMEOUT ]; do
        local remaining_pids=""
        for pid in $pids_list; do
            if is_process_running "$pid"; then
                remaining_pids="$remaining_pids $pid"
            fi
        done
        
        if [ -z "$remaining_pids" ]; then
            show_status "✅" "所有进程已正常退出"
            break
        fi
        
        sleep 1
        wait_count=$((wait_count + 1))
    done
    
    # 强制终止剩余进程
    for pid in $pids_list; do
        if is_process_running "$pid"; then
            log_warn "强制终止进程: $pid"
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done
    
    # 清理PID文件
    if [ -f "$PID_FILE" ]; then
        rm -f "$PID_FILE"
        log_debug "已清理PID文件"
    fi
    
    return 0
}

# 等待进程启动
wait_for_process_start() {
    local pid="$1"
    local wait_count=0
    
    log_info "等待进程启动 (PID: $pid)..."
    
    while [ $wait_count -lt $MAX_STARTUP_WAIT ]; do
        if is_process_running "$pid"; then
            # 额外检查进程是否真正开始工作（可以通过端口检查等）
            if check_process_health "$pid"; then
                show_status "✅" "进程启动成功 (PID: $pid)"
                return 0
            fi
        else
            log_error "进程启动失败或异常退出 (PID: $pid)"
            return 1
        fi
        
        sleep 1
        wait_count=$((wait_count + 1))
    done
    
    log_error "进程启动超时 (PID: $pid)"
    return 1
}

# 检查进程健康状态
check_process_health() {
    local pid="$1"
    
    # 基本的进程存在检查
    if ! is_process_running "$pid"; then
        return 1
    fi
    
    # 可以添加更多健康检查，比如端口监听、HTTP健康检查等
    # 这里简化为基本检查
    return 0
}

# =============================================================================
# 信号处理函数
# =============================================================================

# 全局变量用于信号处理
SIGNAL_RECEIVED=false
CHILD_PID=""
SHUTDOWN_IN_PROGRESS=false

# 优雅关闭处理函数
graceful_shutdown() {
    # 防止重复执行
    if [ "$SHUTDOWN_IN_PROGRESS" = "true" ]; then
        log_debug "关闭已在进行中，忽略重复信号"
        return
    fi
    
    SHUTDOWN_IN_PROGRESS=true
    SIGNAL_RECEIVED=true
    
    echo ""  # 换行，美化输出
    log_info "接收到停止信号，正在优雅关闭..."
    
    if [ -n "$CHILD_PID" ] && is_process_running "$CHILD_PID"; then
        log_info "停止子进程 (PID: $CHILD_PID)..."
        
        # 发送TERM信号
        kill -TERM "$CHILD_PID" 2>/dev/null || true
        
        # 等待进程退出
        local wait_count=0
        while [ $wait_count -lt $GRACEFUL_SHUTDOWN_TIMEOUT ] && is_process_running "$CHILD_PID"; do
            sleep 1
            wait_count=$((wait_count + 1))
        done
        
        # 如果还没退出，强制终止
        if is_process_running "$CHILD_PID"; then
            log_warn "优雅关闭超时，强制终止进程 (PID: $CHILD_PID)"
            kill -KILL "$CHILD_PID" 2>/dev/null || true
            sleep 2
        fi
        
        if ! is_process_running "$CHILD_PID"; then
            show_status "✅" "进程已成功停止"
        fi
    fi
    
    # 清理资源
    cleanup_resources
    
    log_info "程序已退出"
    exit 0
}

# 设置信号处理
setup_signal_handling() {
    local child_pid="$1"
    CHILD_PID="$child_pid"
    
    log_debug "设置信号处理 (子进程PID: $child_pid)"
    
    # 设置trap，捕获INT和TERM信号
    trap 'graceful_shutdown' INT TERM
    
    # 设置EXIT trap用于清理
    trap 'cleanup_on_exit' EXIT
}

# 退出时清理
cleanup_on_exit() {
    if [ "$SIGNAL_RECEIVED" != "true" ] && [ -n "$CHILD_PID" ] && is_process_running "$CHILD_PID"; then
        log_debug "脚本退出时清理子进程"
        kill -TERM "$CHILD_PID" 2>/dev/null || true
    fi
}

# =============================================================================
# 资源管理函数
# =============================================================================

# 创建必要的目录
create_directories() {
    log_info "创建必要目录..."
    
    # 使用兼容的方式处理目录列表
    local directories="$LOG_DIR $STATE_DIR tools"
    
    for dir in $directories; do
        if [ ! -d "$dir" ]; then
            mkdir -p "$dir"
            log_debug "创建目录: $dir"
        fi
    done
    
    show_status "✅" "目录创建完成"
}

# 设置系统资源限制
setup_resource_limits() {
    log_info "配置系统资源限制..."
    
    # 设置Go环境变量
    export GOMAXPROCS=4
    export GOMEMLIMIT=4GiB
    export GODEBUG=gctrace=0
    
    log_debug "Go环境变量: GOMAXPROCS=$GOMAXPROCS, GOMEMLIMIT=$GOMEMLIMIT"
    
    # 设置系统资源限制（如果支持）
    if command -v ulimit >/dev/null 2>&1; then
        # macOS上某些ulimit选项可能不支持，忽略错误
        ulimit -v 4194304 2>/dev/null || log_debug "虚拟内存限制设置失败（忽略）"
        ulimit -m 4194304 2>/dev/null || log_debug "物理内存限制设置失败（忽略）"
    fi
    
    show_status "✅" "资源限制配置完成"
}

# 清理资源
cleanup_resources() {
    log_debug "清理资源..."
    
    # 清理PID文件
    if [ -f "$PID_FILE" ]; then
        rm -f "$PID_FILE"
        log_debug "已清理PID文件: $PID_FILE"
    fi
    
    # 可以添加其他清理操作
    # 比如清理临时文件、关闭网络连接等
}

# =============================================================================
# 启动模式函数
# =============================================================================

# 前台运行模式
start_foreground_mode() {
    log_info "前台模式启动..."
    show_status "💡" "提示: 使用 './start.sh --background' 可在后台运行"
    echo ""
    
    # 启动Go程序
    go run cmd/main.go "$@" &
    local child_pid=$!
    
    # 保存PID
    echo "$child_pid" > "$PID_FILE"
    log_info "程序已启动 (PID: $child_pid)"
    show_status "📊" "监控面板: http://localhost:8890"
    echo ""
    
    # 设置信号处理
    setup_signal_handling "$child_pid"
    
    # 等待进程结束
    if wait "$child_pid"; then
        log_info "程序正常退出"
    else
        local exit_code=$?
        log_warn "程序异常退出 (退出码: $exit_code)"
    fi
    
    # 清理PID文件
    rm -f "$PID_FILE"
}

# 后台运行模式
start_background_mode() {
    log_info "后台模式启动..."
    
    # 启动Go程序
    nohup go run cmd/main.go "$@" >> "$LOG_DIR/data_loader.log" 2>&1 &
    local child_pid=$!
    
    # 保存PID
    echo "$child_pid" > "$PID_FILE"
    
    # 等待进程启动
    if wait_for_process_start "$child_pid"; then
        show_status "✅" "程序已在后台启动 (PID: $child_pid)"
        show_status "📊" "监控面板: http://localhost:8890"
        show_status "📝" "查看日志: tail -f $LOG_DIR/data_loader.log"
        show_status "🛑" "停止程序: ./stop.sh"
    else
        log_error "后台启动失败"
        rm -f "$PID_FILE"
        return 1
    fi
}

# 测试模式
start_test_mode() {
    local symbols="${1:-BTCUSDT}"
    
    log_info "测试模式启动..."
    show_status "🧪" "测试交易对: $symbols"
    
    # 直接运行，不需要后台
    go run cmd/main.go -cmd=run -config="$CONFIG_FILE" -symbols="$symbols"
}

# =============================================================================
# 主要功能函数
# =============================================================================

# 执行完整的环境检查
perform_environment_check() {
    log_info "开始环境检查..."
    
    # 检查必要命令（简化版，不强制要求Docker）
    if ! check_required_commands_simple; then
        log_error "基础命令检查失败，请先安装必要的软件"
        return 1
    fi
    
    # 检查Go环境
    if ! check_go_environment; then
        log_error "Go环境检查失败，请先安装Go 1.19+"
        return 1
    fi
    
    # 检查配置文件
    if ! check_config_file; then
        log_error "配置文件检查失败，请确保config.yml存在且格式正确"
        return 1
    fi
    
    # 检查ClickHouse连接（简化版，不管理容器）
    if check_clickhouse_connection; then
        log_info "ClickHouse服务连接正常"
    else
        log_warn "ClickHouse连接测试失败，程序启动后将重试连接"
        log_info "请确保ClickHouse服务正在运行并且配置正确"
    fi
    
    # 检查网络连接
    check_network_connectivity || true  # 网络失败不阻止启动
    
    show_status "✅" "环境检查完成"
    return 0
}

# 显示ClickHouse帮助信息（简化版）
show_clickhouse_help() {
    echo ""
    log_info "ClickHouse连接失败，可能的解决方案:"
    echo ""
    echo "1. 📝 检查配置文件:"
    echo "   确保 configs/config-*.yml 中的ClickHouse配置正确"
    echo "   检查主机地址、端口、用户名和密码"
    echo ""
    echo "2. 🔍 检查ClickHouse服务状态:"
    echo "   curl http://localhost:8123/ping"
    echo "   # 或检查您配置的ClickHouse地址"
    echo ""
    echo "3. 🔧 确保ClickHouse服务运行:"
    echo "   请联系系统管理员确保ClickHouse服务正常运行"
    echo "   或检查现有的Docker容器状态"
    echo ""
    echo "4. 🌐 网络连接检查:"
    echo "   确保网络连接正常，防火墙设置正确"
    echo ""
}

# 准备启动环境
prepare_startup_environment() {
    log_info "准备启动环境..."
    
    # 创建目录
    create_directories
    
    # 设置资源限制
    setup_resource_limits
    
    # 停止现有进程
    stop_existing_processes
    
    show_status "✅" "启动环境准备完成"
}

# 显示启动信息
show_startup_info() {
    echo ""
    show_status "🚀" "启动币安数据加载器 (优化版)"
    show_status "⏰" "时间: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
}

# 显示完成信息
show_completion_info() {
    echo ""
    show_status "✅" "数据加载器启动完成"
}

# =============================================================================
# 错误处理函数
# =============================================================================

# 错误处理函数
handle_error() {
    local exit_code=$1
    local line_number=$2
    
    log_error "脚本在第 $line_number 行失败，退出码: $exit_code"
    
    # 清理资源
    cleanup_resources
    
    exit "$exit_code"
}

# 设置错误处理
setup_error_handling() {
    # 设置错误捕获
    trap 'handle_error $? $LINENO' ERR
}

# =============================================================================
# 初始化函数
# =============================================================================

# 初始化函数库
init_start_functions() {
    # 设置错误处理
    setup_error_handling
    
    # 初始化日志
    log_debug "start_functions.sh 已加载"
    
    # 检查运行环境
    if [ "${BASH_VERSION:-}" = "" ]; then
        log_error "此脚本需要Bash环境"
        exit 1
    fi
    
    # 设置默认值
    DEBUG="${DEBUG:-false}"
}

# 自动初始化
init_start_functions

log_debug "start_functions.sh 函数库加载完成"