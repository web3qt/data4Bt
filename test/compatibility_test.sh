#!/bin/bash

# Data4BT 兼容性测试套件
# 测试不同环境、配置和依赖版本下的兼容性

set -euo pipefail

# 测试配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
START_SCRIPT="$PROJECT_DIR/start.sh"
STOP_SCRIPT="$PROJECT_DIR/stop.sh"
TEST_LOG="$SCRIPT_DIR/compatibility_test.log"
TEST_REPORT="$SCRIPT_DIR/compatibility_test_report.md"
TEST_CONFIGS_DIR="$SCRIPT_DIR/test_configs"
BACKUP_DIR="$SCRIPT_DIR/backup"

# 测试超时配置
STARTUP_TIMEOUT=60
SHUTDOWN_TIMEOUT=30
TEST_TIMEOUT=120

# 测试统计
TEST_COUNT=0
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
WARN_COUNT=0
START_TIME=$(date +%s)

# 环境信息
OS_TYPE=$(uname -s)
OS_VERSION=$(uname -r)
SHELL_TYPE="$0"
GO_VERSION=$(go version 2>/dev/null || echo "未安装")
DOCKER_VERSION=$(docker --version 2>/dev/null || echo "未安装")

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 日志函数
log_test() {
    echo -e "${BLUE}[COMPAT]${NC} $*" | tee -a "$TEST_LOG"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $*" | tee -a "$TEST_LOG"
    ((PASS_COUNT++))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $*" | tee -a "$TEST_LOG"
    ((FAIL_COUNT++))
}

log_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $*" | tee -a "$TEST_LOG"
    ((SKIP_COUNT++))
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*" | tee -a "$TEST_LOG"
    ((WARN_COUNT++))
}

log_info() {
    echo -e "${CYAN}[INFO]${NC} $*" | tee -a "$TEST_LOG"
}

log_debug() {
    if [ "${DEBUG:-false}" = "true" ]; then
        echo -e "${PURPLE}[DEBUG]${NC} $*" | tee -a "$TEST_LOG"
    fi
}

# 测试框架函数
run_compat_test() {
    local test_name="$1"
    local test_function="$2"
    local timeout="${3:-$TEST_TIMEOUT}"
    
    ((TEST_COUNT++))
    log_test "运行兼容性测试: $test_name"
    
    # 使用timeout运行测试
    if timeout "$timeout" bash -c "$test_function"; then
        log_pass "$test_name"
        return 0
    else
        local exit_code=$?
        if [ $exit_code -eq 124 ]; then
            log_fail "$test_name (超时)"
        else
            log_fail "$test_name (退出码: $exit_code)"
        fi
        return 1
    fi
}

# 清理函数
cleanup() {
    log_info "清理测试环境..."
    
    # 停止所有可能的测试进程
    "$STOP_SCRIPT" --force-timeout 5 >/dev/null 2>&1 || true
    
    # 恢复原始配置文件
    restore_original_configs
    
    # 清理测试文件
    rm -f ".data_loader_pid" ".test_*" 2>/dev/null || true
    
    # 清理临时文件
    rm -f /tmp/data4bt_compat_* 2>/dev/null || true
}

# 备份原始配置
backup_original_configs() {
    log_info "备份原始配置文件..."
    
    mkdir -p "$BACKUP_DIR"
    
    # 备份主要配置文件
    if [ -f "$PROJECT_DIR/config.yml" ]; then
        cp "$PROJECT_DIR/config.yml" "$BACKUP_DIR/config.yml.bak"
    fi
    
    if [ -f "$PROJECT_DIR/.env" ]; then
        cp "$PROJECT_DIR/.env" "$BACKUP_DIR/.env.bak"
    fi
    
    if [ -f "$PROJECT_DIR/docker-compose.yml" ]; then
        cp "$PROJECT_DIR/docker-compose.yml" "$BACKUP_DIR/docker-compose.yml.bak"
    fi
}

# 恢复原始配置
restore_original_configs() {
    log_debug "恢复原始配置文件..."
    
    if [ -d "$BACKUP_DIR" ]; then
        # 恢复配置文件
        if [ -f "$BACKUP_DIR/config.yml.bak" ]; then
            cp "$BACKUP_DIR/config.yml.bak" "$PROJECT_DIR/config.yml"
        fi
        
        if [ -f "$BACKUP_DIR/.env.bak" ]; then
            cp "$BACKUP_DIR/.env.bak" "$PROJECT_DIR/.env"
        fi
        
        if [ -f "$BACKUP_DIR/docker-compose.yml.bak" ]; then
            cp "$BACKUP_DIR/docker-compose.yml.bak" "$PROJECT_DIR/docker-compose.yml"
        fi
    fi
}

# 创建测试配置文件
create_test_configs() {
    log_info "创建测试配置文件..."
    
    mkdir -p "$TEST_CONFIGS_DIR"
    
    # 创建最小配置
    cat > "$TEST_CONFIGS_DIR/minimal_config.yml" << 'EOF'
# 最小配置文件
database:
  host: "localhost"
  port: 9000
  name: "test_db"
  user: "default"
  password: ""

logging:
  level: "info"
  file: "logs/data_loader.log"

server:
  port: 8890
  host: "localhost"
EOF

    # 创建详细配置
    cat > "$TEST_CONFIGS_DIR/detailed_config.yml" << 'EOF'
# 详细配置文件
database:
  host: "localhost"
  port: 9000
  name: "test_db"
  user: "default"
  password: ""
  max_connections: 10
  timeout: 30
  retry_attempts: 3

logging:
  level: "debug"
  file: "logs/data_loader.log"
  max_size: "100MB"
  max_backups: 5
  max_age: 30

server:
  port: 8890
  host: "localhost"
  read_timeout: 30
  write_timeout: 30
  idle_timeout: 60

data_processing:
  batch_size: 1000
  workers: 4
  buffer_size: 10000

monitoring:
  enabled: true
  metrics_port: 8891
  health_check_interval: 30
EOF

    # 创建错误配置（用于测试错误处理）
    cat > "$TEST_CONFIGS_DIR/invalid_config.yml" << 'EOF'
# 无效配置文件
database:
  host: "invalid_host"
  port: 99999
  name: ""
  user: ""
  password: ""

logging:
  level: "invalid_level"
  file: "/invalid/path/log.txt"

server:
  port: -1
  host: ""
EOF

    # 创建不同端口配置
    cat > "$TEST_CONFIGS_DIR/alt_port_config.yml" << 'EOF'
# 替代端口配置
database:
  host: "localhost"
  port: 9000
  name: "test_db"
  user: "default"
  password: ""

logging:
  level: "info"
  file: "logs/data_loader.log"

server:
  port: 8891
  host: "localhost"
EOF
}

# 等待应用启动
wait_for_startup() {
    local timeout="${1:-$STARTUP_TIMEOUT}"
    local port="${2:-8890}"
    local check_interval=2
    local elapsed=0
    
    while [ $elapsed -lt $timeout ]; do
        # 检查PID文件
        if [ -f ".data_loader_pid" ]; then
            local pid
            pid=$(cat .data_loader_pid 2>/dev/null || echo "")
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                # 检查端口是否监听
                if command -v lsof >/dev/null 2>&1; then
                    if lsof -ti ":$port" >/dev/null 2>&1; then
                        return 0
                    fi
                fi
                return 0
            fi
        fi
        
        sleep $check_interval
        elapsed=$((elapsed + check_interval))
    done
    
    return 1
}

# 等待应用停止
wait_for_shutdown() {
    local timeout="${1:-$SHUTDOWN_TIMEOUT}"
    local check_interval=1
    local elapsed=0
    
    while [ $elapsed -lt $timeout ]; do
        if [ ! -f ".data_loader_pid" ]; then
            return 0
        fi
        
        local pid
        pid=$(cat .data_loader_pid 2>/dev/null || echo "")
        if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
        
        sleep $check_interval
        elapsed=$((elapsed + check_interval))
    done
    
    return 1
}

# 测试1: 操作系统兼容性
test_os_compatibility() {
    log_info "测试操作系统兼容性..."
    
    # 检查操作系统类型
    case "$OS_TYPE" in
        "Darwin")
            log_info "检测到 macOS 系统: $OS_VERSION"
            ;;
        "Linux")
            log_info "检测到 Linux 系统: $OS_VERSION"
            ;;
        *)
            log_warn "未知操作系统: $OS_TYPE"
            ;;
    esac
    
    # 检查必要的命令
    local required_commands=("bash" "ps" "kill" "lsof" "grep" "awk" "sed")
    local missing_commands=()
    
    for cmd in "${required_commands[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing_commands+=("$cmd")
        fi
    done
    
    if [ ${#missing_commands[@]} -gt 0 ]; then
        log_fail "缺少必要命令: ${missing_commands[*]}"
        return 1
    fi
    
    # 检查Shell兼容性
    if [ -n "$BASH_VERSION" ]; then
        log_info "Bash版本: $BASH_VERSION"
        
        # 检查Bash版本是否足够新
        local bash_major
        bash_major=$(echo "$BASH_VERSION" | cut -d'.' -f1)
        if [ "$bash_major" -lt 4 ]; then
            log_warn "Bash版本较旧，可能存在兼容性问题"
        fi
    else
        log_warn "未检测到Bash版本信息"
    fi
    
    log_info "操作系统兼容性检查通过"
    return 0
}

# 测试2: Go环境兼容性
test_go_compatibility() {
    log_info "测试Go环境兼容性..."
    
    if ! command -v go >/dev/null 2>&1; then
        log_fail "未安装Go环境"
        return 1
    fi
    
    local go_version_output
    go_version_output=$(go version)
    log_info "Go版本: $go_version_output"
    
    # 检查Go版本
    local go_version
    go_version=$(echo "$go_version_output" | grep -o 'go[0-9]\+\.[0-9]\+' | sed 's/go//')
    
    if [ -n "$go_version" ]; then
        local major minor
        major=$(echo "$go_version" | cut -d'.' -f1)
        minor=$(echo "$go_version" | cut -d'.' -f2)
        
        # 检查是否满足最低版本要求（假设需要Go 1.18+）
        if [ "$major" -gt 1 ] || ([ "$major" -eq 1 ] && [ "$minor" -ge 18 ]); then
            log_info "Go版本满足要求"
        else
            log_warn "Go版本可能过旧，建议升级到1.18+"
        fi
    fi
    
    # 测试Go编译
    log_info "测试Go编译功能..."
    if go build -o /tmp/data4bt_test_build cmd/main.go 2>/dev/null; then
        log_info "Go编译测试通过"
        rm -f /tmp/data4bt_test_build
    else
        log_fail "Go编译测试失败"
        return 1
    fi
    
    return 0
}

# 测试3: Docker环境兼容性
test_docker_compatibility() {
    log_info "测试Docker环境兼容性..."
    
    if ! command -v docker >/dev/null 2>&1; then
        log_skip "Docker未安装，跳过Docker兼容性测试"
        return 0
    fi
    
    log_info "Docker版本: $DOCKER_VERSION"
    
    # 检查Docker是否运行
    if ! docker info >/dev/null 2>&1; then
        log_warn "Docker守护进程未运行"
        return 0
    fi
    
    # 检查ClickHouse容器
    if docker ps --format "table {{.Names}}" | grep -q "clickhouse"; then
        log_info "检测到ClickHouse容器正在运行"
    else
        log_info "ClickHouse容器未运行"
    fi
    
    # 测试Docker Compose（如果存在）
    if [ -f "$PROJECT_DIR/docker-compose.yml" ]; then
        if command -v docker-compose >/dev/null 2>&1; then
            log_info "检测到docker-compose配置文件"
            # 验证配置文件语法
            if docker-compose -f "$PROJECT_DIR/docker-compose.yml" config >/dev/null 2>&1; then
                log_info "docker-compose配置文件语法正确"
            else
                log_warn "docker-compose配置文件语法错误"
            fi
        else
            log_warn "存在docker-compose.yml但未安装docker-compose"
        fi
    fi
    
    return 0
}

# 测试4: 配置文件兼容性
test_config_compatibility() {
    log_info "测试配置文件兼容性..."
    
    # 测试最小配置
    log_info "测试最小配置文件..."
    cp "$TEST_CONFIGS_DIR/minimal_config.yml" "$PROJECT_DIR/config.yml"
    
    if test_config_startup; then
        log_info "最小配置测试通过"
    else
        log_fail "最小配置测试失败"
        return 1
    fi
    
    # 测试详细配置
    log_info "测试详细配置文件..."
    cp "$TEST_CONFIGS_DIR/detailed_config.yml" "$PROJECT_DIR/config.yml"
    
    if test_config_startup; then
        log_info "详细配置测试通过"
    else
        log_fail "详细配置测试失败"
        return 1
    fi
    
    # 测试替代端口配置
    log_info "测试替代端口配置..."
    cp "$TEST_CONFIGS_DIR/alt_port_config.yml" "$PROJECT_DIR/config.yml"
    
    if test_config_startup 8891; then
        log_info "替代端口配置测试通过"
    else
        log_fail "替代端口配置测试失败"
        return 1
    fi
    
    # 测试无效配置（应该失败）
    log_info "测试无效配置处理..."
    cp "$TEST_CONFIGS_DIR/invalid_config.yml" "$PROJECT_DIR/config.yml"
    
    # 启动应该失败
    "$START_SCRIPT" --background --timeout 15 &
    local start_pid=$!
    
    sleep 10
    
    if wait_for_startup 20; then
        log_warn "无效配置意外启动成功"
        "$STOP_SCRIPT" --timeout 10 >/dev/null 2>&1 || true
    else
        log_info "无效配置正确处理（启动失败）"
    fi
    
    kill $start_pid 2>/dev/null || true
    
    return 0
}

# 辅助函数：测试配置启动
test_config_startup() {
    local port="${1:-8890}"
    
    cleanup
    
    "$START_SCRIPT" --background --timeout 30 &
    local start_pid=$!
    
    if wait_for_startup 45 "$port"; then
        "$STOP_SCRIPT" --timeout 15 >/dev/null 2>&1 || true
        wait_for_shutdown 25
        return 0
    else
        kill $start_pid 2>/dev/null || true
        return 1
    fi
}

# 测试5: 环境变量兼容性
test_env_compatibility() {
    log_info "测试环境变量兼容性..."
    
    # 备份当前环境变量
    local original_log_level="${LOG_LEVEL:-}"
    local original_debug="${DEBUG:-}"
    local original_port="${PORT:-}"
    
    # 测试LOG_LEVEL环境变量
    export LOG_LEVEL="debug"
    log_info "测试LOG_LEVEL=debug"
    
    if test_config_startup; then
        log_info "LOG_LEVEL环境变量测试通过"
    else
        log_warn "LOG_LEVEL环境变量测试失败"
    fi
    
    # 测试DEBUG环境变量
    export DEBUG="true"
    log_info "测试DEBUG=true"
    
    if test_config_startup; then
        log_info "DEBUG环境变量测试通过"
    else
        log_warn "DEBUG环境变量测试失败"
    fi
    
    # 测试PORT环境变量
    export PORT="8892"
    log_info "测试PORT=8892"
    
    if test_config_startup 8892; then
        log_info "PORT环境变量测试通过"
    else
        log_warn "PORT环境变量测试失败"
    fi
    
    # 恢复环境变量
    if [ -n "$original_log_level" ]; then
        export LOG_LEVEL="$original_log_level"
    else
        unset LOG_LEVEL
    fi
    
    if [ -n "$original_debug" ]; then
        export DEBUG="$original_debug"
    else
        unset DEBUG
    fi
    
    if [ -n "$original_port" ]; then
        export PORT="$original_port"
    else
        unset PORT
    fi
    
    return 0
}

# 测试6: 脚本参数兼容性
test_script_args_compatibility() {
    log_info "测试脚本参数兼容性..."
    
    cleanup
    
    # 测试各种启动参数组合
    local test_cases=(
        "--background"
        "--background --timeout 20"
        "--background --verbose"
        "--background --timeout 20 --verbose"
        "--test-mode"
    )
    
    for args in "${test_cases[@]}"; do
        log_info "测试参数: $args"
        
        # 对于test-mode，使用不同的测试方法
        if [[ "$args" == *"--test-mode"* ]]; then
            # test-mode应该快速退出
            if timeout 30 "$START_SCRIPT" $args >/dev/null 2>&1; then
                log_info "参数 '$args' 测试通过"
            else
                log_warn "参数 '$args' 测试失败"
            fi
        else
            # 后台模式测试
            "$START_SCRIPT" $args &
            local start_pid=$!
            
            if wait_for_startup 45; then
                log_info "参数 '$args' 测试通过"
                "$STOP_SCRIPT" --timeout 15 >/dev/null 2>&1 || true
                wait_for_shutdown 25
            else
                log_warn "参数 '$args' 测试失败"
                kill $start_pid 2>/dev/null || true
            fi
        fi
        
        cleanup
        sleep 2
    done
    
    # 测试停止脚本参数
    log_info "测试停止脚本参数..."
    
    # 启动一个实例用于测试停止
    "$START_SCRIPT" --background --timeout 30 &
    local start_pid=$!
    
    if wait_for_startup 45; then
        # 测试不同的停止参数
        local stop_test_cases=(
            "--timeout 10"
            "--force-timeout 5"
            "--timeout 10 --force-timeout 5"
            "--verbose"
        )
        
        for stop_args in "${stop_test_cases[@]}"; do
            log_info "测试停止参数: $stop_args"
            
            if "$STOP_SCRIPT" $stop_args >/dev/null 2>&1; then
                log_info "停止参数 '$stop_args' 测试通过"
                break  # 成功停止后退出循环
            else
                log_warn "停止参数 '$stop_args' 测试失败"
            fi
        done
    else
        kill $start_pid 2>/dev/null || true
        log_warn "无法启动应用进行停止参数测试"
    fi
    
    return 0
}

# 测试7: 文件权限兼容性
test_file_permissions() {
    log_info "测试文件权限兼容性..."
    
    # 检查关键文件的权限
    local files_to_check=(
        "$START_SCRIPT"
        "$STOP_SCRIPT"
        "$PROJECT_DIR/scripts/process_manager.sh"
    )
    
    for file in "${files_to_check[@]}"; do
        if [ -f "$file" ]; then
            if [ -x "$file" ]; then
                log_info "文件 $(basename "$file") 权限正确"
            else
                log_fail "文件 $(basename "$file") 不可执行"
                return 1
            fi
        else
            log_warn "文件 $(basename "$file") 不存在"
        fi
    done
    
    # 检查日志目录权限
    local log_dir="$PROJECT_DIR/logs"
    if [ ! -d "$log_dir" ]; then
        mkdir -p "$log_dir" 2>/dev/null || {
            log_fail "无法创建日志目录: $log_dir"
            return 1
        }
    fi
    
    if [ -w "$log_dir" ]; then
        log_info "日志目录权限正确"
    else
        log_fail "日志目录不可写: $log_dir"
        return 1
    fi
    
    # 测试临时文件创建
    local temp_file="/tmp/data4bt_perm_test_$$"
    if touch "$temp_file" 2>/dev/null; then
        log_info "临时文件创建权限正确"
        rm -f "$temp_file"
    else
        log_fail "无法创建临时文件"
        return 1
    fi
    
    return 0
}

# 测试8: 网络端口兼容性
test_network_compatibility() {
    log_info "测试网络端口兼容性..."
    
    # 检查默认端口是否可用
    local ports_to_test=(8890 8891 8892)
    
    for port in "${ports_to_test[@]}"; do
        if command -v lsof >/dev/null 2>&1; then
            if lsof -ti ":$port" >/dev/null 2>&1; then
                log_warn "端口 $port 已被占用"
            else
                log_info "端口 $port 可用"
            fi
        else
            log_info "无法检查端口状态（lsof未安装）"
            break
        fi
    done
    
    # 测试端口绑定
    log_info "测试端口绑定功能..."
    
    # 使用替代端口配置测试
    cp "$TEST_CONFIGS_DIR/alt_port_config.yml" "$PROJECT_DIR/config.yml"
    
    "$START_SCRIPT" --background --timeout 30 &
    local start_pid=$!
    
    if wait_for_startup 45 8891; then
        log_info "端口绑定测试通过"
        
        # 检查端口是否真的在监听
        if command -v lsof >/dev/null 2>&1; then
            if lsof -ti :8891 >/dev/null 2>&1; then
                log_info "端口8891正在监听"
            else
                log_warn "端口8891未在监听"
            fi
        fi
        
        "$STOP_SCRIPT" --timeout 15 >/dev/null 2>&1 || true
        wait_for_shutdown 25
    else
        log_fail "端口绑定测试失败"
        kill $start_pid 2>/dev/null || true
        return 1
    fi
    
    return 0
}

# 生成兼容性测试报告
generate_compatibility_report() {
    local end_time=$(date +%s)
    local duration=$((end_time - START_TIME))
    local success_rate=0
    
    if [ $TEST_COUNT -gt 0 ]; then
        success_rate=$(( PASS_COUNT * 100 / TEST_COUNT ))
    fi
    
    cat > "$TEST_REPORT" << EOF
# Data4BT 兼容性测试报告

## 测试概要

- **测试时间**: $(date)
- **测试持续时间**: ${duration}秒
- **总测试数**: $TEST_COUNT
- **通过数**: $PASS_COUNT
- **失败数**: $FAIL_COUNT
- **跳过数**: $SKIP_COUNT
- **警告数**: $WARN_COUNT
- **成功率**: ${success_rate}%

## 测试环境

- **操作系统**: $OS_TYPE $OS_VERSION
- **Shell**: $SHELL_TYPE
- **Bash版本**: ${BASH_VERSION:-未知}
- **Go版本**: $GO_VERSION
- **Docker版本**: $DOCKER_VERSION
- **项目目录**: $PROJECT_DIR

## 兼容性检查结果

### 环境兼容性

- **操作系统**: $([ $PASS_COUNT -gt 0 ] && echo "✅ 兼容" || echo "❌ 不兼容")
- **Go环境**: $(grep -q "Go环境兼容性.*PASS" "$TEST_LOG" && echo "✅ 兼容" || echo "❌ 不兼容")
- **Docker环境**: $(grep -q "Docker环境兼容性.*PASS\|Docker环境兼容性.*SKIP" "$TEST_LOG" && echo "✅ 兼容" || echo "❌ 不兼容")

### 配置兼容性

- **配置文件**: $(grep -q "配置文件兼容性.*PASS" "$TEST_LOG" && echo "✅ 兼容" || echo "❌ 不兼容")
- **环境变量**: $(grep -q "环境变量兼容性.*PASS" "$TEST_LOG" && echo "✅ 兼容" || echo "❌ 不兼容")
- **脚本参数**: $(grep -q "脚本参数兼容性.*PASS" "$TEST_LOG" && echo "✅ 兼容" || echo "❌ 不兼容")

### 系统兼容性

- **文件权限**: $(grep -q "文件权限兼容性.*PASS" "$TEST_LOG" && echo "✅ 兼容" || echo "❌ 不兼容")
- **网络端口**: $(grep -q "网络端口兼容性.*PASS" "$TEST_LOG" && echo "✅ 兼容" || echo "❌ 不兼容")

## 测试结果详情

### 通过的测试

EOF
    
    # 添加通过的测试
    grep "\[PASS\]" "$TEST_LOG" | sed 's/.*\[PASS\] /- /' >> "$TEST_REPORT"
    
    echo "" >> "$TEST_REPORT"
    echo "### 失败的测试" >> "$TEST_REPORT"
    echo "" >> "$TEST_REPORT"
    
    # 添加失败的测试
    if [ $FAIL_COUNT -gt 0 ]; then
        grep "\[FAIL\]" "$TEST_LOG" | sed 's/.*\[FAIL\] /- /' >> "$TEST_REPORT"
    else
        echo "无失败测试" >> "$TEST_REPORT"
    fi
    
    echo "" >> "$TEST_REPORT"
    echo "### 跳过的测试" >> "$TEST_REPORT"
    echo "" >> "$TEST_REPORT"
    
    # 添加跳过的测试
    if [ $SKIP_COUNT -gt 0 ]; then
        grep "\[SKIP\]" "$TEST_LOG" | sed 's/.*\[SKIP\] /- /' >> "$TEST_REPORT"
    else
        echo "无跳过测试" >> "$TEST_REPORT"
    fi
    
    echo "" >> "$TEST_REPORT"
    echo "### 警告" >> "$TEST_REPORT"
    echo "" >> "$TEST_REPORT"
    
    # 添加警告
    if [ $WARN_COUNT -gt 0 ]; then
        grep "\[WARN\]" "$TEST_LOG" | sed 's/.*\[WARN\] /- /' >> "$TEST_REPORT"
    else
        echo "无警告" >> "$TEST_REPORT"
    fi
    
    cat >> "$TEST_REPORT" << EOF

## 兼容性评估

EOF
    
    # 兼容性评估
    if [ $FAIL_COUNT -eq 0 ]; then
        if [ $WARN_COUNT -eq 0 ]; then
            echo "✅ **完全兼容**: 所有兼容性测试通过，无警告。" >> "$TEST_REPORT"
        else
            echo "⚠️ **基本兼容**: 所有测试通过，但有 $WARN_COUNT 个警告需要关注。" >> "$TEST_REPORT"
        fi
    else
        echo "❌ **兼容性问题**: 有 $FAIL_COUNT 个兼容性测试失败，需要解决。" >> "$TEST_REPORT"
    fi
    
    cat >> "$TEST_REPORT" << EOF

## 建议

### 环境要求

1. **操作系统**: macOS 10.15+ 或 Linux (Ubuntu 18.04+, CentOS 7+)
2. **Go版本**: 1.18 或更高版本
3. **Bash版本**: 4.0 或更高版本
4. **Docker**: 可选，用于ClickHouse等服务

### 兼容性改进建议

EOF
    
    if [ $FAIL_COUNT -gt 0 ]; then
        echo "1. 解决失败的兼容性测试" >> "$TEST_REPORT"
        echo "2. 检查系统依赖和权限" >> "$TEST_REPORT"
        echo "3. 更新相关软件版本" >> "$TEST_REPORT"
    fi
    
    if [ $WARN_COUNT -gt 0 ]; then
        echo "1. 关注警告信息，考虑升级相关组件" >> "$TEST_REPORT"
        echo "2. 测试在目标部署环境中的兼容性" >> "$TEST_REPORT"
    fi
    
    echo "" >> "$TEST_REPORT"
    echo "详细的测试日志请查看: \`$TEST_LOG\`" >> "$TEST_REPORT"
}

# 显示测试摘要
show_compatibility_summary() {
    local end_time=$(date +%s)
    local duration=$((end_time - START_TIME))
    
    echo ""
    echo "==========================================="
    echo "           兼容性测试报告"
    echo "==========================================="
    echo "测试时间:     $(date)"
    echo "执行时长:     ${duration}秒"
    echo "总测试数:     $TEST_COUNT"
    echo -e "通过数:       ${GREEN}$PASS_COUNT${NC}"
    echo -e "失败数:       ${RED}$FAIL_COUNT${NC}"
    echo -e "跳过数:       ${YELLOW}$SKIP_COUNT${NC}"
    echo -e "警告数:       ${YELLOW}$WARN_COUNT${NC}"
    
    if [ $TEST_COUNT -gt 0 ]; then
        local success_rate=$(( PASS_COUNT * 100 / TEST_COUNT ))
        echo "成功率:       ${success_rate}%"
    fi
    
    echo ""
    echo "环境信息:"
    echo "  操作系统:   $OS_TYPE $OS_VERSION"
    echo "  Go版本:     $GO_VERSION"
    echo "  Docker:     $DOCKER_VERSION"
    echo ""
    echo "详细报告:     $TEST_REPORT"
    echo "详细日志:     $TEST_LOG"
    echo "==========================================="
    
    if [ $FAIL_COUNT -eq 0 ]; then
        if [ $WARN_COUNT -eq 0 ]; then
            echo -e "${GREEN}✅ 完全兼容！${NC}"
        else
            echo -e "${YELLOW}⚠️ 基本兼容，有警告${NC}"
        fi
        return 0
    else
        echo -e "${RED}❌ 存在兼容性问题${NC}"
        return 1
    fi
}

# 主函数
main() {
    echo "🔧 Data4BT 兼容性测试套件"
    echo "======================================"
    echo ""
    
    # 初始化
    echo "兼容性测试开始时间: $(date)" > "$TEST_LOG"
    
    # 切换到项目目录
    cd "$PROJECT_DIR"
    
    # 备份原始配置
    backup_original_configs
    
    # 创建测试配置
    create_test_configs
    
    # 清理环境
    cleanup
    
    # 运行兼容性测试
    run_compat_test "操作系统兼容性" test_os_compatibility 60
    run_compat_test "Go环境兼容性" test_go_compatibility 90
    run_compat_test "Docker环境兼容性" test_docker_compatibility 60
    run_compat_test "配置文件兼容性" test_config_compatibility 180
    run_compat_test "环境变量兼容性" test_env_compatibility 180
    run_compat_test "脚本参数兼容性" test_script_args_compatibility 240
    run_compat_test "文件权限兼容性" test_file_permissions 60
    run_compat_test "网络端口兼容性" test_network_compatibility 120
    
    # 最终清理
    cleanup
    
    # 生成兼容性报告
    generate_compatibility_report
    
    # 显示测试摘要
    show_compatibility_summary
}

# 信号处理
trap cleanup EXIT INT TERM

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --debug)
            DEBUG=true
            shift
            ;;
        --timeout)
            TEST_TIMEOUT="$2"
            shift 2
            ;;
        --startup-timeout)
            STARTUP_TIMEOUT="$2"
            shift 2
            ;;
        --help)
            echo "Data4BT 兼容性测试套件"
            echo ""
            echo "用法: $0 [选项]"
            echo ""
            echo "选项:"
            echo "  --debug              启用调试输出"
            echo "  --timeout SECS       设置测试超时时间 (默认: 120秒)"
            echo "  --startup-timeout SECS  设置启动超时时间 (默认: 60秒)"
            echo "  --help               显示此帮助信息"
            echo ""
            exit 0
            ;;
        *)
            echo "未知参数: $1"
            echo "使用 --help 查看帮助信息"
            exit 1
            ;;
    esac
done

# 执行主函数
main "$@"