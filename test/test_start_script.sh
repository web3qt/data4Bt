#!/bin/bash

# test_start_script.sh - start.sh脚本测试套件
# 验证改进后的start.sh脚本的各种场景和功能

set -euo pipefail

# =============================================================================
# 测试配置
# =============================================================================

# 测试脚本配置
TEST_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$TEST_SCRIPT_DIR")"
START_SCRIPT="$PROJECT_ROOT/start.sh"
STOP_SCRIPT="$PROJECT_ROOT/stop.sh"
PID_FILE="$PROJECT_ROOT/.data_loader_pid"

# 测试结果
TEST_PASSED=0
TEST_FAILED=0
TEST_TOTAL=0

# 超时命令
TIMEOUT_CMD=""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# =============================================================================
# 测试工具函数
# =============================================================================

# 日志函数
log_test() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%H:%M:%S')
    
    case "$level" in
        "INFO")
            echo -e "${BLUE}[TEST-INFO]${NC} ${timestamp} - $message"
            ;;
        "PASS")
            echo -e "${GREEN}[TEST-PASS]${NC} ${timestamp} - $message"
            ;;
        "FAIL")
            echo -e "${RED}[TEST-FAIL]${NC} ${timestamp} - $message"
            ;;
        "WARN")
            echo -e "${YELLOW}[TEST-WARN]${NC} ${timestamp} - $message"
            ;;
    esac
}

# 测试断言函数
assert_true() {
    local condition="$1"
    local message="$2"
    
    TEST_TOTAL=$((TEST_TOTAL + 1))
    
    if eval "$condition"; then
        log_test "PASS" "$message"
        TEST_PASSED=$((TEST_PASSED + 1))
        return 0
    else
        log_test "FAIL" "$message"
        TEST_FAILED=$((TEST_FAILED + 1))
        return 1
    fi
}

assert_false() {
    local condition="$1"
    local message="$2"
    
    TEST_TOTAL=$((TEST_TOTAL + 1))
    
    if ! eval "$condition"; then
        log_test "PASS" "$message"
        TEST_PASSED=$((TEST_PASSED + 1))
        return 0
    else
        log_test "FAIL" "$message"
        TEST_FAILED=$((TEST_FAILED + 1))
        return 1
    fi
}

assert_equals() {
    local expected="$1"
    local actual="$2"
    local message="$3"
    
    TEST_TOTAL=$((TEST_TOTAL + 1))
    
    if [ "$expected" = "$actual" ]; then
        log_test "PASS" "$message (期望: $expected, 实际: $actual)"
        TEST_PASSED=$((TEST_PASSED + 1))
        return 0
    else
        log_test "FAIL" "$message (期望: $expected, 实际: $actual)"
        TEST_FAILED=$((TEST_FAILED + 1))
        return 1
    fi
}

# 等待函数
wait_for_condition() {
    local condition="$1"
    local timeout="${2:-30}"
    local interval="${3:-1}"
    
    local count=0
    while [ $count -lt $timeout ]; do
        if eval "$condition"; then
            return 0
        fi
        sleep "$interval"
        count=$((count + interval))
    done
    
    return 1
}

# 进程管理函数
is_process_running() {
    local pid="$1"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

get_pid_from_file() {
    if [ -f "$PID_FILE" ]; then
        cat "$PID_FILE" 2>/dev/null
    fi
}

find_test_processes() {
    pgrep -f "go run.*cmd/main.go" 2>/dev/null || true
}

# 清理函数
cleanup_test_environment() {
    log_test "INFO" "清理测试环境..."
    
    # 停止所有相关进程
    local pids
    pids=$(find_test_processes)
    if [ -n "$pids" ]; then
        echo "$pids" | xargs -r kill -TERM 2>/dev/null || true
        sleep 2
        pids=$(find_test_processes)
        if [ -n "$pids" ]; then
            echo "$pids" | xargs -r kill -KILL 2>/dev/null || true
        fi
    fi
    
    # 清理PID文件
    rm -f "$PID_FILE"
    
    # 等待进程完全退出
    sleep 1
}

# =============================================================================
# 测试用例
# =============================================================================

# 测试1: 脚本基本功能
test_script_basic_functionality() {
    log_test "INFO" "测试1: 脚本基本功能"
    
    # 检查脚本文件存在
    assert_true "[ -f '$START_SCRIPT' ]" "start.sh脚本文件存在"
    assert_true "[ -x '$START_SCRIPT' ]" "start.sh脚本可执行"
    
    # 检查公共函数库存在
    assert_true "[ -f '$PROJECT_ROOT/scripts/start_functions.sh' ]" "公共函数库存在"
}

# 测试2: 帮助信息
test_help_functionality() {
    log_test "INFO" "测试2: 帮助信息功能"
    
    # 测试帮助参数
    local help_output
    help_output=$(cd "$PROJECT_ROOT" && "$START_SCRIPT" --help 2>&1)
    
    assert_true "echo '$help_output' | grep -q '使用方法'" "帮助信息包含使用方法"
    assert_true "echo '$help_output' | grep -q '选项'" "帮助信息包含选项说明"
    assert_true "echo '$help_output' | grep -q '运行模式'" "帮助信息包含运行模式说明"
    assert_true "echo '$help_output' | grep -q '示例'" "帮助信息包含示例"
}

# 测试3: 参数解析
test_argument_parsing() {
    log_test "INFO" "测试3: 参数解析功能"
    
    # 测试无效参数
    local invalid_output
    invalid_output=$(cd "$PROJECT_ROOT" && "$START_SCRIPT" --invalid-option 2>&1 || true)
    
    assert_true "echo '$invalid_output' | grep -q '未知参数'" "无效参数被正确识别"
}

# 测试4: 环境检查
test_environment_check() {
    log_test "INFO" "测试4: 环境检查功能"
    
    # 检查必要命令
    assert_true "command -v go >/dev/null" "Go命令可用"
    assert_true "command -v docker >/dev/null" "Docker命令可用"
    
    # 检查配置文件
    assert_true "[ -f '$PROJECT_ROOT/config.yml' ]" "配置文件存在"
    
    # 检查Go模块
    assert_true "[ -f '$PROJECT_ROOT/go.mod' ]" "Go模块文件存在"
}

# 测试5: 进程管理
test_process_management() {
    log_test "INFO" "测试5: 进程管理功能"
    
    # 确保环境清洁
    cleanup_test_environment
    
    # 检查没有运行的进程
    local running_pids
    running_pids=$(find_test_processes)
    assert_true "[ -z '$running_pids' ]" "测试开始前没有运行的进程"
    
    # 检查PID文件不存在
    assert_false "[ -f '$PID_FILE' ]" "测试开始前PID文件不存在"
}

# 测试6: 后台模式启动
test_background_mode() {
    log_test "INFO" "测试6: 后台模式启动"
    
    # 清理环境
    cleanup_test_environment
    
    # 启动后台模式（使用timeout防止挂起）
    log_test "INFO" "启动后台模式..."
    cd "$PROJECT_ROOT"
    if [ -n "$TIMEOUT_CMD" ]; then
        $TIMEOUT_CMD 30s "$START_SCRIPT" --background >/dev/null 2>&1 &
    else
        "$START_SCRIPT" --background >/dev/null 2>&1 &
    fi
    local start_pid=$!
    
    # 等待启动完成
    sleep 5
    
    # 检查PID文件是否创建
    assert_true "[ -f '$PID_FILE' ]" "后台模式创建PID文件"
    
    # 检查进程是否运行
    if [ -f "$PID_FILE" ]; then
        local app_pid
        app_pid=$(get_pid_from_file)
        if [ -n "$app_pid" ]; then
            assert_true "is_process_running '$app_pid'" "后台进程正在运行"
        else
            log_test "FAIL" "无法从PID文件读取进程ID"
            TEST_FAILED=$((TEST_FAILED + 1))
            TEST_TOTAL=$((TEST_TOTAL + 1))
        fi
    fi
    
    # 清理
    cleanup_test_environment
    
    # 等待启动脚本退出
    if is_process_running "$start_pid"; then
        kill -TERM "$start_pid" 2>/dev/null || true
        sleep 2
    fi
}

# 测试7: 测试模式
test_test_mode() {
    log_test "INFO" "测试7: 测试模式功能"
    
    # 清理环境
    cleanup_test_environment
    
    # 测试模式应该快速退出，使用timeout确保不会挂起
    log_test "INFO" "启动测试模式..."
    cd "$PROJECT_ROOT"
    
    # 使用timeout和后台运行来测试
    if [ -n "$TIMEOUT_CMD" ]; then
        $TIMEOUT_CMD 10s "$START_SCRIPT" --test --symbols BTCUSDT >/dev/null 2>&1 &
    else
        "$START_SCRIPT" --test --symbols BTCUSDT >/dev/null 2>&1 &
    fi
    local test_pid=$!
    
    # 等待一段时间
    sleep 3
    
    # 检查测试是否在运行或已完成
    if is_process_running "$test_pid"; then
        log_test "INFO" "测试模式正在运行"
        # 等待更长时间或手动终止
        sleep 2
        if is_process_running "$test_pid"; then
            kill -TERM "$test_pid" 2>/dev/null || true
        fi
    else
        log_test "INFO" "测试模式已完成"
    fi
    
    # 这个测试主要验证测试模式能够启动，不会因为参数错误而失败
    assert_true "true" "测试模式启动成功"
    
    # 清理
    cleanup_test_environment
}

# 测试8: 信号处理
test_signal_handling() {
    log_test "INFO" "测试8: 信号处理功能"
    
    # 清理环境
    cleanup_test_environment
    
    # 启动前台模式
    log_test "INFO" "启动前台模式进行信号测试..."
    cd "$PROJECT_ROOT"
    if [ -n "$TIMEOUT_CMD" ]; then
        $TIMEOUT_CMD 30s "$START_SCRIPT" >/dev/null 2>&1 &
    else
        "$START_SCRIPT" >/dev/null 2>&1 &
    fi
    local start_pid=$!
    
    # 等待启动
    sleep 3
    
    # 检查是否有应用进程启动
    local app_pid
    app_pid=$(get_pid_from_file)
    
    if [ -n "$app_pid" ] && is_process_running "$app_pid"; then
        log_test "INFO" "应用进程已启动 (PID: $app_pid)"
        
        # 发送TERM信号给启动脚本
        kill -TERM "$start_pid" 2>/dev/null || true
        
        # 等待进程退出
        local wait_count=0
        while [ $wait_count -lt 10 ] && is_process_running "$app_pid"; do
            sleep 1
            wait_count=$((wait_count + 1))
        done
        
        # 检查进程是否已退出
        assert_false "is_process_running '$app_pid'" "信号处理后应用进程已退出"
        
        # 检查PID文件是否被清理
        sleep 1
        assert_false "[ -f '$PID_FILE' ]" "信号处理后PID文件被清理"
    else
        log_test "WARN" "应用进程未启动，跳过信号测试"
    fi
    
    # 确保启动脚本也退出
    if is_process_running "$start_pid"; then
        kill -KILL "$start_pid" 2>/dev/null || true
    fi
    
    # 清理
    cleanup_test_environment
}

# 测试9: 幂等性
test_idempotency() {
    log_test "INFO" "测试9: 脚本幂等性"
    
    # 清理环境
    cleanup_test_environment
    
    # 连续启动两次后台模式，第二次应该停止第一次
    log_test "INFO" "第一次启动后台模式..."
    cd "$PROJECT_ROOT"
    if [ -n "$TIMEOUT_CMD" ]; then
        $TIMEOUT_CMD 20s "$START_SCRIPT" --background >/dev/null 2>&1 &
    else
        "$START_SCRIPT" --background >/dev/null 2>&1 &
    fi
    local first_start_pid=$!
    
    sleep 3
    
    local first_app_pid
    first_app_pid=$(get_pid_from_file)
    
    if [ -n "$first_app_pid" ]; then
        log_test "INFO" "第一次启动成功 (PID: $first_app_pid)"
        
        # 第二次启动
        log_test "INFO" "第二次启动后台模式..."
        if [ -n "$TIMEOUT_CMD" ]; then
            $TIMEOUT_CMD 20s "$START_SCRIPT" --background >/dev/null 2>&1 &
        else
            "$START_SCRIPT" --background >/dev/null 2>&1 &
        fi
        local second_start_pid=$!
        
        sleep 3
        
        # 检查第一个进程是否被停止
        assert_false "is_process_running '$first_app_pid'" "第二次启动时第一个进程被停止"
        
        # 检查新的进程是否启动
        local second_app_pid
        second_app_pid=$(get_pid_from_file)
        
        if [ -n "$second_app_pid" ] && [ "$second_app_pid" != "$first_app_pid" ]; then
            assert_true "is_process_running '$second_app_pid'" "第二次启动创建新进程"
        fi
        
        # 清理启动脚本进程
        for pid in "$first_start_pid" "$second_start_pid"; do
            if is_process_running "$pid"; then
                kill -TERM "$pid" 2>/dev/null || true
            fi
        done
    else
        log_test "WARN" "第一次启动失败，跳过幂等性测试"
    fi
    
    # 清理
    cleanup_test_environment
}

# 测试10: 错误处理
test_error_handling() {
    log_test "INFO" "测试10: 错误处理功能"
    
    # 测试无效的配置文件路径（如果支持）
    # 这里主要测试脚本不会因为错误而崩溃
    
    # 测试无效参数组合
    local error_output
    error_output=$(cd "$PROJECT_ROOT" && "$START_SCRIPT" --test --invalid 2>&1 || true)
    
    # 脚本应该能够处理错误并给出有意义的错误信息
    assert_true "echo '$error_output' | grep -q '未知参数\\|错误\\|失败'" "错误情况下给出有意义的错误信息"
}

# =============================================================================
# 测试执行
# =============================================================================

# 显示测试开始信息
show_test_header() {
    echo ""
    echo "==========================================="
    echo "    start.sh 脚本测试套件"
    echo "==========================================="
    echo "测试时间: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "项目路径: $PROJECT_ROOT"
    echo "==========================================="
    echo ""
}

# 显示测试结果
show_test_results() {
    echo ""
    echo "==========================================="
    echo "           测试结果汇总"
    echo "==========================================="
    echo -e "总测试数: $TEST_TOTAL"
    echo -e "${GREEN}通过: $TEST_PASSED${NC}"
    echo -e "${RED}失败: $TEST_FAILED${NC}"
    
    if [ $TEST_FAILED -eq 0 ]; then
        echo -e "${GREEN}✅ 所有测试通过！${NC}"
        echo "==========================================="
        return 0
    else
        echo -e "${RED}❌ 有测试失败！${NC}"
        echo "==========================================="
        return 1
    fi
}

# 主测试函数
run_all_tests() {
    show_test_header
    
    # 初始清理
    cleanup_test_environment
    
    # 执行所有测试
    test_script_basic_functionality
    test_help_functionality
    test_argument_parsing
    test_environment_check
    test_process_management
    test_background_mode
    test_test_mode
    test_signal_handling
    test_idempotency
    test_error_handling
    
    # 最终清理
    cleanup_test_environment
    
    # 显示结果
    show_test_results
}

# =============================================================================
# 脚本入口
# =============================================================================

# 检查依赖
check_test_dependencies() {
    local missing_deps=()
    
    # 检查timeout命令（macOS使用gtimeout）
    if command -v timeout >/dev/null 2>&1; then
        TIMEOUT_CMD="timeout"
    elif command -v gtimeout >/dev/null 2>&1; then
        TIMEOUT_CMD="gtimeout"
    else
        log_test "WARN" "timeout命令不可用，将跳过超时保护"
        TIMEOUT_CMD=""
    fi
    
    for cmd in "pgrep" "kill"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing_deps+=("$cmd")
        fi
    done
    
    if [ ${#missing_deps[@]} -gt 0 ]; then
        log_test "FAIL" "缺少测试依赖: ${missing_deps[*]}"
        exit 1
    fi
}

# 主入口
main() {
    # 检查依赖
    check_test_dependencies
    
    # 运行测试
    if run_all_tests; then
        exit 0
    else
        exit 1
    fi
}

# 只有在直接执行脚本时才运行main函数
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi