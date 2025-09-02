#!/bin/bash

# Data4BT stop.sh脚本测试套件
# 测试改进后的stop.sh脚本的各种功能

set -euo pipefail

# 测试配置
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$TEST_DIR")"
STOP_SCRIPT="$PROJECT_DIR/stop.sh"
PROCESS_MANAGER="$PROJECT_DIR/scripts/process_manager.sh"
TEST_LOG="$TEST_DIR/test_stop_script.log"
TEST_PID_FILE=".test_data_loader_pid"

# 测试统计
TEST_COUNT=0
PASS_COUNT=0
FAIL_COUNT=0

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_test() {
    echo -e "${BLUE}[TEST]${NC} $*" | tee -a "$TEST_LOG"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $*" | tee -a "$TEST_LOG"
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $*" | tee -a "$TEST_LOG"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*" | tee -a "$TEST_LOG"
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*" | tee -a "$TEST_LOG"
}

# 测试框架函数
start_test() {
    local test_name="$1"
    ((TEST_COUNT++))
    log_test "开始测试: $test_name"
}

end_test() {
    local test_name="$1"
    local result="$2"
    
    if [ "$result" = "pass" ]; then
        ((PASS_COUNT++))
        log_pass "测试通过: $test_name"
    else
        ((FAIL_COUNT++))
        log_fail "测试失败: $test_name"
    fi
    echo ""
}

# 清理函数
cleanup() {
    log_info "清理测试环境..."
    
    # 清理测试PID文件
    rm -f "$TEST_PID_FILE" ".data_loader_pid"
    
    # 停止可能的测试进程
    pkill -f "test_dummy_process" 2>/dev/null || true
    pkill -f "sleep 3600" 2>/dev/null || true
    
    # 清理临时文件
    rm -f /tmp/test_data4bt_* 2>/dev/null || true
}

# 创建测试进程
create_test_process() {
    local process_name="${1:-test_dummy_process}"
    local duration="${2:-60}"
    
    # 创建一个模拟的Data4BT进程
    nohup bash -c "exec -a '$process_name' sleep $duration" >/dev/null 2>&1 &
    local pid=$!
    
    # 等待进程启动
    sleep 1
    
    if kill -0 "$pid" 2>/dev/null; then
        echo "$pid"
        return 0
    else
        return 1
    fi
}

# 创建测试PID文件
create_test_pidfile() {
    local pid="$1"
    local pidfile="${2:-.data_loader_pid}"
    
    echo "$pid" > "$pidfile"
}

# 检查进程是否存在
check_process_exists() {
    local pid="$1"
    kill -0 "$pid" 2>/dev/null
}

# 测试1: 脚本基本功能测试
test_basic_functionality() {
    start_test "脚本基本功能测试"
    
    # 检查脚本是否存在且可执行
    if [ ! -f "$STOP_SCRIPT" ]; then
        end_test "脚本基本功能测试" "fail"
        log_fail "stop.sh脚本不存在: $STOP_SCRIPT"
        return 1
    fi
    
    if [ ! -x "$STOP_SCRIPT" ]; then
        end_test "脚本基本功能测试" "fail"
        log_fail "stop.sh脚本不可执行"
        return 1
    fi
    
    # 检查进程管理函数库是否存在
    if [ ! -f "$PROCESS_MANAGER" ]; then
        end_test "脚本基本功能测试" "fail"
        log_fail "进程管理函数库不存在: $PROCESS_MANAGER"
        return 1
    fi
    
    end_test "脚本基本功能测试" "pass"
}

# 测试2: 帮助信息测试
test_help_functionality() {
    start_test "帮助信息测试"
    
    # 测试 --help 参数
    if "$STOP_SCRIPT" --help >/dev/null 2>&1; then
        log_info "--help 参数正常工作"
    else
        end_test "帮助信息测试" "fail"
        return 1
    fi
    
    # 测试 -h 参数
    if "$STOP_SCRIPT" -h >/dev/null 2>&1; then
        log_info "-h 参数正常工作"
    else
        end_test "帮助信息测试" "fail"
        return 1
    fi
    
    end_test "帮助信息测试" "pass"
}

# 测试无效参数处理
test_invalid_arguments() {
    local output
    local exit_code=0
    
    output=$("$STOP_SCRIPT" --invalid-option 2>&1) || exit_code=$?
    
    if [ $exit_code -ne 0 ] && echo "$output" | grep -q "未知参数"; then
        log_info "无效参数处理正常"
        return 0
    else
        log_fail "无效参数处理异常"
        return 1
    fi
}

# 测试4: 进程查找功能测试
test_process_finding() {
    start_test "进程查找功能测试"
    
    # 创建测试进程
    local test_pid
    test_pid=$(create_test_process "data4bt-test" 30)
    
    if [ -z "$test_pid" ]; then
        end_test "进程查找功能测试" "fail"
        log_fail "无法创建测试进程"
        return 1
    fi
    
    log_info "创建测试进程: $test_pid"
    
    # 创建PID文件
    create_test_pidfile "$test_pid"
    
    # 测试进程查找
    source "$PROCESS_MANAGER"
    local found_pids
    found_pids=$(find_data4bt_processes)
    
    if echo "$found_pids" | grep -q "$test_pid"; then
        log_info "成功找到测试进程: $test_pid"
    else
        kill "$test_pid" 2>/dev/null || true
        rm -f ".data_loader_pid"
        end_test "进程查找功能测试" "fail"
        log_fail "未能找到测试进程"
        return 1
    fi
    
    # 清理
    kill "$test_pid" 2>/dev/null || true
    rm -f ".data_loader_pid"
    
    end_test "进程查找功能测试" "pass"
}

# 测试5: 进程停止功能测试
test_process_stopping() {
    start_test "进程停止功能测试"
    
    # 创建测试进程
    local test_pid
    test_pid=$(create_test_process "data4bt-test" 60)
    
    if [ -z "$test_pid" ]; then
        end_test "进程停止功能测试" "fail"
        log_fail "无法创建测试进程"
        return 1
    fi
    
    log_info "创建测试进程: $test_pid"
    
    # 创建PID文件
    create_test_pidfile "$test_pid"
    
    # 使用stop.sh停止进程
    if "$STOP_SCRIPT" -t 5 -f 3 >/dev/null 2>&1; then
        # 等待一下让进程有时间退出
        sleep 2
        
        # 检查进程是否已停止
        if check_process_exists "$test_pid"; then
            kill "$test_pid" 2>/dev/null || true
            end_test "进程停止功能测试" "fail"
            log_fail "进程未被正确停止"
            return 1
        else
            log_info "测试进程已成功停止"
        fi
    else
        kill "$test_pid" 2>/dev/null || true
        end_test "进程停止功能测试" "fail"
        log_fail "stop.sh执行失败"
        return 1
    fi
    
    # 清理
    rm -f ".data_loader_pid"
    
    end_test "进程停止功能测试" "pass"
}

# 测试6: Dry-run模式测试
test_dry_run_mode() {
    start_test "Dry-run模式测试"
    
    # 创建测试进程
    local test_pid
    test_pid=$(create_test_process "data4bt-test" 30)
    
    if [ -z "$test_pid" ]; then
        end_test "Dry-run模式测试" "fail"
        log_fail "无法创建测试进程"
        return 1
    fi
    
    log_info "创建测试进程: $test_pid"
    
    # 创建PID文件
    create_test_pidfile "$test_pid"
    
    # 使用dry-run模式
    if "$STOP_SCRIPT" --dry-run >/dev/null 2>&1; then
        # 检查进程是否仍然存在（dry-run不应该实际停止进程）
        if check_process_exists "$test_pid"; then
            log_info "Dry-run模式正确工作，进程未被停止"
        else
            kill "$test_pid" 2>/dev/null || true
            end_test "Dry-run模式测试" "fail"
            log_fail "Dry-run模式错误地停止了进程"
            return 1
        fi
    else
        kill "$test_pid" 2>/dev/null || true
        end_test "Dry-run模式测试" "fail"
        log_fail "Dry-run模式执行失败"
        return 1
    fi
    
    # 清理
    kill "$test_pid" 2>/dev/null || true
    rm -f ".data_loader_pid"
    
    end_test "Dry-run模式测试" "pass"
}

# 测试7: 详细输出模式测试
test_verbose_mode() {
    start_test "详细输出模式测试"
    
    # 测试详细输出模式
    local output
    output=$("$STOP_SCRIPT" --verbose --dry-run 2>&1)
    
    if echo "$output" | grep -q "配置信息"; then
        log_info "详细输出模式正常工作"
    else
        end_test "详细输出模式测试" "fail"
        log_fail "详细输出模式未正常工作"
        return 1
    fi
    
    end_test "详细输出模式测试" "pass"
}

# 测试8: 错误处理测试
test_error_handling() {
    start_test "错误处理测试"
    
    # 测试无效的超时参数
    if "$STOP_SCRIPT" --timeout invalid >/dev/null 2>&1; then
        end_test "错误处理测试" "fail"
        log_fail "应该拒绝无效的超时参数"
        return 1
    fi
    
    # 测试无效的强制超时参数
    if "$STOP_SCRIPT" --force-timeout invalid >/dev/null 2>&1; then
        end_test "错误处理测试" "fail"
        log_fail "应该拒绝无效的强制超时参数"
        return 1
    fi
    
    log_info "错误处理正常工作"
    end_test "错误处理测试" "pass"
}

# 测试9: 进程管理函数库集成测试
test_process_manager_integration() {
    start_test "进程管理函数库集成测试"
    
    # 检查是否能正确加载进程管理函数库
    if source "$PROCESS_MANAGER" 2>/dev/null; then
        log_info "进程管理函数库加载成功"
    else
        end_test "进程管理函数库集成测试" "fail"
        log_fail "无法加载进程管理函数库"
        return 1
    fi
    
    # 测试关键函数是否存在
    local required_functions=(
        "find_data4bt_processes"
        "stop_data4bt_processes"
        "show_process_info"
        "cleanup_temp_files"
    )
    
    for func in "${required_functions[@]}"; do
        if declare -f "$func" >/dev/null; then
            log_info "函数 $func 存在"
        else
            end_test "进程管理函数库集成测试" "fail"
            log_fail "缺少必需的函数: $func"
            return 1
        fi
    done
    
    end_test "进程管理函数库集成测试" "pass"
}

# 测试10: 完整流程测试
test_complete_workflow() {
    start_test "完整流程测试"
    
    # 创建多个测试进程
    local test_pids=()
    
    # 创建模拟go run进程
    local go_pid
    go_pid=$(create_test_process "go run cmd/main.go" 60)
    if [ -n "$go_pid" ]; then
        test_pids+=("$go_pid")
        log_info "创建Go进程: $go_pid"
    fi
    
    # 创建模拟data4bt进程
    local data4bt_pid
    data4bt_pid=$(create_test_process "data4bt" 60)
    if [ -n "$data4bt_pid" ]; then
        test_pids+=("$data4bt_pid")
        log_info "创建Data4BT进程: $data4bt_pid"
        
        # 为主进程创建PID文件
        create_test_pidfile "$data4bt_pid"
    fi
    
    if [ ${#test_pids[@]} -eq 0 ]; then
        end_test "完整流程测试" "fail"
        log_fail "无法创建测试进程"
        return 1
    fi
    
    # 执行完整的停止流程
    if "$STOP_SCRIPT" -v -t 10 -f 5; then
        log_info "停止脚本执行成功"
        
        # 等待进程停止
        sleep 3
        
        # 检查所有进程是否已停止
        local remaining_count=0
        for pid in "${test_pids[@]}"; do
            if check_process_exists "$pid"; then
                ((remaining_count++))
                log_warn "进程仍在运行: $pid"
                kill "$pid" 2>/dev/null || true
            fi
        done
        
        if [ $remaining_count -eq 0 ]; then
            log_info "所有测试进程已成功停止"
        else
            end_test "完整流程测试" "fail"
            log_fail "$remaining_count 个进程未被正确停止"
            return 1
        fi
    else
        # 清理剩余进程
        for pid in "${test_pids[@]}"; do
            kill "$pid" 2>/dev/null || true
        done
        
        end_test "完整流程测试" "fail"
        log_fail "停止脚本执行失败"
        return 1
    fi
    
    # 清理
    rm -f ".data_loader_pid"
    
    end_test "完整流程测试" "pass"
}

# 显示测试报告
show_test_report() {
    echo ""
    echo "==========================================="
    echo "           测试报告"
    echo "==========================================="
    echo "总测试数: $TEST_COUNT"
    echo -e "通过: ${GREEN}$PASS_COUNT${NC}"
    echo -e "失败: ${RED}$FAIL_COUNT${NC}"
    
    if [ $FAIL_COUNT -eq 0 ]; then
        echo -e "\n${GREEN}✅ 所有测试通过！${NC}"
        echo "stop.sh脚本功能正常"
    else
        echo -e "\n${RED}❌ 有测试失败！${NC}"
        echo "请检查失败的测试并修复问题"
    fi
    
    echo ""
    echo "详细日志: $TEST_LOG"
    echo "==========================================="
}

# 主函数
main() {
    echo "🧪 Data4BT stop.sh脚本测试套件"
    echo "======================================"
    echo ""
    
    # 初始化测试日志
    echo "测试开始时间: $(date)" > "$TEST_LOG"
    
    # 清理测试环境
    cleanup
    
    # 切换到项目目录
    cd "$PROJECT_DIR"
    
    # 运行所有测试
    test_basic_functionality
    test_help_functionality
    test_argument_parsing
    test_process_finding
    test_process_stopping
    test_dry_run_mode
    test_verbose_mode
    test_error_handling
    test_process_manager_integration
    test_complete_workflow
    
    # 最终清理
    cleanup
    
    # 显示测试报告
    show_test_report
    
    # 返回适当的退出码
    if [ $FAIL_COUNT -eq 0 ]; then
        exit 0
    else
        exit 1
    fi
}

# 信号处理
trap cleanup EXIT INT TERM

# 执行主函数
main "$@"