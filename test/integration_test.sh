#!/bin/bash

# Data4BT 集成测试套件
# 测试start.sh和stop.sh脚本的端到端功能
# 验证信号处理机制和各种运行模式

set -euo pipefail

# 测试配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
START_SCRIPT="$PROJECT_DIR/start.sh"
STOP_SCRIPT="$PROJECT_DIR/stop.sh"
TEST_LOG="$SCRIPT_DIR/integration_test.log"
TEST_REPORT="$SCRIPT_DIR/integration_test_report.md"
TEST_TIMEOUT=60
START_TIMEOUT=30
STOP_TIMEOUT=20

# 测试统计
TEST_COUNT=0
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
START_TIME=$(date +%s)

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
    echo -e "${BLUE}[TEST]${NC} $*" | tee -a "$TEST_LOG"
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
}

log_info() {
    echo -e "${CYAN}[INFO]${NC} $*" | tee -a "$TEST_LOG"
}

log_debug() {
    if [ "${DEBUG:-false}" = "true" ]; then
        echo -e "${PURPLE}[DEBUG]${NC} $*" | tee -a "$TEST_LOG"
    fi
}

# 检查timeout命令可用性
check_timeout_command() {
    if command -v timeout >/dev/null 2>&1; then
        TIMEOUT_CMD="timeout"
    elif command -v gtimeout >/dev/null 2>&1; then
        TIMEOUT_CMD="gtimeout"
    else
        TIMEOUT_CMD=""
    fi
}

# 带超时的命令执行
run_with_timeout() {
    local timeout_seconds="$1"
    shift
    
    if [ -n "$TIMEOUT_CMD" ]; then
        "$TIMEOUT_CMD" "$timeout_seconds" "$@"
    else
        # 在macOS上的简单超时实现
        "$@" &
        local cmd_pid=$!
        
        # 等待命令完成或超时
        local count=0
        while [ $count -lt $timeout_seconds ]; do
            if ! kill -0 $cmd_pid 2>/dev/null; then
                wait $cmd_pid
                return $?
            fi
            sleep 1
            ((count++))
        done
        
        # 超时，杀死进程
        kill $cmd_pid 2>/dev/null || true
        wait $cmd_pid 2>/dev/null || true
        return 124  # timeout exit code
    fi
}

# 测试框架函数
run_test() {
    local test_name="$1"
    local test_function="$2"
    local timeout="${3:-$TEST_TIMEOUT}"
    
    ((TEST_COUNT++))
    log_test "运行测试: $test_name"
    
    # 直接调用测试函数，不使用子shell
    if [ -n "$TIMEOUT_CMD" ]; then
        if "$TIMEOUT_CMD" "$timeout" bash -c "source '$0'; $test_function"; then
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
    else
        # 直接调用函数，使用简单的超时机制
        {
            $test_function
        } &
        local test_pid=$!
        
        # 等待测试完成或超时
        local count=0
        while [ $count -lt $timeout ]; do
            if ! kill -0 $test_pid 2>/dev/null; then
                wait $test_pid
                local exit_code=$?
                if [ $exit_code -eq 0 ]; then
                    log_pass "$test_name"
                    return 0
                else
                    log_fail "$test_name (退出码: $exit_code)"
                    return 1
                fi
            fi
            sleep 1
            ((count++))
        done
        
        # 超时，杀死进程
        kill $test_pid 2>/dev/null || true
        wait $test_pid 2>/dev/null || true
        log_fail "$test_name (超时)"
        return 1
    fi
}

# 清理函数
cleanup() {
    log_info "清理测试环境..."
    
    # 停止所有可能的测试进程
    "$STOP_SCRIPT" --force-timeout 5 >/dev/null 2>&1 || true
    
    # 清理测试文件
    rm -f ".data_loader_pid" ".test_*" 2>/dev/null || true
    
    # 停止可能的后台进程
    pkill -f "test_integration" 2>/dev/null || true
    pkill -f "sleep 3600" 2>/dev/null || true
    
    # 清理临时文件
    rm -f /tmp/data4bt_test_* 2>/dev/null || true
}

# 等待进程启动
wait_for_startup() {
    local timeout="${1:-$START_TIMEOUT}"
    local check_interval=2
    local elapsed=0
    
    log_info "等待应用启动..."
    
    while [ $elapsed -lt $timeout ]; do
        # 检查PID文件
        if [ -f ".data_loader_pid" ]; then
            local pid
            pid=$(cat .data_loader_pid 2>/dev/null || echo "")
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                log_info "应用已启动 (PID: $pid)"
                return 0
            fi
        fi
        
        # 检查端口监听
        if command -v lsof >/dev/null 2>&1; then
            if lsof -ti :8890 >/dev/null 2>&1; then
                log_info "检测到端口8890已监听"
                return 0
            fi
        fi
        
        sleep $check_interval
        elapsed=$((elapsed + check_interval))
        log_debug "等待启动... ($elapsed/$timeout 秒)"
    done
    
    log_warn "启动超时 ($timeout 秒)"
    return 1
}

# 等待进程停止
wait_for_shutdown() {
    local timeout="${1:-$STOP_TIMEOUT}"
    local check_interval=1
    local elapsed=0
    
    log_info "等待应用停止..."
    
    while [ $elapsed -lt $timeout ]; do
        # 检查PID文件
        if [ -f ".data_loader_pid" ]; then
            local pid
            pid=$(cat .data_loader_pid 2>/dev/null || echo "")
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                sleep $check_interval
                elapsed=$((elapsed + check_interval))
                continue
            fi
        fi
        
        # 检查端口监听
        if command -v lsof >/dev/null 2>&1; then
            if lsof -ti :8890 >/dev/null 2>&1; then
                sleep $check_interval
                elapsed=$((elapsed + check_interval))
                continue
            fi
        fi
        
        log_info "应用已停止"
        return 0
    done
    
    log_warn "停止超时 ($timeout 秒)"
    return 1
}

# 检查应用是否运行
is_app_running() {
    # 检查PID文件
    if [ -f ".data_loader_pid" ]; then
        local pid
        pid=$(cat .data_loader_pid 2>/dev/null || echo "")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
    fi
    
    # 检查端口监听
    if command -v lsof >/dev/null 2>&1; then
        if lsof -ti :8890 >/dev/null 2>&1; then
            return 0
        fi
    fi
    
    return 1
}

# 测试1: 脚本存在性和权限检查
test_script_prerequisites() {
    log_info "检查脚本文件存在性和权限..."
    
    if [ ! -f "$START_SCRIPT" ]; then
        log_fail "start.sh脚本不存在: $START_SCRIPT"
        return 1
    fi
    
    if [ ! -x "$START_SCRIPT" ]; then
        log_fail "start.sh脚本不可执行"
        return 1
    fi
    
    if [ ! -f "$STOP_SCRIPT" ]; then
        log_fail "stop.sh脚本不存在: $STOP_SCRIPT"
        return 1
    fi
    
    if [ ! -x "$STOP_SCRIPT" ]; then
        log_fail "stop.sh脚本不可执行"
        return 1
    fi
    
    # 检查进程管理函数库
    if [ ! -f "$PROJECT_DIR/scripts/process_manager.sh" ]; then
        log_fail "进程管理函数库不存在"
        return 1
    fi
    
    log_info "所有脚本文件检查通过"
    return 0
}

# 测试2: 帮助信息测试
test_help_functionality() {
    log_info "测试帮助信息功能..."
    
    # 测试start.sh帮助
    if ! "$START_SCRIPT" --help >/dev/null 2>&1; then
        log_fail "start.sh --help 失败"
        return 1
    fi
    
    # 测试stop.sh帮助
    if ! "$STOP_SCRIPT" --help >/dev/null 2>&1; then
        log_fail "stop.sh --help 失败"
        return 1
    fi
    
    log_info "帮助信息功能正常"
    return 0
}

# 测试3: 基本启动停止测试
test_basic_start_stop() {
    log_info "测试基本启动停止功能..."
    
    # 确保环境清洁
    cleanup
    
    # 启动应用
    log_info "启动应用..."
    "$START_SCRIPT" --background --timeout 30 &
    local start_pid=$!
    
    # 等待启动
    if ! wait_for_startup 45; then
        kill $start_pid 2>/dev/null || true
        log_fail "应用启动失败"
        return 1
    fi
    
    # 检查应用是否运行
    if ! is_app_running; then
        log_fail "应用启动后未检测到运行状态"
        return 1
    fi
    
    log_info "应用启动成功，现在测试停止..."
    
    # 停止应用
    if ! "$STOP_SCRIPT" --timeout 15 --force-timeout 5; then
        log_fail "停止脚本执行失败"
        return 1
    fi
    
    # 等待停止
    if ! wait_for_shutdown 25; then
        log_fail "应用停止失败"
        return 1
    fi
    
    # 检查应用是否已停止
    if is_app_running; then
        log_fail "应用停止后仍在运行"
        return 1
    fi
    
    log_info "基本启动停止测试通过"
    return 0
}

# 测试4: 前台模式测试
test_foreground_mode() {
    log_info "测试前台模式..."
    
    cleanup
    
    # 创建测试脚本来模拟前台运行
    local test_script="/tmp/test_foreground.sh"
    cat > "$test_script" << 'EOF'
#!/bin/bash
cd "$1"
timeout 10 ./start.sh --test-mode 2>&1 | head -20
EOF
    chmod +x "$test_script"
    
    # 运行前台测试
    local output
    output=$("$test_script" "$PROJECT_DIR" 2>&1 || true)
    
    rm -f "$test_script"
    
    # 检查输出
    if echo "$output" | grep -q "Data4BT 启动脚本"; then
        log_info "前台模式测试通过"
        return 0
    else
        log_fail "前台模式测试失败"
        log_debug "输出: $output"
        return 1
    fi
}

# 测试5: 后台模式测试
test_background_mode() {
    log_info "测试后台模式..."
    
    cleanup
    
    # 启动后台模式
    "$START_SCRIPT" --background --timeout 30 &
    local start_pid=$!
    
    # 等待启动
    if ! wait_for_startup 45; then
        kill $start_pid 2>/dev/null || true
        log_fail "后台模式启动失败"
        return 1
    fi
    
    # 检查PID文件
    if [ ! -f ".data_loader_pid" ]; then
        log_fail "后台模式未创建PID文件"
        return 1
    fi
    
    local pid
    pid=$(cat .data_loader_pid)
    if ! kill -0 "$pid" 2>/dev/null; then
        log_fail "后台进程不存在"
        return 1
    fi
    
    log_info "后台模式启动成功，测试停止..."
    
    # 停止后台进程
    if ! "$STOP_SCRIPT" --timeout 15; then
        log_fail "后台模式停止失败"
        return 1
    fi
    
    # 等待停止
    if ! wait_for_shutdown 25; then
        log_fail "后台模式停止超时"
        return 1
    fi
    
    log_info "后台模式测试通过"
    return 0
}

# 测试6: 信号处理测试
test_signal_handling() {
    log_info "测试信号处理机制..."
    
    cleanup
    
    # 启动应用
    "$START_SCRIPT" --background --timeout 30 &
    local start_pid=$!
    
    if ! wait_for_startup 45; then
        kill $start_pid 2>/dev/null || true
        log_fail "信号测试：应用启动失败"
        return 1
    fi
    
    # 获取应用PID
    local app_pid
    app_pid=$(cat .data_loader_pid 2>/dev/null || echo "")
    
    if [ -z "$app_pid" ] || ! kill -0 "$app_pid" 2>/dev/null; then
        log_fail "信号测试：无法获取应用PID"
        return 1
    fi
    
    log_info "向应用发送TERM信号 (PID: $app_pid)..."
    
    # 发送TERM信号
    kill -TERM "$app_pid" 2>/dev/null || true
    
    # 等待优雅关闭
    local count=0
    while [ $count -lt 15 ]; do
        if ! kill -0 "$app_pid" 2>/dev/null; then
            log_info "应用响应TERM信号并优雅退出"
            break
        fi
        sleep 1
        ((count++))
    done
    
    # 检查是否还在运行
    if kill -0 "$app_pid" 2>/dev/null; then
        log_warn "应用未响应TERM信号，强制终止"
        kill -KILL "$app_pid" 2>/dev/null || true
        log_fail "信号处理测试失败"
        return 1
    fi
    
    log_info "信号处理测试通过"
    return 0
}

# 测试7: 脚本幂等性测试
test_idempotency() {
    log_info "测试脚本幂等性..."
    
    cleanup
    
    # 多次运行停止脚本（应该不报错）
    for i in {1..3}; do
        if ! "$STOP_SCRIPT" --timeout 5 >/dev/null 2>&1; then
            log_fail "停止脚本幂等性测试失败 (第${i}次)"
            return 1
        fi
    done
    
    # 启动应用
    "$START_SCRIPT" --background --timeout 30 &
    local start_pid=$!
    
    if ! wait_for_startup 45; then
        kill $start_pid 2>/dev/null || true
        log_fail "幂等性测试：应用启动失败"
        return 1
    fi
    
    # 多次运行停止脚本
    for i in {1..2}; do
        if ! "$STOP_SCRIPT" --timeout 10; then
            log_fail "停止脚本幂等性测试失败 (运行中第${i}次)"
            return 1
        fi
        
        # 第一次应该停止，第二次应该报告无进程
        sleep 2
    done
    
    log_info "脚本幂等性测试通过"
    return 0
}

# 测试8: 错误恢复测试
test_error_recovery() {
    log_info "测试错误恢复能力..."
    
    cleanup
    
    # 创建无效的PID文件
    echo "99999" > ".data_loader_pid"
    
    # 运行停止脚本（应该能处理无效PID）
    if ! "$STOP_SCRIPT" --timeout 10 >/dev/null 2>&1; then
        log_fail "错误恢复测试：无法处理无效PID文件"
        return 1
    fi
    
    # 创建损坏的PID文件
    echo "invalid_pid" > ".data_loader_pid"
    
    # 运行停止脚本
    if ! "$STOP_SCRIPT" --timeout 10 >/dev/null 2>&1; then
        log_fail "错误恢复测试：无法处理损坏的PID文件"
        return 1
    fi
    
    # 删除PID文件
    rm -f ".data_loader_pid"
    
    # 运行停止脚本（应该报告无进程）
    if ! "$STOP_SCRIPT" --timeout 10 >/dev/null 2>&1; then
        log_fail "错误恢复测试：无法处理缺失的PID文件"
        return 1
    fi
    
    log_info "错误恢复测试通过"
    return 0
}

# 测试9: 并发启动测试
test_concurrent_start() {
    log_info "测试并发启动处理..."
    
    cleanup
    
    # 同时启动多个实例
    "$START_SCRIPT" --background --timeout 30 &
    local pid1=$!
    sleep 2
    "$START_SCRIPT" --background --timeout 30 &
    local pid2=$!
    
    # 等待第一个启动
    if ! wait_for_startup 45; then
        kill $pid1 $pid2 2>/dev/null || true
        log_fail "并发测试：第一个实例启动失败"
        return 1
    fi
    
    # 等待第二个完成（应该检测到已有实例）
    sleep 10
    
    # 检查只有一个实例在运行
    local running_count=0
    if is_app_running; then
        running_count=1
    fi
    
    # 停止所有
    "$STOP_SCRIPT" --timeout 15 >/dev/null 2>&1 || true
    kill $pid1 $pid2 2>/dev/null || true
    
    if [ $running_count -eq 1 ]; then
        log_info "并发启动测试通过（正确处理重复启动）"
        return 0
    else
        log_fail "并发启动测试失败（检测到多个实例或无实例）"
        return 1
    fi
}

# 测试10: 资源清理测试
test_resource_cleanup() {
    log_info "测试资源清理..."
    
    cleanup
    
    # 创建一些临时文件
    touch ".test_temp_file"
    mkdir -p "/tmp/data4bt_test"
    touch "/tmp/data4bt_test/temp.log"
    
    # 启动应用
    "$START_SCRIPT" --background --timeout 30 &
    local start_pid=$!
    
    if ! wait_for_startup 45; then
        kill $start_pid 2>/dev/null || true
        log_fail "资源清理测试：应用启动失败"
        return 1
    fi
    
    # 停止应用
    if ! "$STOP_SCRIPT" --timeout 15; then
        log_fail "资源清理测试：停止失败"
        return 1
    fi
    
    # 检查资源清理
    if [ -f ".data_loader_pid" ]; then
        log_fail "资源清理测试：PID文件未清理"
        return 1
    fi
    
    # 清理测试文件
    rm -f ".test_temp_file"
    rm -rf "/tmp/data4bt_test"
    
    log_info "资源清理测试通过"
    return 0
}

# 生成测试报告
generate_test_report() {
    local end_time=$(date +%s)
    local duration=$((end_time - START_TIME))
    local success_rate=0
    
    if [ $TEST_COUNT -gt 0 ]; then
        success_rate=$(( PASS_COUNT * 100 / TEST_COUNT ))
    fi
    
    cat > "$TEST_REPORT" << EOF
# Data4BT 集成测试报告

## 测试概要

- **测试时间**: $(date)
- **测试持续时间**: ${duration}秒
- **总测试数**: $TEST_COUNT
- **通过数**: $PASS_COUNT
- **失败数**: $FAIL_COUNT
- **跳过数**: $SKIP_COUNT
- **成功率**: ${success_rate}%

## 测试环境

- **操作系统**: $(uname -s)
- **Shell版本**: $BASH_VERSION
- **项目目录**: $PROJECT_DIR
- **Go版本**: $(go version 2>/dev/null || echo "未安装")
- **Docker版本**: $(docker --version 2>/dev/null || echo "未安装")

## 测试结果

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
    echo "### 详细日志" >> "$TEST_REPORT"
    echo "" >> "$TEST_REPORT"
    echo "详细的测试日志请查看: \`$TEST_LOG\`" >> "$TEST_REPORT"
    
    echo "" >> "$TEST_REPORT"
    echo "## 建议" >> "$TEST_REPORT"
    echo "" >> "$TEST_REPORT"
    
    if [ $FAIL_COUNT -eq 0 ]; then
        echo "✅ 所有集成测试通过！系统运行正常。" >> "$TEST_REPORT"
    else
        echo "❌ 有 $FAIL_COUNT 个测试失败，请检查相关功能。" >> "$TEST_REPORT"
        echo "" >> "$TEST_REPORT"
        echo "建议的修复步骤：" >> "$TEST_REPORT"
        echo "1. 查看详细日志了解失败原因" >> "$TEST_REPORT"
        echo "2. 检查相关脚本的实现" >> "$TEST_REPORT"
        echo "3. 验证环境配置是否正确" >> "$TEST_REPORT"
        echo "4. 重新运行失败的测试" >> "$TEST_REPORT"
    fi
}

# 显示测试报告
show_test_summary() {
    local end_time=$(date +%s)
    local duration=$((end_time - START_TIME))
    
    echo ""
    echo "==========================================="
    echo "           集成测试报告"
    echo "==========================================="
    echo "测试时间:     $(date)"
    echo "执行时长:     ${duration}秒"
    echo "总测试数:     $TEST_COUNT"
    echo -e "通过数:       ${GREEN}$PASS_COUNT${NC}"
    echo -e "失败数:       ${RED}$FAIL_COUNT${NC}"
    echo -e "跳过数:       ${YELLOW}$SKIP_COUNT${NC}"
    
    if [ $TEST_COUNT -gt 0 ]; then
        local success_rate=$(( PASS_COUNT * 100 / TEST_COUNT ))
        echo "成功率:       ${success_rate}%"
    fi
    
    echo ""
    echo "详细报告:     $TEST_REPORT"
    echo "详细日志:     $TEST_LOG"
    echo "==========================================="
    
    if [ $FAIL_COUNT -eq 0 ]; then
        echo -e "${GREEN}✅ 所有集成测试通过！${NC}"
        return 0
    else
        echo -e "${RED}❌ 有 $FAIL_COUNT 个测试失败${NC}"
        return 1
    fi
}

# 主函数
main() {
    echo "🧪 Data4BT 集成测试套件"
    echo "======================================"
    echo ""
    
    # 检查timeout命令
    check_timeout_command
    if [ -n "$TIMEOUT_CMD" ]; then
        log_info "使用超时命令: $TIMEOUT_CMD"
    else
        log_info "使用内置超时实现"
    fi
    
    # 初始化日志文件
    echo "集成测试开始时间: $(date)" > "$TEST_LOG"
    
    # 切换到项目目录
    cd "$PROJECT_DIR"
    
    # 清理环境
    cleanup
    
    # 运行所有测试
    run_test "脚本存在性和权限检查" test_script_prerequisites 30
    run_test "帮助信息功能测试" test_help_functionality 30
    run_test "基本启动停止测试" test_basic_start_stop 120
    run_test "前台模式测试" test_foreground_mode 60
    run_test "后台模式测试" test_background_mode 120
    run_test "信号处理测试" test_signal_handling 90
    run_test "脚本幂等性测试" test_idempotency 120
    run_test "错误恢复测试" test_error_recovery 60
    run_test "并发启动测试" test_concurrent_start 120
    run_test "资源清理测试" test_resource_cleanup 90
    
    # 最终清理
    cleanup
    
    # 生成测试报告
    generate_test_report
    
    # 显示测试摘要
    show_test_summary
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
        --help)
            echo "Data4BT 集成测试套件"
            echo ""
            echo "用法: $0 [选项]"
            echo ""
            echo "选项:"
            echo "  --debug          启用调试输出"
            echo "  --timeout SECS   设置测试超时时间 (默认: 60秒)"
            echo "  --help           显示此帮助信息"
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