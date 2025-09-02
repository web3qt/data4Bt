#!/bin/bash

# Data4BT 性能测试套件
# 测试应用的启动时间、资源使用、并发处理等性能指标

set -euo pipefail

# 测试配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
START_SCRIPT="$PROJECT_DIR/start.sh"
STOP_SCRIPT="$PROJECT_DIR/stop.sh"
TEST_LOG="$SCRIPT_DIR/performance_test.log"
TEST_REPORT="$SCRIPT_DIR/performance_test_report.md"
PERF_DATA_DIR="$SCRIPT_DIR/perf_data"

# 性能测试配置
STARTUP_ITERATIONS=5
MEMORY_CHECK_INTERVAL=2
MEMORY_CHECK_DURATION=30
CPU_CHECK_INTERVAL=1
CPU_CHECK_DURATION=20
LOAD_TEST_DURATION=60
MAX_STARTUP_TIME=45
MAX_SHUTDOWN_TIME=20

# 测试统计
TEST_COUNT=0
PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0
START_TIME=$(date +%s)

# 性能数据存储
declare -a STARTUP_TIMES
declare -a SHUTDOWN_TIMES
declare -a MEMORY_USAGE
declare -a CPU_USAGE

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
    echo -e "${BLUE}[PERF]${NC} $*" | tee -a "$TEST_LOG"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $*" | tee -a "$TEST_LOG"
    ((PASS_COUNT++))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $*" | tee -a "$TEST_LOG"
    ((FAIL_COUNT++))
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

# 性能数据记录函数
record_metric() {
    local metric_name="$1"
    local value="$2"
    local unit="${3:-}"
    
    echo "$(date '+%Y-%m-%d %H:%M:%S'),$metric_name,$value,$unit" >> "$PERF_DATA_DIR/metrics.csv"
}

# 初始化性能数据目录
init_perf_data() {
    mkdir -p "$PERF_DATA_DIR"
    echo "timestamp,metric,value,unit" > "$PERF_DATA_DIR/metrics.csv"
    echo "timestamp,memory_mb,cpu_percent" > "$PERF_DATA_DIR/resource_usage.csv"
}

# 清理函数
cleanup() {
    log_info "清理测试环境..."
    
    # 停止所有可能的测试进程
    "$STOP_SCRIPT" --force-timeout 5 >/dev/null 2>&1 || true
    
    # 清理测试文件
    rm -f ".data_loader_pid" ".test_*" 2>/dev/null || true
    
    # 停止监控进程
    pkill -f "performance_monitor" 2>/dev/null || true
    pkill -f "resource_monitor" 2>/dev/null || true
    
    # 清理临时文件
    rm -f /tmp/data4bt_perf_* 2>/dev/null || true
}

# 等待应用启动
wait_for_startup() {
    local timeout="${1:-$MAX_STARTUP_TIME}"
    local check_interval=1
    local elapsed=0
    
    while [ $elapsed -lt $timeout ]; do
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
        
        sleep $check_interval
        elapsed=$((elapsed + check_interval))
    done
    
    return 1
}

# 等待应用停止
wait_for_shutdown() {
    local timeout="${1:-$MAX_SHUTDOWN_TIME}"
    local check_interval=1
    local elapsed=0
    
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
        
        return 0
    done
    
    return 1
}

# 获取进程内存使用量（MB）
get_memory_usage() {
    local pid="$1"
    
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
        echo "0"
        return
    fi
    
    # macOS使用ps命令获取RSS（KB），转换为MB
    local memory_kb
    memory_kb=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || echo "0")
    local memory_mb=$((memory_kb / 1024))
    echo "$memory_mb"
}

# 获取进程CPU使用率
get_cpu_usage() {
    local pid="$1"
    
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
        echo "0"
        return
    fi
    
    # macOS使用ps命令获取CPU使用率
    local cpu_percent
    cpu_percent=$(ps -o %cpu= -p "$pid" 2>/dev/null | tr -d ' ' || echo "0")
    echo "$cpu_percent"
}

# 资源监控函数
monitor_resources() {
    local pid="$1"
    local duration="$2"
    local interval="${3:-2}"
    local output_file="$4"
    
    local end_time=$(($(date +%s) + duration))
    
    while [ $(date +%s) -lt $end_time ]; do
        if ! kill -0 "$pid" 2>/dev/null; then
            log_debug "进程 $pid 已停止，结束监控"
            break
        fi
        
        local memory_mb
        local cpu_percent
        memory_mb=$(get_memory_usage "$pid")
        cpu_percent=$(get_cpu_usage "$pid")
        
        echo "$(date '+%Y-%m-%d %H:%M:%S'),$memory_mb,$cpu_percent" >> "$output_file"
        
        sleep "$interval"
    done
}

# 测试框架函数
run_perf_test() {
    local test_name="$1"
    local test_function="$2"
    
    ((TEST_COUNT++))
    log_test "运行性能测试: $test_name"
    
    if $test_function; then
        log_pass "$test_name"
        return 0
    else
        log_fail "$test_name"
        return 1
    fi
}

# 测试1: 启动时间性能测试
test_startup_performance() {
    log_info "测试应用启动时间性能..."
    
    local total_time=0
    local successful_starts=0
    
    for i in $(seq 1 $STARTUP_ITERATIONS); do
        log_info "启动测试 $i/$STARTUP_ITERATIONS"
        
        cleanup
        sleep 2
        
        # 记录启动开始时间
        local start_time=$(date +%s.%N)
        
        # 启动应用
        "$START_SCRIPT" --background --timeout 30 &
        local start_pid=$!
        
        # 等待启动完成
        if wait_for_startup $MAX_STARTUP_TIME; then
            local end_time=$(date +%s.%N)
            local startup_time=$(echo "$end_time - $start_time" | bc -l)
            
            STARTUP_TIMES+=($startup_time)
            total_time=$(echo "$total_time + $startup_time" | bc -l)
            ((successful_starts++))
            
            log_info "启动时间: ${startup_time}秒"
            record_metric "startup_time" "$startup_time" "seconds"
            
            # 停止应用
            "$STOP_SCRIPT" --timeout 15 >/dev/null 2>&1 || true
            wait_for_shutdown $MAX_SHUTDOWN_TIME
        else
            log_warn "启动测试 $i 失败"
            kill $start_pid 2>/dev/null || true
        fi
        
        sleep 3
    done
    
    if [ $successful_starts -eq 0 ]; then
        log_fail "所有启动测试都失败"
        return 1
    fi
    
    # 计算平均启动时间
    local avg_startup_time
    avg_startup_time=$(echo "scale=2; $total_time / $successful_starts" | bc -l)
    
    log_info "成功启动次数: $successful_starts/$STARTUP_ITERATIONS"
    log_info "平均启动时间: ${avg_startup_time}秒"
    
    record_metric "avg_startup_time" "$avg_startup_time" "seconds"
    record_metric "startup_success_rate" "$(echo "scale=2; $successful_starts * 100 / $STARTUP_ITERATIONS" | bc -l)" "percent"
    
    # 性能评估
    if (( $(echo "$avg_startup_time <= 30" | bc -l) )); then
        log_info "启动性能: 优秀 (≤30秒)"
        return 0
    elif (( $(echo "$avg_startup_time <= 45" | bc -l) )); then
        log_warn "启动性能: 一般 (30-45秒)"
        return 0
    else
        log_fail "启动性能: 较差 (>45秒)"
        return 1
    fi
}

# 测试2: 内存使用性能测试
test_memory_performance() {
    log_info "测试应用内存使用性能..."
    
    cleanup
    
    # 启动应用
    "$START_SCRIPT" --background --timeout 30 &
    local start_pid=$!
    
    if ! wait_for_startup $MAX_STARTUP_TIME; then
        kill $start_pid 2>/dev/null || true
        log_fail "应用启动失败，无法进行内存测试"
        return 1
    fi
    
    # 获取应用PID
    local app_pid
    app_pid=$(cat .data_loader_pid 2>/dev/null || echo "")
    
    if [ -z "$app_pid" ] || ! kill -0 "$app_pid" 2>/dev/null; then
        log_fail "无法获取应用PID"
        return 1
    fi
    
    log_info "监控内存使用 (PID: $app_pid, 持续: ${MEMORY_CHECK_DURATION}秒)..."
    
    # 启动资源监控
    local resource_file="$PERF_DATA_DIR/memory_usage.csv"
    echo "timestamp,memory_mb,cpu_percent" > "$resource_file"
    
    monitor_resources "$app_pid" "$MEMORY_CHECK_DURATION" "$MEMORY_CHECK_INTERVAL" "$resource_file" &
    local monitor_pid=$!
    
    # 等待监控完成
    wait $monitor_pid 2>/dev/null || true
    
    # 分析内存使用数据
    if [ -f "$resource_file" ] && [ $(wc -l < "$resource_file") -gt 1 ]; then
        local max_memory
        local avg_memory
        local min_memory
        
        # 跳过标题行，提取内存数据
        max_memory=$(tail -n +2 "$resource_file" | cut -d',' -f2 | sort -n | tail -1)
        min_memory=$(tail -n +2 "$resource_file" | cut -d',' -f2 | sort -n | head -1)
        avg_memory=$(tail -n +2 "$resource_file" | cut -d',' -f2 | awk '{sum+=$1} END {printf "%.1f", sum/NR}')
        
        log_info "内存使用统计:"
        log_info "  最小值: ${min_memory}MB"
        log_info "  最大值: ${max_memory}MB"
        log_info "  平均值: ${avg_memory}MB"
        
        record_metric "max_memory" "$max_memory" "MB"
        record_metric "avg_memory" "$avg_memory" "MB"
        record_metric "min_memory" "$min_memory" "MB"
        
        # 内存使用评估
        if [ "$max_memory" -le 512 ]; then
            log_info "内存使用: 优秀 (≤512MB)"
        elif [ "$max_memory" -le 1024 ]; then
            log_warn "内存使用: 一般 (512-1024MB)"
        else
            log_warn "内存使用: 较高 (>1024MB)"
        fi
    else
        log_fail "无法获取内存使用数据"
        "$STOP_SCRIPT" --timeout 15 >/dev/null 2>&1 || true
        return 1
    fi
    
    # 停止应用
    "$STOP_SCRIPT" --timeout 15 >/dev/null 2>&1 || true
    wait_for_shutdown $MAX_SHUTDOWN_TIME
    
    return 0
}

# 测试3: CPU使用性能测试
test_cpu_performance() {
    log_info "测试应用CPU使用性能..."
    
    cleanup
    
    # 启动应用
    "$START_SCRIPT" --background --timeout 30 &
    local start_pid=$!
    
    if ! wait_for_startup $MAX_STARTUP_TIME; then
        kill $start_pid 2>/dev/null || true
        log_fail "应用启动失败，无法进行CPU测试"
        return 1
    fi
    
    # 获取应用PID
    local app_pid
    app_pid=$(cat .data_loader_pid 2>/dev/null || echo "")
    
    if [ -z "$app_pid" ] || ! kill -0 "$app_pid" 2>/dev/null; then
        log_fail "无法获取应用PID"
        return 1
    fi
    
    log_info "监控CPU使用 (PID: $app_pid, 持续: ${CPU_CHECK_DURATION}秒)..."
    
    # 收集CPU使用数据
    local cpu_data_file="$PERF_DATA_DIR/cpu_usage.csv"
    echo "timestamp,cpu_percent" > "$cpu_data_file"
    
    local end_time=$(($(date +%s) + CPU_CHECK_DURATION))
    
    while [ $(date +%s) -lt $end_time ]; do
        if ! kill -0 "$app_pid" 2>/dev/null; then
            log_debug "进程已停止，结束CPU监控"
            break
        fi
        
        local cpu_percent
        cpu_percent=$(get_cpu_usage "$app_pid")
        echo "$(date '+%Y-%m-%d %H:%M:%S'),$cpu_percent" >> "$cpu_data_file"
        
        sleep "$CPU_CHECK_INTERVAL"
    done
    
    # 分析CPU使用数据
    if [ -f "$cpu_data_file" ] && [ $(wc -l < "$cpu_data_file") -gt 1 ]; then
        local max_cpu
        local avg_cpu
        local min_cpu
        
        # 跳过标题行，提取CPU数据
        max_cpu=$(tail -n +2 "$cpu_data_file" | cut -d',' -f2 | sort -n | tail -1)
        min_cpu=$(tail -n +2 "$cpu_data_file" | cut -d',' -f2 | sort -n | head -1)
        avg_cpu=$(tail -n +2 "$cpu_data_file" | cut -d',' -f2 | awk '{sum+=$1} END {printf "%.1f", sum/NR}')
        
        log_info "CPU使用统计:"
        log_info "  最小值: ${min_cpu}%"
        log_info "  最大值: ${max_cpu}%"
        log_info "  平均值: ${avg_cpu}%"
        
        record_metric "max_cpu" "$max_cpu" "percent"
        record_metric "avg_cpu" "$avg_cpu" "percent"
        record_metric "min_cpu" "$min_cpu" "percent"
        
        # CPU使用评估
        if (( $(echo "$avg_cpu <= 20" | bc -l) )); then
            log_info "CPU使用: 优秀 (≤20%)"
        elif (( $(echo "$avg_cpu <= 50" | bc -l) )); then
            log_warn "CPU使用: 一般 (20-50%)"
        else
            log_warn "CPU使用: 较高 (>50%)"
        fi
    else
        log_fail "无法获取CPU使用数据"
        "$STOP_SCRIPT" --timeout 15 >/dev/null 2>&1 || true
        return 1
    fi
    
    # 停止应用
    "$STOP_SCRIPT" --timeout 15 >/dev/null 2>&1 || true
    wait_for_shutdown $MAX_SHUTDOWN_TIME
    
    return 0
}

# 测试4: 停止时间性能测试
test_shutdown_performance() {
    log_info "测试应用停止时间性能..."
    
    local total_time=0
    local successful_stops=0
    
    for i in $(seq 1 3); do
        log_info "停止测试 $i/3"
        
        cleanup
        
        # 启动应用
        "$START_SCRIPT" --background --timeout 30 &
        local start_pid=$!
        
        if ! wait_for_startup $MAX_STARTUP_TIME; then
            kill $start_pid 2>/dev/null || true
            log_warn "停止测试 $i: 启动失败"
            continue
        fi
        
        sleep 5  # 让应用稳定运行一段时间
        
        # 记录停止开始时间
        local start_time=$(date +%s.%N)
        
        # 停止应用
        if "$STOP_SCRIPT" --timeout 15; then
            if wait_for_shutdown $MAX_SHUTDOWN_TIME; then
                local end_time=$(date +%s.%N)
                local shutdown_time=$(echo "$end_time - $start_time" | bc -l)
                
                SHUTDOWN_TIMES+=($shutdown_time)
                total_time=$(echo "$total_time + $shutdown_time" | bc -l)
                ((successful_stops++))
                
                log_info "停止时间: ${shutdown_time}秒"
                record_metric "shutdown_time" "$shutdown_time" "seconds"
            else
                log_warn "停止测试 $i: 停止超时"
            fi
        else
            log_warn "停止测试 $i: 停止脚本失败"
        fi
        
        sleep 2
    done
    
    if [ $successful_stops -eq 0 ]; then
        log_fail "所有停止测试都失败"
        return 1
    fi
    
    # 计算平均停止时间
    local avg_shutdown_time
    avg_shutdown_time=$(echo "scale=2; $total_time / $successful_stops" | bc -l)
    
    log_info "成功停止次数: $successful_stops/3"
    log_info "平均停止时间: ${avg_shutdown_time}秒"
    
    record_metric "avg_shutdown_time" "$avg_shutdown_time" "seconds"
    
    # 性能评估
    if (( $(echo "$avg_shutdown_time <= 10" | bc -l) )); then
        log_info "停止性能: 优秀 (≤10秒)"
        return 0
    elif (( $(echo "$avg_shutdown_time <= 20" | bc -l) )); then
        log_warn "停止性能: 一般 (10-20秒)"
        return 0
    else
        log_fail "停止性能: 较差 (>20秒)"
        return 1
    fi
}

# 测试5: 负载稳定性测试
test_load_stability() {
    log_info "测试应用负载稳定性..."
    
    cleanup
    
    # 启动应用
    "$START_SCRIPT" --background --timeout 30 &
    local start_pid=$!
    
    if ! wait_for_startup $MAX_STARTUP_TIME; then
        kill $start_pid 2>/dev/null || true
        log_fail "应用启动失败，无法进行负载测试"
        return 1
    fi
    
    # 获取应用PID
    local app_pid
    app_pid=$(cat .data_loader_pid 2>/dev/null || echo "")
    
    if [ -z "$app_pid" ] || ! kill -0 "$app_pid" 2>/dev/null; then
        log_fail "无法获取应用PID"
        return 1
    fi
    
    log_info "运行负载稳定性测试 (持续: ${LOAD_TEST_DURATION}秒)..."
    
    # 启动资源监控
    local stability_file="$PERF_DATA_DIR/stability_test.csv"
    echo "timestamp,memory_mb,cpu_percent" > "$stability_file"
    
    monitor_resources "$app_pid" "$LOAD_TEST_DURATION" 5 "$stability_file" &
    local monitor_pid=$!
    
    # 模拟一些负载（如果有API端点的话）
    local load_test_pid=""
    if command -v curl >/dev/null 2>&1; then
        # 尝试对健康检查端点发送请求
        {
            local end_time=$(($(date +%s) + LOAD_TEST_DURATION))
            while [ $(date +%s) -lt $end_time ]; do
                curl -s "http://localhost:8890/health" >/dev/null 2>&1 || true
                sleep 5
            done
        } &
        load_test_pid=$!
    fi
    
    # 等待测试完成
    wait $monitor_pid 2>/dev/null || true
    
    if [ -n "$load_test_pid" ]; then
        kill $load_test_pid 2>/dev/null || true
    fi
    
    # 检查应用是否仍在运行
    if ! kill -0 "$app_pid" 2>/dev/null; then
        log_fail "应用在负载测试期间崩溃"
        return 1
    fi
    
    # 分析稳定性数据
    if [ -f "$stability_file" ] && [ $(wc -l < "$stability_file") -gt 1 ]; then
        local data_points
        data_points=$(tail -n +2 "$stability_file" | wc -l)
        
        log_info "负载测试完成，收集了 $data_points 个数据点"
        
        # 检查内存泄漏（简单的线性回归检查）
        local memory_trend
        memory_trend=$(tail -n +2 "$stability_file" | cut -d',' -f2 | awk '
            BEGIN {n=0; sum_x=0; sum_y=0; sum_xy=0; sum_x2=0}
            {n++; x=n; y=$1; sum_x+=x; sum_y+=y; sum_xy+=x*y; sum_x2+=x*x}
            END {
                if (n > 1) {
                    slope = (n*sum_xy - sum_x*sum_y) / (n*sum_x2 - sum_x*sum_x)
                    printf "%.2f", slope
                } else {
                    print "0"
                }
            }')
        
        log_info "内存使用趋势: ${memory_trend}MB/数据点"
        record_metric "memory_trend" "$memory_trend" "MB_per_point"
        
        # 评估稳定性
        if (( $(echo "$memory_trend <= 1" | bc -l) )); then
            log_info "内存稳定性: 优秀 (无明显泄漏)"
        elif (( $(echo "$memory_trend <= 5" | bc -l) )); then
            log_warn "内存稳定性: 一般 (轻微增长)"
        else
            log_warn "内存稳定性: 需关注 (明显增长)"
        fi
    else
        log_fail "无法获取稳定性测试数据"
        "$STOP_SCRIPT" --timeout 15 >/dev/null 2>&1 || true
        return 1
    fi
    
    # 停止应用
    "$STOP_SCRIPT" --timeout 15 >/dev/null 2>&1 || true
    wait_for_shutdown $MAX_SHUTDOWN_TIME
    
    log_info "负载稳定性测试完成"
    return 0
}

# 生成性能测试报告
generate_performance_report() {
    local end_time=$(date +%s)
    local duration=$((end_time - START_TIME))
    local success_rate=0
    
    if [ $TEST_COUNT -gt 0 ]; then
        success_rate=$(( PASS_COUNT * 100 / TEST_COUNT ))
    fi
    
    cat > "$TEST_REPORT" << EOF
# Data4BT 性能测试报告

## 测试概要

- **测试时间**: $(date)
- **测试持续时间**: ${duration}秒
- **总测试数**: $TEST_COUNT
- **通过数**: $PASS_COUNT
- **失败数**: $FAIL_COUNT
- **警告数**: $WARN_COUNT
- **成功率**: ${success_rate}%

## 测试环境

- **操作系统**: $(uname -s) $(uname -r)
- **CPU**: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "未知")
- **内存**: $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 / 1024 ))GB
- **Shell版本**: $BASH_VERSION
- **Go版本**: $(go version 2>/dev/null || echo "未安装")

## 性能指标

EOF
    
    # 添加启动时间统计
    if [ ${#STARTUP_TIMES[@]} -gt 0 ]; then
        local min_startup=$(printf '%s\n' "${STARTUP_TIMES[@]}" | sort -n | head -1)
        local max_startup=$(printf '%s\n' "${STARTUP_TIMES[@]}" | sort -n | tail -1)
        local avg_startup=$(printf '%s\n' "${STARTUP_TIMES[@]}" | awk '{sum+=$1} END {printf "%.2f", sum/NR}')
        
        cat >> "$TEST_REPORT" << EOF
### 启动性能

- **最快启动**: ${min_startup}秒
- **最慢启动**: ${max_startup}秒
- **平均启动**: ${avg_startup}秒
- **测试次数**: ${#STARTUP_TIMES[@]}

EOF
    fi
    
    # 添加停止时间统计
    if [ ${#SHUTDOWN_TIMES[@]} -gt 0 ]; then
        local min_shutdown=$(printf '%s\n' "${SHUTDOWN_TIMES[@]}" | sort -n | head -1)
        local max_shutdown=$(printf '%s\n' "${SHUTDOWN_TIMES[@]}" | sort -n | tail -1)
        local avg_shutdown=$(printf '%s\n' "${SHUTDOWN_TIMES[@]}" | awk '{sum+=$1} END {printf "%.2f", sum/NR}')
        
        cat >> "$TEST_REPORT" << EOF
### 停止性能

- **最快停止**: ${min_shutdown}秒
- **最慢停止**: ${max_shutdown}秒
- **平均停止**: ${avg_shutdown}秒
- **测试次数**: ${#SHUTDOWN_TIMES[@]}

EOF
    fi
    
    # 添加资源使用统计
    if [ -f "$PERF_DATA_DIR/metrics.csv" ]; then
        cat >> "$TEST_REPORT" << EOF
### 资源使用

详细的性能数据请查看:
- 性能指标: \`$PERF_DATA_DIR/metrics.csv\`
- 内存使用: \`$PERF_DATA_DIR/memory_usage.csv\`
- CPU使用: \`$PERF_DATA_DIR/cpu_usage.csv\`
- 稳定性测试: \`$PERF_DATA_DIR/stability_test.csv\`

EOF
    fi
    
    cat >> "$TEST_REPORT" << EOF
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
    echo "### 警告" >> "$TEST_REPORT"
    echo "" >> "$TEST_REPORT"
    
    # 添加警告
    if [ $WARN_COUNT -gt 0 ]; then
        grep "\[WARN\]" "$TEST_LOG" | sed 's/.*\[WARN\] /- /' >> "$TEST_REPORT"
    else
        echo "无警告" >> "$TEST_REPORT"
    fi
    
    cat >> "$TEST_REPORT" << EOF

## 性能评估

EOF
    
    # 性能评估
    if [ $FAIL_COUNT -eq 0 ]; then
        if [ $WARN_COUNT -eq 0 ]; then
            echo "✅ **优秀**: 所有性能测试通过，无警告。" >> "$TEST_REPORT"
        else
            echo "⚠️ **良好**: 所有测试通过，但有 $WARN_COUNT 个性能警告需要关注。" >> "$TEST_REPORT"
        fi
    else
        echo "❌ **需要改进**: 有 $FAIL_COUNT 个性能测试失败，需要优化。" >> "$TEST_REPORT"
    fi
    
    cat >> "$TEST_REPORT" << EOF

## 建议

### 性能优化建议

1. **启动优化**: 如果启动时间超过30秒，考虑优化初始化流程
2. **内存管理**: 监控内存使用趋势，避免内存泄漏
3. **CPU优化**: 如果CPU使用率持续较高，考虑算法优化
4. **并发处理**: 测试并发场景下的性能表现

### 监控建议

1. 在生产环境中持续监控启动时间
2. 设置内存使用阈值告警
3. 监控CPU使用率和负载
4. 定期进行性能回归测试

详细的测试日志请查看: \`$TEST_LOG\`
EOF
}

# 显示测试摘要
show_performance_summary() {
    local end_time=$(date +%s)
    local duration=$((end_time - START_TIME))
    
    echo ""
    echo "==========================================="
    echo "           性能测试报告"
    echo "==========================================="
    echo "测试时间:     $(date)"
    echo "执行时长:     ${duration}秒"
    echo "总测试数:     $TEST_COUNT"
    echo -e "通过数:       ${GREEN}$PASS_COUNT${NC}"
    echo -e "失败数:       ${RED}$FAIL_COUNT${NC}"
    echo -e "警告数:       ${YELLOW}$WARN_COUNT${NC}"
    
    if [ $TEST_COUNT -gt 0 ]; then
        local success_rate=$(( PASS_COUNT * 100 / TEST_COUNT ))
        echo "成功率:       ${success_rate}%"
    fi
    
    echo ""
    echo "性能数据:     $PERF_DATA_DIR/"
    echo "详细报告:     $TEST_REPORT"
    echo "详细日志:     $TEST_LOG"
    echo "==========================================="
    
    if [ $FAIL_COUNT -eq 0 ]; then
        if [ $WARN_COUNT -eq 0 ]; then
            echo -e "${GREEN}✅ 性能测试全部通过！${NC}"
        else
            echo -e "${YELLOW}⚠️ 测试通过，但有性能警告${NC}"
        fi
        return 0
    else
        echo -e "${RED}❌ 有 $FAIL_COUNT 个性能测试失败${NC}"
        return 1
    fi
}

# 主函数
main() {
    echo "🚀 Data4BT 性能测试套件"
    echo "======================================"
    echo ""
    
    # 检查依赖
    if ! command -v bc >/dev/null 2>&1; then
        echo "错误: 需要安装 bc 命令进行数学计算"
        echo "macOS: brew install bc"
        exit 1
    fi
    
    # 初始化
    init_perf_data
    echo "性能测试开始时间: $(date)" > "$TEST_LOG"
    
    # 切换到项目目录
    cd "$PROJECT_DIR"
    
    # 清理环境
    cleanup
    
    # 运行性能测试
    run_perf_test "启动时间性能测试" test_startup_performance
    run_perf_test "内存使用性能测试" test_memory_performance
    run_perf_test "CPU使用性能测试" test_cpu_performance
    run_perf_test "停止时间性能测试" test_shutdown_performance
    run_perf_test "负载稳定性测试" test_load_stability
    
    # 最终清理
    cleanup
    
    # 生成性能报告
    generate_performance_report
    
    # 显示测试摘要
    show_performance_summary
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
        --iterations)
            STARTUP_ITERATIONS="$2"
            shift 2
            ;;
        --memory-duration)
            MEMORY_CHECK_DURATION="$2"
            shift 2
            ;;
        --cpu-duration)
            CPU_CHECK_DURATION="$2"
            shift 2
            ;;
        --load-duration)
            LOAD_TEST_DURATION="$2"
            shift 2
            ;;
        --help)
            echo "Data4BT 性能测试套件"
            echo ""
            echo "用法: $0 [选项]"
            echo ""
            echo "选项:"
            echo "  --debug                启用调试输出"
            echo "  --iterations N         启动测试迭代次数 (默认: 5)"
            echo "  --memory-duration N    内存监控持续时间 (默认: 30秒)"
            echo "  --cpu-duration N       CPU监控持续时间 (默认: 20秒)"
            echo "  --load-duration N      负载测试持续时间 (默认: 60秒)"
            echo "  --help                 显示此帮助信息"
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