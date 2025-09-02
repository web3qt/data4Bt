#!/bin/bash

# Data4BT 进程管理函数库
# 提供统一的进程查找、停止、管理功能

set -euo pipefail

# 全局变量
PROCESS_MANAGER_VERSION="1.0.0"
DEFAULT_GRACEFUL_TIMEOUT=30
DEFAULT_FORCE_TIMEOUT=10
LOG_PREFIX="[ProcessManager]"

# 日志函数
log_info() {
    echo "${LOG_PREFIX} ℹ️  $*"
}

log_warn() {
    echo "${LOG_PREFIX} ⚠️  $*" >&2
}

log_error() {
    echo "${LOG_PREFIX} ❌ $*" >&2
}

log_success() {
    echo "${LOG_PREFIX} ✅ $*"
}

log_debug() {
    if [ "${DEBUG:-false}" = "true" ]; then
        echo "${LOG_PREFIX} 🐛 $*" >&2
    fi
}

# 检查进程是否存在
# 参数: PID
# 返回: 0=存在, 1=不存在
is_process_running() {
    local pid="$1"
    
    if [ -z "$pid" ]; then
        return 1
    fi
    
    if ! [[ "$pid" =~ ^[0-9]+$ ]]; then
        log_error "无效的PID: $pid"
        return 1
    fi
    
    kill -0 "$pid" 2>/dev/null
}

# 通过PID文件查找进程
# 参数: PID文件路径
# 输出: PID (如果存在)
find_process_by_pidfile() {
    local pidfile="$1"
    
    if [ ! -f "$pidfile" ]; then
        log_debug "PID文件不存在: $pidfile"
        return 1
    fi
    
    local pid
    pid=$(cat "$pidfile" 2>/dev/null || true)
    
    if [ -z "$pid" ]; then
        log_warn "PID文件为空: $pidfile"
        return 1
    fi
    
    if is_process_running "$pid"; then
        echo "$pid"
        return 0
    else
        log_debug "PID文件中的进程已不存在: $pid"
        return 1
    fi
}

# 通过进程名查找进程
# 参数: 进程名模式
# 输出: PID列表 (每行一个)
find_processes_by_name() {
    local pattern="$1"
    
    if [ -z "$pattern" ]; then
        log_error "进程名模式不能为空"
        return 1
    fi
    
    log_debug "查找进程模式: $pattern"
    pgrep -f "$pattern" 2>/dev/null || true
}

# 通过端口查找进程
# 参数: 端口号
# 输出: PID列表 (每行一个)
find_processes_by_port() {
    local port="$1"
    
    if [ -z "$port" ]; then
        log_error "端口号不能为空"
        return 1
    fi
    
    if ! [[ "$port" =~ ^[0-9]+$ ]]; then
        log_error "无效的端口号: $port"
        return 1
    fi
    
    log_debug "查找监听端口的进程: $port"
    
    # 使用lsof查找监听指定端口的进程
    if command -v lsof >/dev/null 2>&1; then
        lsof -ti :"$port" 2>/dev/null || true
    else
        # 备用方法：使用netstat
        if command -v netstat >/dev/null 2>&1; then
            netstat -tlnp 2>/dev/null | grep ":$port " | awk '{print $7}' | cut -d'/' -f1 | grep -E '^[0-9]+$' || true
        else
            log_warn "未找到lsof或netstat命令，无法通过端口查找进程"
            return 1
        fi
    fi
}

# 综合查找Data4BT相关进程
# 输出: 去重后的PID列表 (每行一个)
find_data4bt_processes() {
    local pids=()
    local temp_pid
    
    log_debug "开始查找Data4BT相关进程..."
    
    # 方法1: 从PID文件查找
    if temp_pid=$(find_process_by_pidfile ".data_loader_pid" 2>/dev/null); then
        log_debug "从PID文件找到进程: $temp_pid"
        pids+=("$temp_pid")
    fi
    
    # 方法2: 通过进程名查找
    local name_patterns=(
        "go run.*cmd/main.go"
        "data4bt"
        "binance-data-loader"
        "./data-loader"
        "./data_loader"
    )
    
    for pattern in "${name_patterns[@]}"; do
        while IFS= read -r temp_pid; do
            if [ -n "$temp_pid" ] && [[ "$temp_pid" =~ ^[0-9]+$ ]]; then
                log_debug "通过进程名找到进程: $temp_pid (模式: $pattern)"
                pids+=("$temp_pid")
            fi
        done < <(find_processes_by_name "$pattern")
    done
    
    # 方法3: 通过端口查找 (如果配置了监听端口)
    local ports=("8890" "8080" "3000")  # 常用端口
    for port in "${ports[@]}"; do
        while IFS= read -r temp_pid; do
            if [ -n "$temp_pid" ] && [[ "$temp_pid" =~ ^[0-9]+$ ]]; then
                log_debug "通过端口找到进程: $temp_pid (端口: $port)"
                pids+=("$temp_pid")
            fi
        done < <(find_processes_by_port "$port")
    done
    
    # 去重并排序，只输出有效的PID
    if [ ${#pids[@]} -gt 0 ]; then
        printf '%s\n' "${pids[@]}" | sort -u | grep -E '^[0-9]+$' || true
    fi
}

# 发送信号给进程
# 参数: PID, 信号名 (默认TERM)
# 返回: 0=成功, 1=失败
send_signal_to_process() {
    local pid="$1"
    local signal="${2:-TERM}"
    
    if ! is_process_running "$pid"; then
        log_debug "进程不存在，无需发送信号: $pid"
        return 1
    fi
    
    log_debug "发送信号 $signal 给进程: $pid"
    
    if kill -"$signal" "$pid" 2>/dev/null; then
        log_debug "信号发送成功: $signal -> $pid"
        return 0
    else
        log_warn "信号发送失败: $signal -> $pid"
        return 1
    fi
}

# 等待进程退出
# 参数: PID, 超时时间(秒)
# 返回: 0=进程已退出, 1=超时
wait_for_process_exit() {
    local pid="$1"
    local timeout="${2:-$DEFAULT_GRACEFUL_TIMEOUT}"
    
    log_debug "等待进程退出: $pid (超时: ${timeout}秒)"
    
    local count=0
    while [ $count -lt "$timeout" ]; do
        if ! is_process_running "$pid"; then
            log_debug "进程已退出: $pid"
            return 0
        fi
        
        sleep 1
        ((count++))
        
        # 每5秒显示一次等待状态
        if [ $((count % 5)) -eq 0 ]; then
            log_debug "等待进程退出: $pid (已等待 ${count}/${timeout} 秒)"
        fi
    done
    
    log_warn "等待进程退出超时: $pid"
    return 1
}

# 优雅停止进程
# 参数: PID, 优雅超时时间(秒), 强制超时时间(秒)
# 返回: 0=成功停止, 1=失败
graceful_stop_process() {
    local pid="$1"
    local graceful_timeout="${2:-$DEFAULT_GRACEFUL_TIMEOUT}"
    local force_timeout="${3:-$DEFAULT_FORCE_TIMEOUT}"
    
    if ! is_process_running "$pid"; then
        log_debug "进程已不存在: $pid"
        return 0
    fi
    
    log_info "优雅停止进程: $pid"
    
    # 第一步: 发送TERM信号
    if send_signal_to_process "$pid" "TERM"; then
        if wait_for_process_exit "$pid" "$graceful_timeout"; then
            log_success "进程优雅退出: $pid"
            return 0
        fi
    fi
    
    # 第二步: 发送KILL信号
    log_warn "优雅停止超时，强制终止进程: $pid"
    
    if send_signal_to_process "$pid" "KILL"; then
        if wait_for_process_exit "$pid" "$force_timeout"; then
            log_success "进程强制退出: $pid"
            return 0
        fi
    fi
    
    log_error "无法停止进程: $pid"
    return 1
}

# 批量停止进程
# 参数: PID列表 (空格分隔)
# 返回: 0=全部成功, 1=部分或全部失败
batch_stop_processes() {
    local pids_str="$1"
    local graceful_timeout="${2:-$DEFAULT_GRACEFUL_TIMEOUT}"
    local force_timeout="${3:-$DEFAULT_FORCE_TIMEOUT}"
    
    if [ -z "$pids_str" ]; then
        log_info "没有需要停止的进程"
        return 0
    fi
    
    # 转换为数组
    local pids
    read -ra pids <<< "$pids_str"
    
    log_info "批量停止进程: ${pids[*]}"
    
    local failed_pids=()
    local success_count=0
    
    # 第一阶段: 发送TERM信号给所有进程
    log_info "第一阶段: 发送TERM信号..."
    for pid in "${pids[@]}"; do
        if [ -n "$pid" ] && is_process_running "$pid"; then
            send_signal_to_process "$pid" "TERM" || true
        fi
    done
    
    # 等待优雅退出
    log_info "等待进程优雅退出 (${graceful_timeout}秒)..."
    sleep 2  # 给进程一点时间开始处理信号
    
    local remaining_pids=()
    for pid in "${pids[@]}"; do
        if [ -n "$pid" ]; then
            if wait_for_process_exit "$pid" "$graceful_timeout"; then
                log_success "进程优雅退出: $pid"
                ((success_count++))
            else
                remaining_pids+=("$pid")
            fi
        fi
    done
    
    # 第二阶段: 强制终止剩余进程
    if [ ${#remaining_pids[@]} -gt 0 ]; then
        log_warn "第二阶段: 强制终止剩余进程: ${remaining_pids[*]}"
        
        for pid in "${remaining_pids[@]}"; do
            if is_process_running "$pid"; then
                if send_signal_to_process "$pid" "KILL"; then
                    if wait_for_process_exit "$pid" "$force_timeout"; then
                        log_success "进程强制退出: $pid"
                        ((success_count++))
                    else
                        failed_pids+=("$pid")
                    fi
                else
                    failed_pids+=("$pid")
                fi
            else
                # 进程在等待期间自己退出了
                log_success "进程已退出: $pid"
                ((success_count++))
            fi
        done
    fi
    
    # 报告结果
    log_info "停止结果: 成功 $success_count/${#pids[@]}"
    
    if [ ${#failed_pids[@]} -gt 0 ]; then
        log_error "无法停止的进程: ${failed_pids[*]}"
        return 1
    fi
    
    return 0
}

# 清理PID文件
# 参数: PID文件路径
cleanup_pidfile() {
    local pidfile="$1"
    
    if [ -f "$pidfile" ]; then
        log_debug "清理PID文件: $pidfile"
        rm -f "$pidfile" || log_warn "无法删除PID文件: $pidfile"
    fi
}

# 清理临时文件和目录
cleanup_temp_files() {
    local temp_dirs=(
        "/tmp/data4bt"
        "./tmp"
        "./temp"
    )
    
    local temp_files=(
        ".data_loader_pid"
        "./data-loader.log"
        "./nohup.out"
    )
    
    log_info "清理临时文件和目录..."
    
    # 清理临时目录
    for dir in "${temp_dirs[@]}"; do
        if [ -d "$dir" ]; then
            log_debug "清理临时目录: $dir"
            rm -rf "$dir" 2>/dev/null || log_warn "无法删除临时目录: $dir"
        fi
    done
    
    # 清理临时文件
    for file in "${temp_files[@]}"; do
        if [ -f "$file" ]; then
            log_debug "清理临时文件: $file"
            rm -f "$file" 2>/dev/null || log_warn "无法删除临时文件: $file"
        fi
    done
}

# 显示进程信息
# 参数: PID列表 (空格分隔)
show_process_info() {
    local pids_str="$1"
    
    if [ -z "$pids_str" ]; then
        log_info "没有进程信息可显示"
        return 0
    fi
    
    local pids
    read -ra pids <<< "$pids_str"
    
    log_info "进程信息:"
    echo "PID     PPID    CMD"
    echo "------- ------- ----------------------------------------"
    
    for pid in "${pids[@]}"; do
        if [ -n "$pid" ] && is_process_running "$pid"; then
            ps -p "$pid" -o pid,ppid,cmd --no-headers 2>/dev/null || \
                echo "$pid     ?       <进程信息获取失败>"
        fi
    done
}

# 检查系统资源
check_system_resources() {
    log_info "系统资源检查:"
    
    # 检查内存使用
    if command -v free >/dev/null 2>&1; then
        echo "内存使用情况:"
        free -h | head -2
    elif [ -f "/proc/meminfo" ]; then
        echo "内存使用情况:"
        grep -E "MemTotal|MemAvailable|MemFree" /proc/meminfo
    fi
    
    echo ""
    
    # 检查磁盘使用
    echo "磁盘使用情况:"
    df -h . 2>/dev/null || echo "无法获取磁盘使用信息"
    
    echo ""
    
    # 检查进程数量
    local process_count
    process_count=$(ps aux | wc -l)
    echo "当前进程数量: $process_count"
}

# 主要的停止函数 - 停止所有Data4BT进程
# 参数: 优雅超时时间(秒), 强制超时时间(秒)
# 返回: 0=成功, 1=失败
stop_data4bt_processes() {
    local graceful_timeout="${1:-$DEFAULT_GRACEFUL_TIMEOUT}"
    local force_timeout="${2:-$DEFAULT_FORCE_TIMEOUT}"
    
    log_info "开始停止Data4BT进程..."
    
    # 查找所有相关进程
    local pids
    pids=$(find_data4bt_processes)
    
    if [ -z "$pids" ]; then
        log_info "未发现运行中的Data4BT进程"
        cleanup_temp_files
        return 0
    fi
    
    # 显示找到的进程
    log_info "发现以下Data4BT进程:"
    show_process_info "$pids"
    echo ""
    
    # 批量停止进程
    local pids_str
    pids_str=$(echo "$pids" | tr '\n' ' ')
    
    if batch_stop_processes "$pids_str" "$graceful_timeout" "$force_timeout"; then
        log_success "所有Data4BT进程已成功停止"
        cleanup_temp_files
        return 0
    else
        log_error "部分Data4BT进程停止失败"
        
        # 显示剩余进程
        local remaining_pids
        remaining_pids=$(find_data4bt_processes)
        if [ -n "$remaining_pids" ]; then
            log_warn "剩余进程:"
            show_process_info "$remaining_pids"
        fi
        
        return 1
    fi
}

# 版本信息
show_version() {
    echo "Data4BT Process Manager v$PROCESS_MANAGER_VERSION"
}

# 帮助信息
show_help() {
    cat << EOF
Data4BT Process Manager v$PROCESS_MANAGER_VERSION

这是一个进程管理函数库，提供以下功能:

主要函数:
  stop_data4bt_processes [graceful_timeout] [force_timeout]
                        停止所有Data4BT相关进程
  find_data4bt_processes
                        查找所有Data4BT相关进程
  show_process_info <pids>
                        显示进程信息
  cleanup_temp_files    清理临时文件
  check_system_resources
                        检查系统资源

工具函数:
  is_process_running <pid>
                        检查进程是否运行
  graceful_stop_process <pid> [graceful_timeout] [force_timeout]
                        优雅停止单个进程
  batch_stop_processes <pids> [graceful_timeout] [force_timeout]
                        批量停止进程

查找函数:
  find_process_by_pidfile <pidfile>
                        通过PID文件查找进程
  find_processes_by_name <pattern>
                        通过进程名查找进程
  find_processes_by_port <port>
                        通过端口查找进程

使用示例:
  source scripts/process_manager.sh
  stop_data4bt_processes 30 10

环境变量:
  DEBUG=true           启用调试输出

EOF
}

# 如果直接执行此脚本，显示帮助信息
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    show_help
fi