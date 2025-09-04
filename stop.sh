#!/bin/bash

# Data4BT 一键停止脚本
# 停止ClickHouse服务和相关进程
# 版本: 2.0.0 - 集成进程管理函数库

set -euo pipefail

# 脚本配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROCESS_MANAGER="${SCRIPT_DIR}/scripts/process_manager.sh"
GRACEFUL_TIMEOUT=30
FORCE_TIMEOUT=10
SHOW_HELP=false
VERBOSE=false
DRY_RUN=false

# 加载进程管理函数库
if [ -f "$PROCESS_MANAGER" ]; then
    source "$PROCESS_MANAGER"
else
    echo "❌ 错误：未找到进程管理函数库: $PROCESS_MANAGER"
    echo "请确保scripts/process_manager.sh文件存在"
    exit 1
fi

# 显示脚本头部信息
show_header() {
    echo "🛑 Data4BT 一键停止脚本 v2.0.0"
    echo "==================================="
    echo ""
}

# 参数解析
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                SHOW_HELP=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                export DEBUG=true
                shift
                ;;
            -t|--timeout)
                if [[ -n "${2:-}" && "$2" =~ ^[0-9]+$ ]]; then
                    GRACEFUL_TIMEOUT="$2"
                    shift 2
                else
                    echo "❌ 错误：--timeout 需要一个数字参数"
                    exit 1
                fi
                ;;
            -f|--force-timeout)
                if [[ -n "${2:-}" && "$2" =~ ^[0-9]+$ ]]; then
                    FORCE_TIMEOUT="$2"
                    shift 2
                else
                    echo "❌ 错误：--force-timeout 需要一个数字参数"
                    exit 1
                fi
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            *)
                echo "❌ 错误：未知参数 $1"
                echo "使用 --help 查看帮助信息"
                exit 1
                ;;
        esac
    done
}

# 显示帮助信息
show_help() {
    cat << EOF
Data4BT 一键停止脚本 v2.0.0

用法: $0 [选项]

选项:
  -h, --help              显示此帮助信息
  -v, --verbose           启用详细输出
  -t, --timeout SECONDS  设置优雅关闭超时时间 (默认: 30秒)
  -f, --force-timeout SECONDS
                          设置强制终止超时时间 (默认: 10秒)
  --dry-run               仅显示将要执行的操作，不实际执行

功能:
  - 智能查找Data4BT相关进程 (PID文件、进程名、端口)
  - 优雅关闭进程 (SIGTERM)
  - 强制终止顽固进程 (SIGKILL)
  - 清理临时文件和PID文件
  - 检查ClickHouse容器状态
  - 显示剩余进程信息

示例:
  $0                      # 使用默认设置停止所有进程
  $0 -v                   # 启用详细输出
  $0 -t 60 -f 15          # 设置60秒优雅关闭，15秒强制终止
  $0 --dry-run            # 预览将要执行的操作

EOF
}

# 检查Docker环境
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_warn "未找到Docker命令，将跳过容器检查"
        return 1
    fi
    return 0
}

# 停止Data4BT进程的主要函数
stop_data4bt() {
    log_info "开始停止Data4BT相关进程..."
    
    if [ "$DRY_RUN" = "true" ]; then
        log_info "[DRY RUN] 预览模式 - 将要执行的操作:"
        log_info "[DRY RUN] 1. 查找Data4BT相关进程"
        log_info "[DRY RUN] 2. 发送TERM信号进行优雅关闭 (超时: ${GRACEFUL_TIMEOUT}秒)"
        log_info "[DRY RUN] 3. 如需要，发送KILL信号强制终止 (超时: ${FORCE_TIMEOUT}秒)"
        log_info "[DRY RUN] 4. 清理临时文件和PID文件"
        
        # 在dry-run模式下仍然查找进程以显示信息
        local pids
        pids=$(find_data4bt_processes)
        if [ -n "$pids" ]; then
            log_info "[DRY RUN] 发现以下进程将被停止:"
            show_process_info "$pids"
        else
            log_info "[DRY RUN] 未发现运行中的Data4BT进程"
        fi
        return 0
    fi
    
    # 实际执行停止操作
    if stop_data4bt_processes "$GRACEFUL_TIMEOUT" "$FORCE_TIMEOUT"; then
        log_success "Data4BT进程停止完成"
        return 0
    else
        log_error "Data4BT进程停止失败"
        return 1
    fi
}

# 检查ClickHouse容器状态（仅显示信息，不进行任何操作）
check_clickhouse_container() {
    if ! check_docker; then
        return 0
    fi
    
    log_info "检查ClickHouse容器状态（仅供参考）..."
    
    local container_ids
    container_ids=$(docker ps -q --filter "name=clickhouse" --filter "status=running" 2>/dev/null || true)
    
    if [ -n "$container_ids" ]; then
        log_info "发现运行中的ClickHouse容器:"
        
        # 显示容器详细信息
        while IFS= read -r container_id; do
            if [ -n "$container_id" ]; then
                local container_info
                container_info=$(docker ps --filter "id=$container_id" --format "table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || true)
                if [ -n "$container_info" ]; then
                    echo "$container_info"
                fi
            fi
        done <<< "$container_ids"
        
        log_info "⚠️  重要提醒：ClickHouse容器保持运行状态（可能被其他项目使用）"
        log_info "🔒 安全保护：本脚本不会停止任何Docker容器"
        log_info "💡 如需停止ClickHouse容器，请手动执行: docker stop <container_id>"
    else
        log_info "未发现运行中的ClickHouse容器"
    fi
    
    echo ""
}

# 检查剩余进程
check_remaining_processes() {
    log_info "检查剩余进程..."
    
    local remaining_pids
    remaining_pids=$(find_data4bt_processes)
    
    if [ -n "$remaining_pids" ]; then
        log_warn "发现剩余相关进程:"
        show_process_info "$remaining_pids"
        echo ""
        log_warn "如需手动清理，请运行:"
        local pids_str
        pids_str=$(echo "$remaining_pids" | tr '\n' ' ')
        echo "   kill $pids_str"
        echo "   或者使用: $0 --force-timeout 5"
        return 1
    else
        log_success "未发现剩余相关进程"
        return 0
    fi
}

# 显示完成信息
show_completion_info() {
    echo ""
    log_success "Data4BT 停止完成!"
    echo "==========================================="
    echo ""
    echo "📝 提示:"
    echo "   重新启动:     ./start.sh"
    echo "   生产环境:     ./start.sh --prod"
    echo "   查看状态:     docker ps"
    echo "   查看帮助:     $0 --help"
    echo ""
    echo "🔒 安全提醒: 本脚本只停止Data4BT相关进程，不影响其他服务"
    echo "💡 ClickHouse等共享服务保持运行状态，确保其他项目正常工作"
    echo ""
}

# 主函数
main() {
    # 解析命令行参数
    parse_arguments "$@"
    
    # 显示帮助信息
    if [ "$SHOW_HELP" = "true" ]; then
        show_help
        exit 0
    fi
    
    # 显示脚本头部
    show_header
    
    # 如果启用了详细模式，显示配置信息
    if [ "$VERBOSE" = "true" ]; then
        log_info "配置信息:"
        log_info "  优雅关闭超时: ${GRACEFUL_TIMEOUT}秒"
        log_info "  强制终止超时: ${FORCE_TIMEOUT}秒"
        log_info "  详细输出: $VERBOSE"
        log_info "  预览模式: $DRY_RUN"
        echo ""
    fi
    
    # 停止Data4BT进程
    local stop_result=0
    stop_data4bt || stop_result=$?
    
    # 检查ClickHouse容器
    check_clickhouse_container
    
    # 检查剩余进程
    local remaining_result=0
    check_remaining_processes || remaining_result=$?
    
    # 显示系统资源信息（仅在详细模式下）
    if [ "$VERBOSE" = "true" ]; then
        echo ""
        check_system_resources
    fi
    
    # 显示完成信息
    show_completion_info
    
    # 返回适当的退出码
    if [ $stop_result -ne 0 ] || [ $remaining_result -ne 0 ]; then
        exit 1
    fi
    
    exit 0
}

# 执行主函数
main "$@"