#!/bin/bash

# 改进版数据加载器启动脚本
# 解决信号处理时序竞争问题，增强错误处理和日志输出
# 支持前台和后台运行模式，确保脚本幂等性

set -euo pipefail

# =============================================================================
# 脚本初始化
# =============================================================================

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 加载公共函数库
if [ -f "$SCRIPT_DIR/scripts/start_functions.sh" ]; then
    # shellcheck source=scripts/start_functions.sh
    source "$SCRIPT_DIR/scripts/start_functions.sh"
else
    echo "错误: 无法找到公共函数库 scripts/start_functions.sh" >&2
    exit 1
fi

# =============================================================================
# 参数解析
# =============================================================================

# 默认参数
RUN_MODE="foreground"  # foreground, background, test
TEST_SYMBOLS="BTCUSDT"
SHOW_HELP=false
VERBOSE=false

# 解析命令行参数
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --background|-bg)
                RUN_MODE="background"
                shift
                ;;
            --test|-t)
                RUN_MODE="test"
                if [[ $# -gt 1 && $2 != -* ]]; then
                    TEST_SYMBOLS="$2"
                    shift
                fi
                shift
                ;;
            --verbose|-v)
                VERBOSE=true
                DEBUG=true
                shift
                ;;
            --help|-h)
                SHOW_HELP=true
                shift
                ;;
            --symbols)
                if [[ $# -gt 1 ]]; then
                    TEST_SYMBOLS="$2"
                    shift 2
                else
                    log_error "--symbols 需要指定交易对"
                    exit 1
                fi
                ;;
            -*)
                log_error "未知参数: $1"
                show_help
                exit 1
                ;;
            *)
                log_error "未知参数: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

# 显示帮助信息
show_help() {
    cat << EOF
使用方法: $0 [选项]

选项:
    --background, -bg     后台运行模式
    --test, -t [SYMBOLS]  测试模式，可选择指定交易对 (默认: BTCUSDT)
    --symbols SYMBOLS     指定测试交易对
    --verbose, -v         详细输出模式
    --help, -h           显示此帮助信息

运行模式:
    前台模式 (默认):     ./start.sh
    后台模式:           ./start.sh --background
    测试模式:           ./start.sh --test
    测试指定交易对:      ./start.sh --test --symbols ETHUSDT

示例:
    ./start.sh                          # 前台运行
    ./start.sh --background             # 后台运行
    ./start.sh --test                   # 测试模式 (BTCUSDT)
    ./start.sh --test --symbols ETHUSDT # 测试指定交易对
    ./start.sh --verbose               # 详细输出模式

EOF
}

# =============================================================================
# 主要功能函数
# =============================================================================

# 构建命令行参数
build_command_args() {
    local args="-cmd=run -config=$CONFIG_FILE"
    
    # 测试模式的特殊参数
    if [ "$RUN_MODE" = "test" ]; then
        args="$args -symbols=$TEST_SYMBOLS"
    fi
    
    echo "$args"
}

# 执行启动前检查
perform_startup_checks() {
    log_info "执行启动前检查..."
    
    # 检查是否已有实例在运行
    local running_pids
    running_pids=$(find_running_processes)
    
    if [ -n "$running_pids" ]; then
        log_warn "发现运行中的实例: $running_pids"
        
        if [ "$RUN_MODE" != "test" ]; then
            log_info "停止现有实例以避免冲突..."
            stop_existing_processes
        else
            log_error "测试模式不允许与现有实例并行运行"
            return 1
        fi
    fi
    
    return 0
}

# 主启动函数
main_startup() {
    local cmd_args
    cmd_args=$(build_command_args)
    
    case "$RUN_MODE" in
        "foreground")
            start_foreground_mode $cmd_args
            ;;
        "background")
            start_background_mode $cmd_args
            ;;
        "test")
            start_test_mode "$TEST_SYMBOLS"
            ;;
        *)
            log_error "未知的运行模式: $RUN_MODE"
            return 1
            ;;
    esac
}

# =============================================================================
# 主程序入口
# =============================================================================

main() {
    # 解析参数
    parse_arguments "$@"
    
    # 显示帮助
    if [ "$SHOW_HELP" = "true" ]; then
        show_help
        exit 0
    fi
    
    # 设置详细输出
    if [ "$VERBOSE" = "true" ]; then
        DEBUG=true
        log_debug "启用详细输出模式"
    fi
    
    # 显示启动信息
    show_startup_info
    log_info "运行模式: $RUN_MODE"
    
    # 执行环境检查
    if ! perform_environment_check; then
        log_error "环境检查失败"
        exit 1
    fi
    
    # 准备启动环境
    if ! prepare_startup_environment; then
        log_error "启动环境准备失败"
        exit 1
    fi
    
    # 执行启动前检查
    if ! perform_startup_checks; then
        log_error "启动前检查失败"
        exit 1
    fi
    
    # 主启动流程
    if main_startup; then
        show_completion_info
    else
        log_error "启动失败"
        exit 1
    fi
}

# =============================================================================
# 脚本执行
# =============================================================================

# 只有在直接执行脚本时才运行main函数
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi