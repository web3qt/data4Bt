#!/bin/bash

# 信号处理测试脚本
# 用于验证go run和脚本的信号处理问题

echo "🧪 信号处理测试脚本"
echo "=================="
echo ""

# 测试1: 直接运行go run的信号处理
test_go_run_signal() {
    echo "📋 测试1: go run信号处理"
    echo "启动: go run cmd/main.go -cmd=status"
    echo "请按 Ctrl+C 尝试停止..."
    echo "预期: 应该能够正常停止"
    echo ""
    
    # 记录开始时间
    start_time=$(date +%s)
    
    # 运行程序
    go run cmd/main.go -cmd=status
    
    # 记录结束时间
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    echo ""
    echo "✅ 测试1完成，运行时间: ${duration}秒"
    echo ""
}

# 测试2: start.sh脚本的信号处理
test_start_script_signal() {
    echo "📋 测试2: start.sh脚本信号处理"
    echo "启动: ./start.sh --test"
    echo "请按 Ctrl+C 尝试停止..."
    echo "预期: 应该能够优雅停止"
    echo ""
    
    # 记录开始时间
    start_time=$(date +%s)
    
    # 运行脚本
    ./start.sh --test
    
    # 记录结束时间
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    echo ""
    echo "✅ 测试2完成，运行时间: ${duration}秒"
    echo ""
}

# 测试3: 进程层级分析
test_process_hierarchy() {
    echo "📋 测试3: 进程层级分析"
    echo "分析go run创建的进程层级..."
    echo ""
    
    # 启动go run并获取PID
    echo "启动 go run cmd/main.go -cmd=status &"
    go run cmd/main.go -cmd=status &
    GO_RUN_PID=$!
    
    echo "go run进程PID: $GO_RUN_PID"
    
    # 等待一下让进程启动
    sleep 2
    
    # 查看进程树
    echo ""
    echo "进程树分析:"
    echo "----------"
    if command -v pstree &> /dev/null; then
        pstree -p $GO_RUN_PID 2>/dev/null || echo "pstree命令不可用"
    else
        echo "查找相关进程:"
        ps -ef | grep -E "(go run|main)" | grep -v grep || echo "未找到相关进程"
    fi
    
    echo ""
    echo "子进程分析:"
    echo "----------"
    # 查找子进程
    children=$(pgrep -P $GO_RUN_PID 2>/dev/null || true)
    if [ -n "$children" ]; then
        echo "子进程PID: $children"
        for child in $children; do
            echo "子进程 $child 详情:"
            ps -p $child -o pid,ppid,command 2>/dev/null || echo "进程已退出"
        done
    else
        echo "未找到子进程"
    fi
    
    # 停止进程
    echo ""
    echo "发送TERM信号到go run进程..."
    kill -TERM $GO_RUN_PID 2>/dev/null || echo "进程已退出"
    
    # 等待进程退出
    sleep 3
    
    # 检查是否还有残留进程
    echo "检查残留进程:"
    remaining=$(pgrep -f "cmd/main.go" 2>/dev/null || true)
    if [ -n "$remaining" ]; then
        echo "⚠️  发现残留进程: $remaining"
        echo "强制清理..."
        echo $remaining | xargs kill -KILL 2>/dev/null || true
    else
        echo "✅ 无残留进程"
    fi
    
    echo ""
    echo "✅ 测试3完成"
    echo ""
}

# 测试4: stop.sh脚本功能
test_stop_script() {
    echo "📋 测试4: stop.sh脚本功能测试"
    echo "启动后台进程并测试stop.sh..."
    echo ""
    
    # 启动后台进程
    echo "启动后台进程: ./start.sh --background"
    ./start.sh --background
    
    # 等待进程启动
    sleep 5
    
    # 检查进程状态
    if [ -f ".data_loader_pid" ]; then
        pid=$(cat .data_loader_pid)
        echo "后台进程PID: $pid"
        
        if kill -0 $pid 2>/dev/null; then
            echo "✅ 后台进程正在运行"
        else
            echo "❌ 后台进程未运行"
            return 1
        fi
    else
        echo "❌ 未找到PID文件"
        return 1
    fi
    
    # 测试stop.sh
    echo ""
    echo "执行 ./stop.sh"
    ./stop.sh
    
    # 检查是否成功停止
    sleep 2
    if [ -f ".data_loader_pid" ]; then
        echo "❌ PID文件仍然存在"
    else
        echo "✅ PID文件已清理"
    fi
    
    # 检查进程是否还在运行
    remaining=$(pgrep -f "cmd/main.go" 2>/dev/null || true)
    if [ -n "$remaining" ]; then
        echo "❌ 发现残留进程: $remaining"
        echo "强制清理..."
        echo $remaining | xargs kill -KILL 2>/dev/null || true
    else
        echo "✅ 所有进程已停止"
    fi
    
    echo ""
    echo "✅ 测试4完成"
    echo ""
}

# 主测试函数
run_tests() {
    echo "选择要运行的测试:"
    echo "1) go run信号处理测试"
    echo "2) start.sh脚本信号处理测试"
    echo "3) 进程层级分析测试"
    echo "4) stop.sh脚本功能测试"
    echo "5) 运行所有测试"
    echo ""
    read -p "请输入选择 (1-5): " choice
    
    case $choice in
        1)
            test_go_run_signal
            ;;
        2)
            test_start_script_signal
            ;;
        3)
            test_process_hierarchy
            ;;
        4)
            test_stop_script
            ;;
        5)
            echo "🚀 运行所有测试..."
            echo ""
            test_go_run_signal
            test_start_script_signal
            test_process_hierarchy
            test_stop_script
            ;;
        *)
            echo "❌ 无效选择"
            exit 1
            ;;
    esac
}

# 检查前置条件
check_prerequisites() {
    echo "🔍 检查前置条件..."
    
    # 检查Go环境
    if ! command -v go &> /dev/null; then
        echo "❌ 未找到Go环境"
        exit 1
    fi
    echo "✅ Go环境: $(go version)"
    
    # 检查必要文件
    if [ ! -f "cmd/main.go" ]; then
        echo "❌ 未找到cmd/main.go"
        exit 1
    fi
    echo "✅ 找到cmd/main.go"
    
    if [ ! -f "start.sh" ]; then
        echo "❌ 未找到start.sh"
        exit 1
    fi
    echo "✅ 找到start.sh"
    
    if [ ! -f "stop.sh" ]; then
        echo "❌ 未找到stop.sh"
        exit 1
    fi
    echo "✅ 找到stop.sh"
    
    if [ ! -f "config.yml" ]; then
        echo "❌ 未找到config.yml"
        exit 1
    fi
    echo "✅ 找到config.yml"
    
    echo ""
}

# 主程序
main() {
    check_prerequisites
    run_tests
    
    echo "🎉 测试完成！"
    echo ""
    echo "📝 测试结果总结:"
    echo "- 如果go run无法通过Ctrl+C停止，说明存在信号传递问题"
    echo "- 如果start.sh无法优雅停止，说明trap机制有问题"
    echo "- 如果发现多层进程，说明go run创建了子进程"
    echo "- 如果stop.sh无法清理进程，说明进程查找逻辑有问题"
    echo ""
}

# 如果直接运行脚本
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main "$@"
fi