#!/bin/bash

# Data4BT verify-data 命令集成测试脚本
# 测试所有verify-data命令的功能和参数组合

set -e  # 遇到错误立即退出

echo "🧪 开始 verify-data 命令集成测试"
echo "======================================"

# 测试计数器
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# 测试结果记录
TEST_RESULTS=()

# 辅助函数：运行测试
run_test() {
    local test_name="$1"
    local command="$2"
    local expected_exit_code="${3:-0}"  # 默认期望退出码为0
    local should_contain="$4"  # 可选：输出应包含的内容
    local should_not_contain="$5"  # 可选：输出不应包含的内容
    
    echo "\n📋 测试: $test_name"
    echo "命令: $command"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    # 执行命令并捕获输出和退出码
    set +e  # 临时允许命令失败
    output=$(eval "$command" 2>&1)
    actual_exit_code=$?
    set -e
    
    # 检查退出码
    if [ "$actual_exit_code" -eq "$expected_exit_code" ]; then
        echo "✅ 退出码正确: $actual_exit_code"
    else
        echo "❌ 退出码错误: 期望 $expected_exit_code, 实际 $actual_exit_code"
        TEST_RESULTS+=("❌ $test_name: 退出码错误")
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
    
    # 检查输出内容（如果指定）
    if [ -n "$should_contain" ]; then
        if echo "$output" | grep -q "$should_contain"; then
            echo "✅ 输出包含期望内容: $should_contain"
        else
            echo "❌ 输出不包含期望内容: $should_contain"
            TEST_RESULTS+=("❌ $test_name: 输出内容错误")
            FAILED_TESTS=$((FAILED_TESTS + 1))
            return 1
        fi
    fi
    
    # 检查输出不应包含的内容（如果指定）
    if [ -n "$should_not_contain" ]; then
        if echo "$output" | grep -q "$should_not_contain"; then
            echo "❌ 输出包含不应有的内容: $should_not_contain"
            TEST_RESULTS+=("❌ $test_name: 输出包含不应有内容")
            FAILED_TESTS=$((FAILED_TESTS + 1))
            return 1
        else
            echo "✅ 输出不包含不应有内容: $should_not_contain"
        fi
    fi
    
    echo "✅ 测试通过: $test_name"
    TEST_RESULTS+=("✅ $test_name: 通过")
    PASSED_TESTS=$((PASSED_TESTS + 1))
}

# 测试1: 命令识别测试
echo "\n🔍 测试组1: 命令识别"
run_test "命令识别" \
    "go run cmd/main.go -cmd=verify-data" \
    1 \
    "symbols parameter is required" \
    "unknown command"

# 测试2: 参数验证测试
echo "\n🔍 测试组2: 参数验证"
run_test "空symbols参数" \
    "go run cmd/main.go -cmd=verify-data -symbols=" \
    1 \
    "symbols parameter is required"

run_test "无效开始日期" \
    "go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT -start=invalid-date" \
    1 \
    "invalid start date format"

run_test "无效结束日期" \
    "go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT -end=invalid-date" \
    1 \
    "invalid end date format"

# 测试3: 基本功能测试
echo "\n🔍 测试组3: 基本功能"
run_test "单个交易对验证" \
    "go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT" \
    0 \
    "开始验证数据质量"

run_test "多个交易对验证" \
    "go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT,ETHUSDT" \
    0 \
    "开始验证数据质量"

run_test "大小写混合交易对" \
    "go run cmd/main.go -cmd=verify-data -symbols=btcusdt,ETHusdt" \
    0 \
    "开始验证数据质量"

# 测试4: 时间参数测试
echo "\n🔍 测试组4: 时间参数"
run_test "指定时间范围" \
    "go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT -start=2024-01-01 -end=2024-01-02" \
    0 \
    "时间范围: 2024-01-01 到 2024-01-02"

run_test "只指定开始时间" \
    "go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT -start=2024-01-01" \
    0 \
    "开始验证数据质量"

run_test "只指定结束时间" \
    "go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT -end=2024-01-02" \
    0 \
    "开始验证数据质量"

# 测试5: 输出模式测试
echo "\n🔍 测试组5: 输出模式"
run_test "简洁模式输出" \
    "go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT" \
    0 \
    "验证结论"

run_test "详细模式输出" \
    "go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT -detailed=true" \
    0 \
    '"total_symbols"'

# 测试6: 边界条件测试
echo "\n🔍 测试组6: 边界条件"
run_test "带空格的交易对" \
    "go run cmd/main.go -cmd=verify-data -symbols='BTCUSDT, ETHUSDT , ADAUSDT'" \
    0 \
    "开始验证数据质量"

run_test "单字符交易对" \
    "go run cmd/main.go -cmd=verify-data -symbols=A" \
    0 \
    "开始验证数据质量"

# 测试7: 性能测试
echo "\n🔍 测试组7: 性能测试"
run_test "多交易对性能" \
    "go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,DOTUSDT" \
    0 \
    "开始验证数据质量"

# 测试8: 兼容性测试
echo "\n🔍 测试组8: 兼容性测试"
run_test "验证现有命令仍然工作" \
    "go run cmd/main.go -cmd=check-quality --help || go run cmd/main.go -cmd=status" \
    0

run_test "验证help信息" \
    "go run cmd/main.go -cmd=unknown-command" \
    1 \
    "unknown command"

# 输出测试结果摘要
echo "\n\n📊 测试结果摘要"
echo "======================================"
echo "总测试数: $TOTAL_TESTS"
echo "通过: $PASSED_TESTS"
echo "失败: $FAILED_TESTS"
echo "成功率: $(( PASSED_TESTS * 100 / TOTAL_TESTS ))%"

echo "\n📋 详细结果:"
for result in "${TEST_RESULTS[@]}"; do
    echo "  $result"
done

# 最终结果
if [ "$FAILED_TESTS" -eq 0 ]; then
    echo "\n🎉 所有集成测试通过！"
    echo "✅ verify-data 命令功能完整，可以投入使用"
    exit 0
else
    echo "\n❌ 有 $FAILED_TESTS 个测试失败"
    echo "🔧 请检查失败的测试并修复问题"
    exit 1
fi