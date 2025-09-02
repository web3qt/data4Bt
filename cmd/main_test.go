package main

import (
	"fmt"
	"reflect"
	"testing"
)

// TestParseSymbolsParameter 测试交易对参数解析函数
func TestParseSymbolsParameter(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "单个交易对",
			input:    "BTCUSDT",
			expected: []string{"BTCUSDT"},
		},
		{
			name:     "多个交易对",
			input:    "BTCUSDT,ETHUSDT,ADAUSDT",
			expected: []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"},
		},
		{
			name:     "带空格的交易对",
			input:    "BTCUSDT, ETHUSDT , ADAUSDT",
			expected: []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"},
		},
		{
			name:     "小写交易对（应转换为大写）",
			input:    "btcusdt,ethusdt",
			expected: []string{"BTCUSDT", "ETHUSDT"},
		},
		{
			name:     "混合大小写",
			input:    "BtcUsdt,ethUSDT,ADAusdt",
			expected: []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"},
		},
		{
			name:     "空字符串",
			input:    "",
			expected: nil,
		},
		{
			name:     "只有逗号",
			input:    ",,,",
			expected: nil,
		},
		{
			name:     "空元素混合",
			input:    "BTCUSDT,,ETHUSDT,",
			expected: []string{"BTCUSDT", "ETHUSDT"},
		},
		{
			name:     "单个字符",
			input:    "A",
			expected: []string{"A"},
		},
		{
			name:     "特殊字符（数字）",
			input:    "BTC1USDT,ETH2USDT",
			expected: []string{"BTC1USDT", "ETH2USDT"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseSymbolsParameter(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("parseSymbolsParameter(%q) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestParseSymbolsParameterEdgeCases 测试边界情况
func TestParseSymbolsParameterEdgeCases(t *testing.T) {
	// 测试非常长的输入
	longInput := ""
	expectedLong := make([]string, 100)
	for i := 0; i < 100; i++ {
		if i > 0 {
			longInput += ","
		}
		symbol := fmt.Sprintf("SYM%dUSDT", i)
		longInput += symbol
		expectedLong[i] = symbol
	}

	result := parseSymbolsParameter(longInput)
	if !reflect.DeepEqual(result, expectedLong) {
		t.Errorf("parseSymbolsParameter with long input failed")
	}

	// 测试只有空格的输入
	result = parseSymbolsParameter("   ")
	if result != nil {
		t.Errorf("parseSymbolsParameter with only spaces should return nil, got %v", result)
	}

	// 测试制表符和换行符
	result = parseSymbolsParameter("\t\n")
	if result != nil {
		t.Errorf("parseSymbolsParameter with tabs and newlines should return nil, got %v", result)
	}
}

// BenchmarkParseSymbolsParameter 性能基准测试
func BenchmarkParseSymbolsParameter(b *testing.B) {
	testCases := []string{
		"BTCUSDT",
		"BTCUSDT,ETHUSDT,ADAUSDT",
		"BTCUSDT, ETHUSDT , ADAUSDT , BNBUSDT , DOTUSDT",
	}

	for _, tc := range testCases {
		b.Run(tc, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				parseSymbolsParameter(tc)
			}
		})
	}
}

// TestParseSymbolsParameterConcurrency 并发安全测试
func TestParseSymbolsParameterConcurrency(t *testing.T) {
	input := "BTCUSDT,ETHUSDT,ADAUSDT"
	expected := []string{"BTCUSDT", "ETHUSDT", "ADAUSDT"}

	// 启动多个goroutine并发调用函数
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				result := parseSymbolsParameter(input)
				if !reflect.DeepEqual(result, expected) {
					t.Errorf("Concurrent call failed: got %v, expected %v", result, expected)
					return
				}
			}
			done <- true
		}()
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}
}