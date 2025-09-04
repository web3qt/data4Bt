package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_ValidConfig(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_config.yml")
	
	configYAML := `
log:
  level: "debug"
  format: "json"
  output: "stdout"

binance:
  base_url: "https://data.binance.vision"
  data_path: "/data/spot/daily/klines"
  symbols_filter: "USDT"
  interval: "1m"
  timeout: 120s
  retry_count: 5
  retry_delay: 10s

database:
  clickhouse:
    hosts:
      - "localhost:9000"
    database: "test_db"
    username: "default"
    password: "test_password"
    compression: "lz4"
    dial_timeout: 60s
    max_open_conns: 10
    max_idle_conns: 5
    conn_max_lifetime: 10m

downloader:
  concurrency: 10
  buffer_size: 100

parser:
  concurrency: 5
  buffer_size: 50

importer:
  batch_size: 10000
  buffer_size: 20
  flush_interval: 30s
`
	
	err := os.WriteFile(configFile, []byte(configYAML), 0644)
	if err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}
	
	cfg, err := Load(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	
	// 验证基本配置
	if cfg.Log.Level != "debug" {
		t.Errorf("Expected log level debug, got %s", cfg.Log.Level)
	}
	if cfg.Binance.BaseURL != "https://data.binance.vision" {
		t.Errorf("Expected base URL https://data.binance.vision, got %s", cfg.Binance.BaseURL)
	}
	if cfg.Database.ClickHouse.Database != "test_db" {
		t.Errorf("Expected database test_db, got %s", cfg.Database.ClickHouse.Database)
	}
}

func TestLoad_DefaultValues(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "minimal_config.yml")
	
	minimalYAML := `
log:
  level: "info"

binance:
  base_url: "https://data.binance.vision"

database:
  clickhouse:
    hosts:
      - "localhost:9000"
    database: "minimal_db"
    username: "default"
    password: "password"
`
	
	err := os.WriteFile(configFile, []byte(minimalYAML), 0644)
	if err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}
	
	cfg, err := Load(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	
	// 验证默认值被设置
	if cfg.Downloader.Concurrency != 10 {
		t.Errorf("Expected default downloader concurrency 10, got %d", cfg.Downloader.Concurrency)
	}
	if cfg.Parser.Concurrency != 5 {
		t.Errorf("Expected default parser concurrency 5, got %d", cfg.Parser.Concurrency)
	}
	if cfg.Importer.BatchSize != 2 {
		t.Errorf("Expected default importer batch size 2, got %d", cfg.Importer.BatchSize)
	}
}

func TestLoad_NonexistentFile(t *testing.T) {
	_, err := Load("nonexistent.yml")
	if err == nil {
		t.Error("Expected error when loading nonexistent file, but got nil")
	}
}

func TestDetectCurrentEnvironment(t *testing.T) {
	// 保存原始环境变量
	originalEnv := os.Getenv("BDL_ENV")
	defer func() {
		if originalEnv != "" {
			os.Setenv("BDL_ENV", originalEnv)
		} else {
			os.Unsetenv("BDL_ENV")
		}
	}()
	
	// 测试环境变量检测
	os.Setenv("BDL_ENV", "prod")
	env := DetectCurrentEnvironment()
	if env != EnvProduction {
		t.Errorf("Expected production environment, got %s", env)
	}
	
	// 测试默认环境
	os.Unsetenv("BDL_ENV")
	env = DetectCurrentEnvironment()
	if env != EnvDevelopment {
		t.Errorf("Expected development environment as default, got %s", env)
	}
}

func TestValidateConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	
	// 测试有效文件
	validFile := filepath.Join(tempDir, "valid.yml")
	err := os.WriteFile(validFile, []byte("key: value"), 0644)
	if err != nil {
		t.Fatalf("Failed to create valid file: %v", err)
	}
	
	err = ValidateConfigFile(validFile)
	if err != nil {
		t.Errorf("Expected valid file to pass validation, got error: %v", err)
	}
	
	// 测试无效文件
	err = ValidateConfigFile("nonexistent.yml")
	if err == nil {
		t.Error("Expected nonexistent file to fail validation")
	}
	
	// 测试空路径
	err = ValidateConfigFile("")
	if err == nil {
		t.Error("Expected empty path to fail validation")
	}
}

// Benchmark tests
func BenchmarkLoad(b *testing.B) {
	tempDir := b.TempDir()
	configFile := filepath.Join(tempDir, "bench_config.yml")
	
	configYAML := `
log:
  level: "info"
binance:
  base_url: "https://data.binance.vision"
database:
  clickhouse:
    hosts: ["localhost:9000"]
    database: "bench_db"
    username: "default"
    password: "password"
`
	
	os.WriteFile(configFile, []byte(configYAML), 0644)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_, _ = Load(configFile)
	}
}

func BenchmarkDetectCurrentEnvironment(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = DetectCurrentEnvironment()
	}
}