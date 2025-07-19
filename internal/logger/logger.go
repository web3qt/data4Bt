package logger

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"binance-data-loader/internal/config"
)

// Init 初始化日志系统
func Init(cfg config.LogConfig) error {
	// 设置日志级别
	level, err := zerolog.ParseLevel(strings.ToLower(cfg.Level))
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
	
	// 设置时间格式
	zerolog.TimeFieldFormat = time.RFC3339
	
	var writer io.Writer
	
	// 根据输出配置设置writer
	switch strings.ToLower(cfg.Output) {
	case "file":
		if cfg.FilePath == "" {
			cfg.FilePath = "logs/app.log"
		}
		
		// 创建日志目录
		logDir := filepath.Dir(cfg.FilePath)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return err
		}
		
		// 打开日志文件
		file, err := os.OpenFile(cfg.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		writer = file
		
	case "stdout":
		fallthrough
	default:
		writer = os.Stdout
	}
	
	// 根据格式配置设置输出格式
	switch strings.ToLower(cfg.Format) {
	case "console":
		// 控制台友好格式
		writer = zerolog.ConsoleWriter{
			Out:        writer,
			TimeFormat: "2006-01-02 15:04:05",
			NoColor:    cfg.Output == "file", // 文件输出时不使用颜色
		}
	case "json":
		fallthrough
	default:
		// JSON格式（默认）
	}
	
	// 设置全局logger
	log.Logger = zerolog.New(writer).With().Timestamp().Logger()
	
	return nil
}

// GetLogger 获取带有组件名称的logger
func GetLogger(component string) zerolog.Logger {
	return log.With().Str("component", component).Logger()
}

// GetLoggerWithFields 获取带有自定义字段的logger
func GetLoggerWithFields(fields map[string]interface{}) zerolog.Logger {
	logger := log.Logger
	for key, value := range fields {
		logger = logger.With().Interface(key, value).Logger()
	}
	return logger
}

// LogProgress 记录进度信息
func LogProgress(component string, current, total int, message string) {
	progress := float64(current) / float64(total) * 100
	log.Info().
		Str("component", component).
		Int("current", current).
		Int("total", total).
		Float64("progress", progress).
		Msg(message)
}

// LogError 记录错误信息
func LogError(component string, err error, message string, fields ...map[string]interface{}) {
	logger := log.Error().
		Str("component", component).
		Err(err)
	
	// 添加额外字段
	for _, fieldMap := range fields {
		for key, value := range fieldMap {
			logger = logger.Interface(key, value)
		}
	}
	
	logger.Msg(message)
}

// LogPerformance 记录性能指标
func LogPerformance(component string, operation string, duration time.Duration, fields ...map[string]interface{}) {
	logger := log.Info().
		Str("component", component).
		Str("operation", operation).
		Dur("duration", duration).
		Float64("duration_ms", float64(duration.Nanoseconds())/1e6)
	
	// 添加额外字段
	for _, fieldMap := range fields {
		for key, value := range fieldMap {
			logger = logger.Interface(key, value)
		}
	}
	
	logger.Msg("Performance metric")
}

// LogDataQuality 记录数据质量信息
func LogDataQuality(component string, symbol string, totalRows, validRows, invalidRows int) {
	validityRate := float64(validRows) / float64(totalRows) * 100
	
	log.Info().
		Str("component", component).
		Str("symbol", symbol).
		Int("total_rows", totalRows).
		Int("valid_rows", validRows).
		Int("invalid_rows", invalidRows).
		Float64("validity_rate", validityRate).
		Msg("Data quality report")
}

// LogSystemMetrics 记录系统指标
func LogSystemMetrics(component string, metrics map[string]interface{}) {
	logger := log.Info().
		Str("component", component).
		Str("type", "system_metrics")
	
	for key, value := range metrics {
		logger = logger.Interface(key, value)
	}
	
	logger.Msg("System metrics")
}

// LogTaskStatus 记录任务状态
func LogTaskStatus(component string, taskID string, status string, details map[string]interface{}) {
	logger := log.Info().
		Str("component", component).
		Str("task_id", taskID).
		Str("status", status)
	
	for key, value := range details {
		logger = logger.Interface(key, value)
	}
	
	logger.Msg("Task status update")
}