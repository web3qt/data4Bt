package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
)

// ProgressReporter 进度报告器
type ProgressReporter struct {
	config   config.MonitoringConfig
	logger   zerolog.Logger
	progress map[string]*domain.ProgressReport
	mutex    sync.RWMutex
	server   *http.Server
	startTime time.Time
	totalTasks int
	completedTasks int
}

// NewProgressReporter 创建新的进度报告器
func NewProgressReporter(cfg config.MonitoringConfig) *ProgressReporter {
	return &ProgressReporter{
		config:    cfg,
		logger:    logger.GetLogger("progress_reporter"),
		progress:  make(map[string]*domain.ProgressReport),
		startTime: time.Now(),
	}
}

// Start 启动进度报告器
func (p *ProgressReporter) Start(totalTasks int) error {
	p.totalTasks = totalTasks
	p.startTime = time.Now()

	if !p.config.Enabled {
		p.logger.Info().Msg("Progress monitoring disabled")
		return nil
	}

	p.logger.Info().
		Int("total_tasks", totalTasks).
		Int("port", p.config.MetricsPort).
		Msg("Starting progress reporter")

	ctx := context.Background()

	// 启动HTTP服务器
	if err := p.startHTTPServer(ctx); err != nil {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	// 启动进度报告协程
	go p.progressReporter(ctx)

	return nil
}

// ReportProgress 报告进度
func (p *ProgressReporter) ReportProgress(progress *domain.ProgressReport) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	key := fmt.Sprintf("%s_%s", progress.CurrentSymbol, progress.CurrentDate.Format("2006-01-02"))
	p.progress[key] = progress

	p.completedTasks = progress.CompletedTasks

	// 记录进度日志
	logger.LogProgress("progress_reporter", progress.CompletedTasks, progress.TotalTasks, 
		fmt.Sprintf("Progress updated for %s on %s", progress.CurrentSymbol, progress.CurrentDate.Format("2006-01-02")))
}

// GetOverallProgress 获取总体进度
func (p *ProgressReporter) GetOverallProgress() map[string]interface{} {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	elapsed := time.Since(p.startTime)
	completionRate := float64(p.completedTasks) / float64(p.totalTasks) * 100

	// 计算预估剩余时间
	var estimatedTimeRemaining time.Duration
	if p.completedTasks > 0 {
		avgTimePerTask := elapsed / time.Duration(p.completedTasks)
		remainingTasks := p.totalTasks - p.completedTasks
		estimatedTimeRemaining = avgTimePerTask * time.Duration(remainingTasks)
	}

	// 统计记录数
	totalRecords := 0
	for _, progress := range p.progress {
		totalRecords += progress.TotalTasks
	}

	return map[string]interface{}{
		"start_time":               p.startTime,
		"elapsed_time":             elapsed,
		"total_tasks":              p.totalTasks,
		"completed_tasks":          p.completedTasks,
		"remaining_tasks":          p.totalTasks - p.completedTasks,
		"completion_rate":          completionRate,
		"estimated_time_remaining": estimatedTimeRemaining,
		"total_records":            totalRecords,
		"tasks_per_minute":         float64(p.completedTasks) / elapsed.Minutes(),
		"active_symbols":           len(p.progress),
	}
}

// GetDetailedProgress 获取详细进度
func (p *ProgressReporter) GetDetailedProgress() map[string]*domain.ProgressReport {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	result := make(map[string]*domain.ProgressReport)
	for key, progress := range p.progress {
		result[key] = progress
	}

	return result
}

// GetSymbolProgress 获取指定交易对的进度
func (p *ProgressReporter) GetSymbolProgress(symbol string) []*domain.ProgressReport {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	var result []*domain.ProgressReport
	for _, progress := range p.progress {
		if progress.CurrentSymbol == symbol {
			result = append(result, progress)
		}
	}

	return result
}

// startHTTPServer 启动HTTP服务器
func (p *ProgressReporter) startHTTPServer(ctx context.Context) error {
	mux := http.NewServeMux()

	// 总体进度端点
	mux.HandleFunc("/progress", p.handleOverallProgress)

	// 详细进度端点
	mux.HandleFunc("/progress/detailed", p.handleDetailedProgress)

	// 交易对进度端点
	mux.HandleFunc("/progress/symbol/", p.handleSymbolProgress)

	// 健康检查端点
	mux.HandleFunc("/health", p.handleHealth)

	// 静态文件服务（如果需要Web界面）
	mux.HandleFunc("/", p.handleDashboard)

	p.server = &http.Server{
		Addr: fmt.Sprintf(":%d", p.config.MetricsPort),
		Handler: mux,
	}

	go func() {
		if err := p.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			p.logger.Error().Err(err).Msg("HTTP server error")
		}
	}()

	// 等待服务器启动
	time.Sleep(100 * time.Millisecond)

	p.logger.Info().
		Int("port", p.config.MetricsPort).
		Msg("HTTP server started")

	return nil
}

// progressReporter 定期报告进度
func (p *ProgressReporter) progressReporter(ctx context.Context) {
	if p.config.ProgressReportInterval <= 0 {
		return
	}

	ticker := time.NewTicker(p.config.ProgressReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.logProgress()
		}
	}
}

// logProgress 记录进度日志
func (p *ProgressReporter) logProgress() {
	progress := p.GetOverallProgress()

	p.logger.Info().
		Int("total_tasks", progress["total_tasks"].(int)).
		Int("completed_tasks", progress["completed_tasks"].(int)).
		Int("remaining_tasks", progress["remaining_tasks"].(int)).
		Float64("completion_rate", progress["completion_rate"].(float64)).
		Dur("elapsed_time", progress["elapsed_time"].(time.Duration)).
		Dur("estimated_time_remaining", progress["estimated_time_remaining"].(time.Duration)).
		Int("total_records", progress["total_records"].(int)).
		Float64("tasks_per_minute", progress["tasks_per_minute"].(float64)).
		Msg("Progress report")
}

// HTTP处理器

// handleOverallProgress 处理总体进度请求
func (p *ProgressReporter) handleOverallProgress(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	progress := p.GetOverallProgress()
	if err := json.NewEncoder(w).Encode(progress); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleDetailedProgress 处理详细进度请求
func (p *ProgressReporter) handleDetailedProgress(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	progress := p.GetDetailedProgress()
	if err := json.NewEncoder(w).Encode(progress); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleSymbolProgress 处理交易对进度请求
func (p *ProgressReporter) handleSymbolProgress(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// 从URL路径中提取交易对
	path := r.URL.Path
	if len(path) < len("/progress/symbol/") {
		http.Error(w, "Symbol not specified", http.StatusBadRequest)
		return
	}

	symbol := path[len("/progress/symbol/"):]
	if symbol == "" {
		http.Error(w, "Symbol not specified", http.StatusBadRequest)
		return
	}

	progress := p.GetSymbolProgress(symbol)
	if err := json.NewEncoder(w).Encode(progress); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleHealth 处理健康检查请求
func (p *ProgressReporter) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"uptime":    time.Since(p.startTime),
	}

	if err := json.NewEncoder(w).Encode(health); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleDashboard 处理仪表板请求
func (p *ProgressReporter) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/html")

	dashboardHTML := `
<!DOCTYPE html>
<html>
<head>
    <title>Binance Data Loader - Progress Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .card { background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .progress-bar { width: 100%; height: 20px; background-color: #e0e0e0; border-radius: 10px; overflow: hidden; }
        .progress-fill { height: 100%; background-color: #4caf50; transition: width 0.3s ease; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }
        .stat { text-align: center; }
        .stat-value { font-size: 2em; font-weight: bold; color: #333; }
        .stat-label { color: #666; margin-top: 5px; }
        .refresh-btn { background: #2196f3; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; }
        .refresh-btn:hover { background: #1976d2; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Binance Data Loader - Progress Dashboard</h1>
        
        <div class="card">
            <h2>Overall Progress</h2>
            <div id="progress-container">
                <div class="progress-bar">
                    <div class="progress-fill" id="progress-fill" style="width: 0%"></div>
                </div>
                <p id="progress-text">Loading...</p>
            </div>
        </div>
        
        <div class="card">
            <h2>Statistics</h2>
            <div class="stats" id="stats">
                <!-- Stats will be populated by JavaScript -->
            </div>
        </div>
        
        <div class="card">
            <button class="refresh-btn" onclick="loadProgress()">Refresh</button>
            <p><small>Auto-refresh every 5 seconds</small></p>
        </div>
    </div>
    
    <script>
        function loadProgress() {
            fetch('/progress')
                .then(response => response.json())
                .then(data => {
                    updateProgress(data);
                })
                .catch(error => {
                    console.error('Error loading progress:', error);
                });
        }
        
        function updateProgress(data) {
            // Update progress bar
            const progressFill = document.getElementById('progress-fill');
            const progressText = document.getElementById('progress-text');
            
            progressFill.style.width = data.completion_rate + '%';
            progressText.textContent = data.completed_tasks + ' / ' + data.total_tasks + ' tasks completed (' + data.completion_rate.toFixed(1) + '%)';
            
            // Update stats
            const statsContainer = document.getElementById('stats');
            statsContainer.innerHTML = 
                '<div class="stat">' +
                    '<div class="stat-value">' + data.completed_tasks + '</div>' +
                    '<div class="stat-label">Completed Tasks</div>' +
                '</div>' +
                '<div class="stat">' +
                    '<div class="stat-value">' + data.remaining_tasks + '</div>' +
                    '<div class="stat-label">Remaining Tasks</div>' +
                '</div>' +
                '<div class="stat">' +
                    '<div class="stat-value">' + formatDuration(data.elapsed_time) + '</div>' +
                    '<div class="stat-label">Elapsed Time</div>' +
                '</div>' +
                '<div class="stat">' +
                    '<div class="stat-value">' + formatDuration(data.estimated_time_remaining) + '</div>' +
                    '<div class="stat-label">Est. Time Remaining</div>' +
                '</div>' +
                '<div class="stat">' +
                    '<div class="stat-value">' + data.total_records.toLocaleString() + '</div>' +
                    '<div class="stat-label">Total Records</div>' +
                '</div>' +
                '<div class="stat">' +
                    '<div class="stat-value">' + data.tasks_per_minute.toFixed(1) + '</div>' +
                    '<div class="stat-label">Tasks/Minute</div>' +
                '</div>';
        }
        
        function formatDuration(nanoseconds) {
            const seconds = Math.floor(nanoseconds / 1000000000);
            const hours = Math.floor(seconds / 3600);
            const minutes = Math.floor((seconds % 3600) / 60);
            const secs = seconds % 60;
            
            if (hours > 0) {
                return hours + 'h ' + minutes + 'm ' + secs + 's';
            } else if (minutes > 0) {
                return minutes + 'm ' + secs + 's';
            } else {
                return secs + 's';
            }
        }
        
        // Load initial progress
        loadProgress();
        
        // Auto-refresh every 5 seconds
        setInterval(loadProgress, 5000);
    </script>
</body>
</html>
`

	w.Write([]byte(dashboardHTML))
}

// Stop 停止进度报告器
func (p *ProgressReporter) Stop(ctx context.Context) error {
	if p.server != nil {
		p.logger.Info().Msg("Stopping HTTP server")
		return p.server.Shutdown(ctx)
	}
	return nil
}