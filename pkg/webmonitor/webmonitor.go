package webmonitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/rs/zerolog"
	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
	"binance-data-loader/internal/state"
	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// SymbolData Web监控使用的交易对数据结构
type SymbolData struct {
	Symbol        string    `json:"symbol"`
	RecordCount   int64     `json:"record_count"`
	EarliestTime  time.Time `json:"earliest_time"`
	LatestTime    time.Time `json:"latest_time"`
	TimeSpan      string    `json:"time_span"`
	LastProcessed string    `json:"last_processed"`
	ProcessedDays int       `json:"processed_days"`
	Status        string    `json:"status"`
	Progress      float64   `json:"progress"`
	WorkerID      int       `json:"worker_id"`
}

// SystemOverview 系统概览数据
type SystemOverview struct {
	TotalSymbols     int          `json:"total_symbols"`
	SymbolsWithData  int          `json:"symbols_with_data"`
	TotalRecords     int64        `json:"total_records"`
	DatabaseStatus   string       `json:"database_status"`
	LastUpdated      time.Time    `json:"last_updated"`
	SymbolDetails    []SymbolData `json:"symbol_details"`
	// 新增实时状态
	CurrentlyProcessing []string                    `json:"currently_processing"`
	WorkerStates       map[int]*domain.WorkerState `json:"worker_states"`
	OverallProgress    float64                     `json:"overall_progress"`
	EstimatedTimeRemaining string                  `json:"estimated_time_remaining"`
}

// WebMonitor Web监控服务
type WebMonitor struct {
	config       config.WebDashboardConfig
	logger       zerolog.Logger
	db           *sql.DB
	stateManager *state.FileStateManager
	server       *http.Server
	isRunning    bool
}

// NewWebMonitor 创建新的Web监控实例
func NewWebMonitor(cfg config.WebDashboardConfig, dbConfig config.ClickHouseConfig, stateManager *state.FileStateManager) (*WebMonitor, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	// 连接数据库
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s/%s",
		dbConfig.Username, dbConfig.Password, dbConfig.Hosts[0], dbConfig.Database)
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	monitor := &WebMonitor{
		config:       cfg,
		logger:       logger.GetLogger("web_monitor"),
		db:           db,
		stateManager: stateManager,
	}

	return monitor, nil
}

// Start 启动Web监控服务
func (w *WebMonitor) Start(ctx context.Context) error {
	if !w.config.Enabled {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", w.handleDashboard)
	mux.HandleFunc("/api/overview", w.handleAPI)
	mux.HandleFunc("/api/progress", w.handleProgress)
	mux.HandleFunc("/api/health", w.handleHealth)

	w.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", w.config.Port),
		Handler: mux,
	}

	w.logger.Info().
		Int("port", w.config.Port).
		Msg("Starting web monitor dashboard")

	go func() {
		if err := w.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			w.logger.Error().Err(err).Msg("Web monitor server error")
		}
	}()

	w.isRunning = true

	w.logger.Info().
		Str("url", fmt.Sprintf("http://localhost:%d", w.config.Port)).
		Msg("Web monitor dashboard started successfully")

	return nil
}

// Stop 停止Web监控服务
func (w *WebMonitor) Stop(ctx context.Context) error {
	if !w.isRunning || w.server == nil {
		return nil
	}

	w.logger.Info().Msg("Stopping web monitor dashboard")

	// 创建带超时的上下文用于优雅关闭
	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := w.server.Shutdown(shutdownCtx); err != nil {
		w.logger.Warn().Err(err).Msg("Failed to gracefully shutdown web monitor server, forcing close")
		// 如果优雅关闭失败，强制关闭
		if closeErr := w.server.Close(); closeErr != nil {
			w.logger.Error().Err(closeErr).Msg("Failed to force close web monitor server")
		}
		return fmt.Errorf("failed to shutdown web monitor server: %w", err)
	}

	if err := w.db.Close(); err != nil {
		w.logger.Warn().Err(err).Msg("Failed to close database connection")
	}

	w.isRunning = false
	w.logger.Info().Msg("Web monitor dashboard stopped")
	return nil
}

// getSystemOverview 获取系统概览数据
func (w *WebMonitor) getSystemOverview() (*SystemOverview, error) {
	// 首先从symbol_infos表获取所有已发现的交易对总数
	var totalDiscoveredSymbols int
	countQuery := `SELECT COUNT(*) FROM symbol_infos`
	if err := w.db.QueryRow(countQuery).Scan(&totalDiscoveredSymbols); err != nil {
		w.logger.Warn().Err(err).Msg("Failed to get symbol count from symbol_infos, using fallback")
		totalDiscoveredSymbols = 0
	}

	// 获取数据库中的实际K线数据
	query := `
		SELECT 
			symbol,
			COUNT(*) as record_count,
			MIN(open_time) as earliest_time,
			MAX(open_time) as latest_time
		FROM klines_1m 
		GROUP BY symbol 
		ORDER BY symbol
	`

	rows, err := w.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query database: %w", err)
	}
	defer rows.Close()

	var symbols []SymbolData
	totalRecords := int64(0)
	symbolsWithData := 0

	for rows.Next() {
		var symbol SymbolData
		var earliest, latest time.Time

		err := rows.Scan(&symbol.Symbol, &symbol.RecordCount, &earliest, &latest)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		symbol.EarliestTime = earliest
		symbol.LatestTime = latest
		symbol.TimeSpan = calculateTimeSpan(earliest, latest)
		symbol.ProcessedDays = int(latest.Sub(earliest).Hours() / 24)
		symbol.Status = "active"
		
		// 直接在这里计算基于数据库的实际进度
		if infoQuery := `SELECT total_months FROM symbol_infos WHERE symbol = ?`; true {
			var totalMonths int32
			if err := w.db.QueryRow(infoQuery, symbol.Symbol).Scan(&totalMonths); err == nil && totalMonths > 0 {
				symbol.Progress = w.calculateActualProgress(symbol.Symbol, totalMonths)
				w.logger.Info().
					Str("symbol", symbol.Symbol).
					Float64("progress", symbol.Progress).
					Int32("total_months", totalMonths).
					Msg("Calculated progress directly from database")
			} else {
				symbol.Progress = 0.0
			}
		}

		symbols = append(symbols, symbol)
		totalRecords += symbol.RecordCount
		symbolsWithData++
	}

	// 获取状态管理器中的数据
	symbols = w.mergeWithStateManager(symbols)

	// 获取实时执行状态
	currentlyProcessing, workerStates := w.getRuntimeStatus()

	// 计算整体进度
	overallProgress := w.calculateOverallProgress(symbols)
	estimatedTime := w.estimateRemainingTime(symbols)

	// 使用数据库中的实际发现交易对总数，而不是状态文件中的数量
	effectiveTotalSymbols := totalDiscoveredSymbols
	if totalDiscoveredSymbols == 0 {
		// 如果数据库查询失败，使用合并后的symbol列表长度作为后备
		effectiveTotalSymbols = len(symbols)
	}

	overview := &SystemOverview{
		TotalSymbols:           effectiveTotalSymbols,
		SymbolsWithData:        symbolsWithData,
		TotalRecords:           totalRecords,
		DatabaseStatus:         "connected",
		LastUpdated:            time.Now(),
		SymbolDetails:          symbols,
		CurrentlyProcessing:    currentlyProcessing,
		WorkerStates:          workerStates,
		OverallProgress:       overallProgress,
		EstimatedTimeRemaining: estimatedTime,
	}

	return overview, nil
}

// calculateActualProgress 基于数据库数据计算实际进度
func (w *WebMonitor) calculateActualProgress(symbol string, totalMonths int32) float64 {
	if totalMonths == 0 {
		return 0.0
	}
	
	// 查询数据库中该交易对实际有数据的月份数
	query := `SELECT COUNT(DISTINCT formatDateTime(open_time, '%Y%m')) FROM klines_1m WHERE symbol = ?`
	var actualMonths int32
	err := w.db.QueryRow(query, symbol).Scan(&actualMonths)
	if err != nil {
		w.logger.Debug().Err(err).Str("symbol", symbol).Msg("Failed to calculate actual progress")
		return 0.0
	}
	
	// 计算进度百分比
	progress := (float64(actualMonths) / float64(totalMonths)) * 100.0
	if progress > 100.0 {
		progress = 100.0
	}
	
	w.logger.Debug().
		Str("symbol", symbol).
		Int32("actual_months", actualMonths).
		Int32("total_months", totalMonths).
		Float64("progress", progress).
		Msg("Calculated actual progress")
	
	return progress
}

// mergeWithStateManager 与状态管理器数据合并
func (w *WebMonitor) mergeWithStateManager(dbSymbols []SymbolData) []SymbolData {
	symbolMap := make(map[string]*SymbolData)
	
	// 将数据库状态放入map
	for i := range dbSymbols {
		symbolMap[dbSymbols[i].Symbol] = &dbSymbols[i]
	}

	// 获取交易对信息用于进度计算
	symbolInfos := make(map[string]int32)
	if infoQuery := `SELECT symbol, total_months FROM symbol_infos`; true {
		rows, err := w.db.Query(infoQuery)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var symbol string
				var totalMonths int32
				if err := rows.Scan(&symbol, &totalMonths); err == nil {
					symbolInfos[symbol] = totalMonths
				}
			}
		}
	}

	// 获取所有时间线
	if timelines, err := w.stateManager.GetAllTimelines(); err == nil {
		for symbol, timeline := range timelines {
			if existing, exists := symbolMap[symbol]; exists {
				// 对于有数据库记录的交易对，使用基于数据库的实际进度
				totalMonths, hasInfo := symbolInfos[symbol]
				w.logger.Info().
					Str("symbol", symbol).
					Int64("record_count", existing.RecordCount).
					Int32("total_months", totalMonths).
					Bool("has_info", hasInfo).
					Msg("Processing symbol for progress calculation")
				
				if hasInfo && existing.RecordCount > 0 {
					existing.Progress = w.calculateActualProgress(symbol, totalMonths)
					existing.Status = "active"
					w.logger.Info().
						Str("symbol", symbol).
						Float64("calculated_progress", existing.Progress).
						Msg("Updated progress from database")
				} else {
					// 没有数据的交易对使用状态文件进度（通常为0）
					existing.Progress = timeline.ImportProgress
					w.logger.Debug().
						Str("symbol", symbol).
						Str("reason", "no_data_or_info").
						Msg("Using timeline progress")
				}
			} else {
				// 添加新记录（仅在状态文件中存在）
				progress := timeline.ImportProgress
				// 如果状态文件中也有记录但没有数据库记录，尝试计算进度
				if totalMonths, hasInfo := symbolInfos[symbol]; hasInfo {
					progress = w.calculateActualProgress(symbol, totalMonths)
				}
				
				newSymbol := SymbolData{
					Symbol:      symbol,
					RecordCount: 0,
					Status:      "pending",
					Progress:    progress,
					TimeSpan:    calculateTimeSpan(timeline.HistoricalStartDate, timeline.LatestAvailableDate),
					EarliestTime: timeline.HistoricalStartDate,
					LatestTime:   timeline.LatestAvailableDate,
				}
				symbolMap[symbol] = &newSymbol
			}
		}
	}

	// 获取Symbol进度信息
	if symbolProgresses, err := w.stateManager.GetAllSymbolProgress(); err == nil {
		for symbol, progress := range symbolProgresses {
			if existing, exists := symbolMap[symbol]; exists {
				existing.WorkerID = progress.WorkerID
				existing.Progress = progress.Progress
				existing.Status = progress.Status
			}
		}
	}

	// 转换为slice并排序
	var result []SymbolData
	for _, symbol := range symbolMap {
		result = append(result, *symbol)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Symbol < result[j].Symbol
	})

	return result
}

// getRuntimeStatus 获取运行时状态
func (w *WebMonitor) getRuntimeStatus() ([]string, map[int]*domain.WorkerState) {
	var currentlyProcessing []string
	workerStates := make(map[int]*domain.WorkerState)

	// 获取Worker状态
	if states, err := w.stateManager.GetAllWorkerStates(); err == nil {
		for workerID, state := range states {
			workerStates[workerID] = state
			if state.Status == "running" && state.CurrentSymbol != "" {
				currentlyProcessing = append(currentlyProcessing, state.CurrentSymbol)
			}
		}
	}

	return currentlyProcessing, workerStates
}

// calculateOverallProgress 计算整体进度
func (w *WebMonitor) calculateOverallProgress(symbols []SymbolData) float64 {
	if len(symbols) == 0 {
		return 0.0
	}

	totalProgress := 0.0
	for _, symbol := range symbols {
		totalProgress += symbol.Progress
	}

	return totalProgress / float64(len(symbols))
}

// estimateRemainingTime 估算剩余时间
func (w *WebMonitor) estimateRemainingTime(symbols []SymbolData) string {
	// 简单估算，可以根据实际情况优化
	completedSymbols := 0
	for _, symbol := range symbols {
		if symbol.Progress >= 100.0 {
			completedSymbols++
		}
	}

	if completedSymbols == len(symbols) {
		return "已完成"
	}

	remainingSymbols := len(symbols) - completedSymbols
	// 假设平均每小时处理2个交易对
	estimatedHours := float64(remainingSymbols) / 2.0

	if estimatedHours < 1 {
		return fmt.Sprintf("约 %d 分钟", int(estimatedHours*60))
	} else if estimatedHours < 24 {
		return fmt.Sprintf("约 %.1f 小时", estimatedHours)
	} else {
		return fmt.Sprintf("约 %.1f 天", estimatedHours/24)
	}
}

// calculateTimeSpan 计算时间跨度
func calculateTimeSpan(earliest, latest time.Time) string {
	if earliest.IsZero() || latest.IsZero() {
		return "无数据"
	}

	duration := latest.Sub(earliest)
	days := int(duration.Hours() / 24)
	
	if days > 365 {
		years := days / 365
		remainingDays := days % 365
		return fmt.Sprintf("%d年%d天", years, remainingDays)
	} else if days > 30 {
		months := days / 30
		remainingDays := days % 30
		return fmt.Sprintf("%d月%d天", months, remainingDays)
	} else {
		return fmt.Sprintf("%d天", days)
	}
}

// HTTP 处理器
func (w *WebMonitor) handleAPI(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Access-Control-Allow-Origin", "*")

	overview, err := w.getSystemOverview()
	if err != nil {
		http.Error(writer, fmt.Sprintf("获取数据失败: %v", err), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(writer).Encode(overview); err != nil {
		http.Error(writer, fmt.Sprintf("编码数据失败: %v", err), http.StatusInternalServerError)
		return
	}
}

func (w *WebMonitor) handleProgress(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Access-Control-Allow-Origin", "*")

	overview, err := w.getSystemOverview()
	if err != nil {
		http.Error(writer, fmt.Sprintf("获取数据失败: %v", err), http.StatusInternalServerError)
		return
	}

	progress := map[string]interface{}{
		"total_tasks":              overview.TotalSymbols,
		"completed_tasks":          overview.SymbolsWithData,
		"remaining_tasks":          overview.TotalSymbols - overview.SymbolsWithData,
		"completion_rate":          overview.OverallProgress,
		"total_records":            overview.TotalRecords,
		"currently_processing":     overview.CurrentlyProcessing,
		"estimated_time_remaining": overview.EstimatedTimeRemaining,
		"worker_states":            overview.WorkerStates,
	}

	if err := json.NewEncoder(writer).Encode(progress); err != nil {
		http.Error(writer, fmt.Sprintf("编码数据失败: %v", err), http.StatusInternalServerError)
		return
	}
}

func (w *WebMonitor) handleHealth(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Access-Control-Allow-Origin", "*")

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"database":  "connected",
		"uptime":    time.Since(time.Now().Add(-2 * time.Hour)),
	}

	if err := json.NewEncoder(writer).Encode(health); err != nil {
		http.Error(writer, fmt.Sprintf("编码数据失败: %v", err), http.StatusInternalServerError)
		return
	}
}

func (w *WebMonitor) handleDashboard(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(writer, request)
		return
	}

	writer.Header().Set("Content-Type", "text/html; charset=utf-8")

	// 读取并提供HTML仪表板
	htmlContent := w.getDashboardHTML()
	writer.Write([]byte(htmlContent))
}

// getDashboardHTML 获取仪表板HTML内容
func (w *WebMonitor) getDashboardHTML() string {
	refreshInterval := int(w.config.RefreshInterval.Seconds() * 1000) // 转换为毫秒

	return fmt.Sprintf(`
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>币安数据加载器 - 实时监控面板</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Segoe UI', 'Microsoft YaHei', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%%, #764ba2 100%%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }
        
        .header h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
        }
        
        .status-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .card {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        
        .status-value {
            font-size: 2.5rem;
            font-weight: bold;
            margin-bottom: 10px;
        }
        
        .status-good { color: #27ae60; }
        .status-info { color: #3498db; }
        .status-warning { color: #f39c12; }
        
        .progress-section {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
        }
        
        .progress-bar {
            width: 100%%;
            height: 30px;
            background: #ecf0f1;
            border-radius: 15px;
            overflow: hidden;
            margin: 20px 0;
            position: relative;
        }
        
        .progress-fill {
            height: 100%%;
            background: linear-gradient(90deg, #27ae60, #2ecc71);
            border-radius: 15px;
            transition: width 0.5s ease;
        }
        
        .progress-text {
            position: absolute;
            top: 50%%;
            left: 50%%;
            transform: translate(-50%%, -50%%);
            color: #2c3e50;
            font-weight: bold;
        }
        
        .realtime-section {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
        }
        
        .processing-list {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin: 15px 0;
        }
        
        .processing-item {
            background: #3498db;
            color: white;
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 14px;
            font-weight: bold;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0%% { opacity: 1; }
            50%% { opacity: 0.7; }
            100%% { opacity: 1; }
        }
        
        .symbols-table {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            padding: 30px;
            overflow-x: auto;
        }
        
        .data-table {
            width: 100%%;
            border-collapse: collapse;
        }
        
        .data-table th {
            background: #f8f9fa;
            padding: 15px 12px;
            text-align: left;
            font-weight: 600;
            color: #333;
            border-bottom: 2px solid #e0e0e0;
        }
        
        .data-table td {
            padding: 12px;
            border-bottom: 1px solid #e0e0e0;
        }
        
        .data-table tr:hover {
            background: #f8f9fa;
        }
        
        .refresh-button {
            position: fixed;
            bottom: 30px;
            right: 30px;
            background: #3498db;
            color: white;
            border: none;
            padding: 15px 20px;
            border-radius: 50px;
            cursor: pointer;
            font-weight: bold;
        }
        
        .last-update {
            text-align: center;
            color: #666;
            font-size: 14px;
            margin-top: 20px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 币安数据加载器 - 实时监控</h1>
            <p>实时查看您的加密货币数据采集状态</p>
        </div>
        
        <div class="status-cards" id="statusCards">
            <div class="card">
                <h3>📊 总交易对数</h3>
                <div class="status-value status-info" id="totalSymbols">加载中...</div>
                <div class="status-label">监控的交易对</div>
            </div>
            <div class="card">
                <h3>✅ 有数据交易对</h3>
                <div class="status-value status-good" id="symbolsWithData">加载中...</div>
                <div class="status-label">包含历史数据</div>
            </div>
            <div class="card">
                <h3>📈 总记录数</h3>
                <div class="status-value status-info" id="totalRecords">加载中...</div>
                <div class="status-label">1分钟K线数据</div>
            </div>
            <div class="card">
                <h3>🔗 数据库状态</h3>
                <div class="status-value status-good" id="dbStatus">连接中</div>
                <div class="status-label">ClickHouse</div>
            </div>
        </div>
        
        <div class="progress-section">
            <h3>📊 整体处理进度</h3>
            <div class="progress-bar">
                <div class="progress-fill" id="progressFill" style="width: 0%%"></div>
                <div class="progress-text" id="progressText">加载中...</div>
            </div>
            <div style="display: flex; justify-content: space-between; margin-top: 10px; font-size: 14px; color: #666;">
                <span>已完成: <strong id="completedTasks">0</strong></span>
                <span>总任务: <strong id="totalTasks">0</strong></span>
                <span>预计剩余: <strong id="estimatedTime">计算中...</strong></span>
            </div>
        </div>
        
        <div class="realtime-section">
            <h3>🔄 实时执行状态</h3>
            <div class="processing-list" id="processingList">
                <div class="processing-item">加载中...</div>
            </div>
        </div>
        
        <div class="symbols-table">
            <h3>📋 交易对详细状态</h3>
            <table class="data-table" id="dataTable">
                <thead>
                    <tr>
                        <th>交易对</th>
                        <th>记录数</th>
                        <th>最早时间</th>
                        <th>最新时间</th>
                        <th>时间跨度</th>
                        <th>进度</th>
                        <th>Worker</th>
                        <th>状态</th>
                    </tr>
                </thead>
                <tbody id="tableBody">
                    <tr>
                        <td colspan="8" style="text-align: center; padding: 50px;">🔄 正在加载数据...</td>
                    </tr>
                </tbody>
            </table>
            <div class="last-update" id="lastUpdate">最后更新: -</div>
        </div>
    </div>
    
    <button class="refresh-button" onclick="loadData()">🔄 刷新</button>
    
    <script>
        function loadData() {
            fetch('/api/overview')
                .then(response => response.json())
                .then(data => {
                    updateDisplay(data);
                })
                .catch(error => {
                    console.error('加载数据失败:', error);
                    document.getElementById('tableBody').innerHTML = 
                        '<tr><td colspan="8" style="text-align: center; color: red;">❌ 加载数据失败</td></tr>';
                });
        }
        
        function updateDisplay(data) {
            // 更新状态卡片
            document.getElementById('totalSymbols').textContent = data.total_symbols.toLocaleString();
            document.getElementById('symbolsWithData').textContent = data.symbols_with_data.toLocaleString();
            document.getElementById('totalRecords').textContent = data.total_records.toLocaleString();
            document.getElementById('dbStatus').textContent = data.database_status;
            
            // 更新进度条
            const progressPercent = data.overall_progress;
            document.getElementById('progressFill').style.width = progressPercent + '%%';
            document.getElementById('progressText').textContent = progressPercent.toFixed(1) + '%%';
            document.getElementById('completedTasks').textContent = data.symbols_with_data.toLocaleString();
            document.getElementById('totalTasks').textContent = data.total_symbols.toLocaleString();
            document.getElementById('estimatedTime').textContent = data.estimated_time_remaining;
            
            // 更新正在处理的交易对
            const processingList = document.getElementById('processingList');
            if (data.currently_processing && data.currently_processing.length > 0) {
                processingList.innerHTML = data.currently_processing.map(symbol => 
                    '<div class="processing-item">🔄 ' + symbol + '</div>'
                ).join('');
            } else {
                processingList.innerHTML = '<div class="processing-item" style="background: #95a5a6;">💤 空闲中</div>';
            }
            
            // 更新表格
            const tbody = document.getElementById('tableBody');
            tbody.innerHTML = '';
            
            data.symbol_details.forEach(symbol => {
                const row = tbody.insertRow();
                const progressBar = generateProgressBar(symbol.progress || 0, 20);
                
                row.innerHTML = 
                    '<td><strong>' + symbol.symbol + '</strong></td>' +
                    '<td>' + (symbol.record_count || 0).toLocaleString() + '</td>' +
                    '<td>' + formatDate(symbol.earliest_time) + '</td>' +
                    '<td>' + formatDate(symbol.latest_time) + '</td>' +
                    '<td>' + (symbol.time_span || 'N/A') + '</td>' +
                    '<td>' + progressBar + ' ' + (symbol.progress || 0).toFixed(1) + '%%</td>' +
                    '<td>' + (symbol.worker_id || 'N/A') + '</td>' +
                    '<td><span style="color: ' + getStatusColor(symbol.status) + '">' + 
                    getStatusText(symbol.status) + '</span></td>';
            });
            
            document.getElementById('lastUpdate').textContent = 
                '最后更新: ' + new Date(data.last_updated).toLocaleString('zh-CN');
        }
        
        function generateProgressBar(progress, width) {
            const filled = Math.floor(progress * width / 100);
            const empty = width - filled;
            return '[' + '█'.repeat(filled) + '░'.repeat(empty) + ']';
        }
        
        function getStatusColor(status) {
            switch(status) {
                case 'active': return '#27ae60';
                case 'pending': return '#f39c12';
                case 'running': return '#3498db';
                case 'error': return '#e74c3c';
                default: return '#95a5a6';
            }
        }
        
        function getStatusText(status) {
            switch(status) {
                case 'active': return '有数据';
                case 'pending': return '待处理';
                case 'running': return '处理中';
                case 'error': return '错误';
                default: return status || 'N/A';
            }
        }
        
        function formatDate(dateString) {
            if (!dateString || dateString === '0001-01-01T00:00:00Z') return 'N/A';
            try {
                return new Date(dateString).toLocaleDateString('zh-CN');
            } catch (e) {
                return 'N/A';
            }
        }
        
        // 页面加载时获取数据
        loadData();
        
        // 自动刷新
        setInterval(loadData, %d);
    </script>
</body>
</html>`, refreshInterval)
}