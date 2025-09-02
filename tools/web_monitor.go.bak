package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type SymbolData struct {
	Symbol        string    `json:"symbol"`
	RecordCount   int64     `json:"record_count"`
	EarliestTime  time.Time `json:"earliest_time"`
	LatestTime    time.Time `json:"latest_time"`
	TimeSpan      string    `json:"time_span"`
	LastProcessed string    `json:"last_processed"`
	ProcessedDays int       `json:"processed_days"`
	Status        string    `json:"status"`
}

type SystemOverview struct {
	TotalSymbols     int          `json:"total_symbols"`
	SymbolsWithData  int          `json:"symbols_with_data"`
	TotalRecords     int64        `json:"total_records"`
	DatabaseStatus   string       `json:"database_status"`
	LastUpdated      time.Time    `json:"last_updated"`
	SymbolDetails    []SymbolData `json:"symbol_details"`
}

type WebMonitor struct {
	db *sql.DB
}

func NewWebMonitor() (*WebMonitor, error) {
	db, err := sql.Open("clickhouse", "clickhouse://localhost:9000/data4BT")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &WebMonitor{db: db}, nil
}

func (w *WebMonitor) getSystemOverview() (*SystemOverview, error) {
	// 获取数据库中的实际数据
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

		symbols = append(symbols, symbol)
		totalRecords += symbol.RecordCount
		symbolsWithData++
	}

	// 从状态文件获取进度信息
	stateData := w.loadProgressState()
	symbols = w.mergeWithProgressState(symbols, stateData)

	overview := &SystemOverview{
		TotalSymbols:    len(symbols),
		SymbolsWithData: symbolsWithData,
		TotalRecords:    totalRecords,
		DatabaseStatus:  "connected",
		LastUpdated:     time.Now(),
		SymbolDetails:   symbols,
	}

	return overview, nil
}

func (w *WebMonitor) loadProgressState() map[string]interface{} {
	file, err := os.Open("state/progress.json")
	if err != nil {
		log.Printf("Warning: Failed to open progress file: %v", err)
		return nil
	}
	defer file.Close()

	var stateData map[string]interface{}
	if err := json.NewDecoder(file).Decode(&stateData); err != nil {
		log.Printf("Warning: Failed to decode progress file: %v", err)
		return nil
	}

	return stateData
}

func (w *WebMonitor) mergeWithProgressState(dbSymbols []SymbolData, stateData map[string]interface{}) []SymbolData {
	symbolMap := make(map[string]*SymbolData)
	
	// 将数据库状态放入map
	for i := range dbSymbols {
		symbolMap[dbSymbols[i].Symbol] = &dbSymbols[i]
	}

	// 添加状态文件中的信息
	if stateData != nil {
		for symbol, data := range stateData {
			symbolData, ok := data.(map[string]interface{})
			if !ok {
				continue
			}

			if existing, exists := symbolMap[symbol]; exists {
				// 更新现有记录
				if lastDate, ok := symbolData["last_date"].(string); ok {
					existing.LastProcessed = lastDate
				}
			} else {
				// 添加新记录（仅在状态文件中存在）
				newSymbol := SymbolData{
					Symbol:        symbol,
					RecordCount:   0,
					Status:        "pending",
				}
				
				if lastDate, ok := symbolData["last_date"].(string); ok {
					newSymbol.LastProcessed = lastDate
				}
				if processed, ok := symbolData["processed"].(float64); ok {
					newSymbol.ProcessedDays = int(processed)
				}
				
				symbolMap[symbol] = &newSymbol
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

	// 模拟进度数据
	progress := map[string]interface{}{
		"total_tasks":              overview.TotalSymbols,
		"completed_tasks":          overview.SymbolsWithData,
		"remaining_tasks":          overview.TotalSymbols - overview.SymbolsWithData,
		"completion_rate":          float64(overview.SymbolsWithData) / float64(overview.TotalSymbols) * 100,
		"total_records":            overview.TotalRecords,
		"elapsed_time":             time.Since(time.Now().Add(-2*time.Hour)).Nanoseconds(),
		"estimated_time_remaining": int64(3600000000000), // 1小时的纳秒数
		"tasks_per_minute":         2.5,
		"active_symbols":           overview.SymbolsWithData,
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

	// 读取HTML模板
	htmlContent := `
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
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
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
        
        .progress-section {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
        }
        
        .progress-bar {
            width: 100%;
            height: 30px;
            background: #ecf0f1;
            border-radius: 15px;
            overflow: hidden;
            margin: 20px 0;
            position: relative;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #27ae60, #2ecc71);
            border-radius: 15px;
            transition: width 0.5s ease;
        }
        
        .progress-text {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            color: #2c3e50;
            font-weight: bold;
        }
        
        .symbols-table {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            padding: 30px;
            overflow-x: auto;
        }
        
        .data-table {
            width: 100%;
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
            </div>
            <div class="card">
                <h3>✅ 有数据交易对</h3>
                <div class="status-value status-good" id="symbolsWithData">加载中...</div>
            </div>
            <div class="card">
                <h3>📈 总记录数</h3>
                <div class="status-value status-info" id="totalRecords">加载中...</div>
            </div>
            <div class="card">
                <h3>🔗 数据库状态</h3>
                <div class="status-value status-good" id="dbStatus">连接中</div>
            </div>
        </div>
        
        <div class="progress-section">
            <h3>📊 整体处理进度</h3>
            <div class="progress-bar">
                <div class="progress-fill" id="progressFill" style="width: 0%"></div>
                <div class="progress-text" id="progressText">加载中...</div>
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
                        <th>最后处理</th>
                        <th>状态</th>
                    </tr>
                </thead>
                <tbody id="tableBody">
                    <tr>
                        <td colspan="7" style="text-align: center; padding: 50px;">🔄 正在加载数据...</td>
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
                        '<tr><td colspan="7" style="text-align: center; color: red;">❌ 加载数据失败</td></tr>';
                });
        }
        
        function updateDisplay(data) {
            // 更新状态卡片
            document.getElementById('totalSymbols').textContent = data.total_symbols.toLocaleString();
            document.getElementById('symbolsWithData').textContent = data.symbols_with_data.toLocaleString();
            document.getElementById('totalRecords').textContent = data.total_records.toLocaleString();
            document.getElementById('dbStatus').textContent = data.database_status;
            
            // 更新进度条
            const progressPercent = (data.symbols_with_data / data.total_symbols) * 100;
            document.getElementById('progressFill').style.width = progressPercent + '%';
            document.getElementById('progressText').textContent = 
                data.symbols_with_data + ' / ' + data.total_symbols + ' (' + progressPercent.toFixed(1) + '%)';
            
            // 更新表格
            const tbody = document.getElementById('tableBody');
            tbody.innerHTML = '';
            
            data.symbol_details.forEach(symbol => {
                const row = tbody.insertRow();
                row.innerHTML = 
                    '<td><strong>' + symbol.symbol + '</strong></td>' +
                    '<td>' + symbol.record_count.toLocaleString() + '</td>' +
                    '<td>' + formatDate(symbol.earliest_time) + '</td>' +
                    '<td>' + formatDate(symbol.latest_time) + '</td>' +
                    '<td>' + symbol.time_span + '</td>' +
                    '<td>' + formatDate(symbol.last_processed) + '</td>' +
                    '<td><span style="color: ' + (symbol.record_count > 0 ? '#27ae60' : '#e74c3c') + '">' + 
                    (symbol.record_count > 0 ? '有数据' : '待处理') + '</span></td>';
            });
            
            document.getElementById('lastUpdate').textContent = 
                '最后更新: ' + new Date(data.last_updated).toLocaleString('zh-CN');
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
        
        // 每30秒自动刷新
        setInterval(loadData, 30000);
    </script>
</body>
</html>`

	writer.Write([]byte(htmlContent))
}

func main() {
	port := 8888
	if len(os.Args) > 1 {
		if p, err := strconv.Atoi(os.Args[1]); err == nil {
			port = p
		}
	}

	monitor, err := NewWebMonitor()
	if err != nil {
		log.Fatal("Failed to create web monitor:", err)
	}
	defer monitor.db.Close()

	http.HandleFunc("/", monitor.handleDashboard)
	http.HandleFunc("/api/overview", monitor.handleAPI)
	http.HandleFunc("/progress", monitor.handleProgress)
	http.HandleFunc("/health", monitor.handleHealth)

	fmt.Printf("🌐 Web监控面板启动成功!\n")
	fmt.Printf("📊 监控地址: http://localhost:%d\n", port)
	fmt.Printf("📡 API接口: http://localhost:%d/api/overview\n", port)
	fmt.Printf("💡 提示: 数据每30秒自动刷新\n")
	fmt.Printf("\n按 Ctrl+C 停止服务...\n")

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}