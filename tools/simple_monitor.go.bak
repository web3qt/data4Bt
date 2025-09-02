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

type SymbolInfo struct {
	Symbol       string    `json:"symbol"`
	RecordCount  int64     `json:"record_count"`
	EarliestTime time.Time `json:"earliest_time"`
	LatestTime   time.Time `json:"latest_time"`
	TimeSpan     string    `json:"time_span"`
	Status       string    `json:"status"`
}

type MonitorData struct {
	TotalSymbols    int          `json:"total_symbols"`
	SymbolsWithData int          `json:"symbols_with_data"`
	TotalRecords    int64        `json:"total_records"`
	DatabaseStatus  string       `json:"database_status"`
	LastUpdated     time.Time    `json:"last_updated"`
	SymbolDetails   []SymbolInfo `json:"symbol_details"`
}

type SimpleMonitor struct {
	db *sql.DB
}

func NewSimpleMonitor() (*SimpleMonitor, error) {
	db, err := sql.Open("clickhouse", "clickhouse://default:123456@localhost:9000/data4BT")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &SimpleMonitor{db: db}, nil
}

func (m *SimpleMonitor) getData() (*MonitorData, error) {
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

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query database: %w", err)
	}
	defer rows.Close()

	var symbols []SymbolInfo
	totalRecords := int64(0)
	symbolsWithData := 0

	for rows.Next() {
		var symbol SymbolInfo
		var earliest, latest time.Time

		err := rows.Scan(&symbol.Symbol, &symbol.RecordCount, &earliest, &latest)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		symbol.EarliestTime = earliest
		symbol.LatestTime = latest
		symbol.TimeSpan = formatTimeSpan(earliest, latest)
		symbol.Status = "active"

		symbols = append(symbols, symbol)
		totalRecords += symbol.RecordCount
		symbolsWithData++
	}

	// 从状态文件获取额外信息
	m.addStateInfo(&symbols)

	result := &MonitorData{
		TotalSymbols:    len(symbols),
		SymbolsWithData: symbolsWithData,
		TotalRecords:    totalRecords,
		DatabaseStatus:  "connected",
		LastUpdated:     time.Now(),
		SymbolDetails:   symbols,
	}

	return result, nil
}

func (m *SimpleMonitor) addStateInfo(symbols *[]SymbolInfo) {
	file, err := os.Open("state/progress.json")
	if err != nil {
		return // 如果没有状态文件就跳过
	}
	defer file.Close()

	var stateData map[string]interface{}
	if err := json.NewDecoder(file).Decode(&stateData); err != nil {
		return
	}

	symbolMap := make(map[string]*SymbolInfo)
	for i := range *symbols {
		symbolMap[(*symbols)[i].Symbol] = &(*symbols)[i]
	}

	// 添加状态文件中的信息
	for symbol, _ := range stateData {
		if _, exists := symbolMap[symbol]; !exists {
			// 添加仅在状态文件中存在的交易对
			newSymbol := SymbolInfo{
				Symbol:      symbol,
				RecordCount: 0,
				Status:      "pending",
				TimeSpan:    "无数据",
			}
			*symbols = append(*symbols, newSymbol)
		}
	}

	// 重新排序
	sort.Slice(*symbols, func(i, j int) bool {
		return (*symbols)[i].Symbol < (*symbols)[j].Symbol
	})
}

func formatTimeSpan(earliest, latest time.Time) string {
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

func (m *SimpleMonitor) handleAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	result, err := m.getData()
	if err != nil {
		http.Error(w, fmt.Sprintf("获取数据失败: %v", err), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(result)
}

func (m *SimpleMonitor) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	
	html := `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>币安数据加载器 - 监控面板</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .header { text-align: center; margin-bottom: 30px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center; }
        .stat-value { font-size: 2em; font-weight: bold; color: #2c3e50; }
        .stat-label { color: #7f8c8d; margin-top: 5px; }
        .table-container { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background: #f8f9fa; font-weight: bold; }
        tr:hover { background: #f8f9fa; }
        .refresh-btn { position: fixed; bottom: 20px; right: 20px; background: #3498db; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; }
        .status-active { color: #27ae60; font-weight: bold; }
        .status-pending { color: #e74c3c; font-weight: bold; }
        .loading { text-align: center; padding: 50px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🚀 币安数据加载器监控面板</h1>
        <p>实时监控数据采集状态</p>
    </div>
    
    <div class="stats" id="stats">
        <div class="stat-card">
            <div class="stat-value" id="totalSymbols">-</div>
            <div class="stat-label">总交易对数</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="symbolsWithData">-</div>
            <div class="stat-label">有数据交易对</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="totalRecords">-</div>
            <div class="stat-label">总记录数</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="dbStatus">-</div>
            <div class="stat-label">数据库状态</div>
        </div>
    </div>
    
    <div class="table-container">
        <h2>交易对详细信息</h2>
        <div id="loading" class="loading">🔄 正在加载数据...</div>
        <table id="dataTable" style="display:none;">
            <thead>
                <tr>
                    <th>交易对</th>
                    <th>记录数</th>
                    <th>最早时间</th>
                    <th>最新时间</th>
                    <th>时间跨度</th>
                    <th>状态</th>
                </tr>
            </thead>
            <tbody id="tableBody">
            </tbody>
        </table>
        <p style="text-align: center; margin-top: 20px; color: #7f8c8d;">
            最后更新: <span id="lastUpdate">-</span>
        </p>
    </div>
    
    <button class="refresh-btn" onclick="loadData()">🔄 刷新</button>
    
    <script>
        function loadData() {
            fetch('/api/data')
                .then(response => response.json())
                .then(data => {
                    updateDisplay(data);
                    document.getElementById('loading').style.display = 'none';
                    document.getElementById('dataTable').style.display = 'table';
                })
                .catch(error => {
                    console.error('Error:', error);
                    document.getElementById('loading').innerHTML = '❌ 加载失败: ' + error.message;
                });
        }
        
        function updateDisplay(data) {
            document.getElementById('totalSymbols').textContent = data.total_symbols.toLocaleString();
            document.getElementById('symbolsWithData').textContent = data.symbols_with_data.toLocaleString();
            document.getElementById('totalRecords').textContent = data.total_records.toLocaleString();
            document.getElementById('dbStatus').textContent = data.database_status;
            
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
                    '<td><span class="status-' + (symbol.record_count > 0 ? 'active' : 'pending') + '">' + 
                    (symbol.record_count > 0 ? '有数据' : '待处理') + '</span></td>';
            });
            
            document.getElementById('lastUpdate').textContent = 
                new Date(data.last_updated).toLocaleString('zh-CN');
        }
        
        function formatDate(dateString) {
            if (!dateString || dateString === '0001-01-01T00:00:00Z') return 'N/A';
            try {
                return new Date(dateString).toLocaleDateString('zh-CN');
            } catch (e) {
                return 'N/A';
            }
        }
        
        loadData();
        setInterval(loadData, 30000);
    </script>
</body>
</html>`

	w.Write([]byte(html))
}

func main() {
	port := 8888
	if len(os.Args) > 1 {
		if p, err := strconv.Atoi(os.Args[1]); err == nil {
			port = p
		}
	}

	monitor, err := NewSimpleMonitor()
	if err != nil {
		log.Fatal("启动监控器失败:", err)
	}
	defer monitor.db.Close()

	http.HandleFunc("/", monitor.handleDashboard)
	http.HandleFunc("/api/data", monitor.handleAPI)

	fmt.Printf("🌐 Web监控面板启动成功!\n")
	fmt.Printf("📊 访问地址: http://localhost:%d\n", port)
	fmt.Printf("📡 API接口: http://localhost:%d/api/data\n", port)
	fmt.Printf("💡 数据每30秒自动刷新\n\n")
	fmt.Printf("按 Ctrl+C 停止服务...\n")

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}