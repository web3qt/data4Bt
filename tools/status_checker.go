package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type SymbolStatus struct {
	Symbol        string    `json:"symbol"`
	RecordCount   int64     `json:"record_count"`
	EarliestTime  time.Time `json:"earliest_time"`
	LatestTime    time.Time `json:"latest_time"`
	TimeSpan      string    `json:"time_span"`
	LastProcessed string    `json:"last_processed_from_state"`
	ProcessedDays int       `json:"processed_days"`
	Status        string    `json:"status"`
}

type SystemStatus struct {
	TotalSymbols     int            `json:"total_symbols"`
	SymbolsWithData  int            `json:"symbols_with_data"`
	TotalRecords     int64          `json:"total_records"`
	DatabaseStatus   string         `json:"database_status"`
	LastUpdated      time.Time      `json:"last_updated"`
	SymbolDetails    []SymbolStatus `json:"symbol_details"`
}

func main() {
	// 连接ClickHouse
	conn, err := sql.Open("clickhouse", "clickhouse://default:123456@localhost:9000/data4BT")
	if err != nil {
		log.Fatal("Failed to connect to ClickHouse:", err)
	}
	defer conn.Close()

	// 测试连接
	if err := conn.Ping(); err != nil {
		log.Fatal("Failed to ping ClickHouse:", err)
	}

	// 从数据库获取实际数据状态
	dbStatus, err := getDataBaseStatus(conn)
	if err != nil {
		log.Fatal("Failed to get database status:", err)
	}

	// 从状态文件获取进度信息
	stateStatus, err := getStateFileStatus()
	if err != nil {
		log.Printf("Warning: Failed to get state file status: %v", err)
	}

	// 合并信息
	combinedStatus := combineStatus(dbStatus, stateStatus)

	// 输出结果
	if len(os.Args) > 1 && os.Args[1] == "--json" {
		outputJSON(combinedStatus)
	} else {
		outputTable(combinedStatus)
	}
}

func getDataBaseStatus(conn *sql.DB) ([]SymbolStatus, error) {
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

	rows, err := conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query database: %w", err)
	}
	defer rows.Close()

	var statuses []SymbolStatus
	for rows.Next() {
		var status SymbolStatus
		var earliest, latest time.Time

		err := rows.Scan(&status.Symbol, &status.RecordCount, &earliest, &latest)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		status.EarliestTime = earliest
		status.LatestTime = latest
		status.TimeSpan = calculateTimeSpan(earliest, latest)
		status.ProcessedDays = int(latest.Sub(earliest).Hours() / 24)
		status.Status = "active"

		statuses = append(statuses, status)
	}

	return statuses, nil
}

func getStateFileStatus() (map[string]interface{}, error) {
	file, err := os.Open("state/progress.json")
	if err != nil {
		return nil, fmt.Errorf("failed to open progress file: %w", err)
	}
	defer file.Close()

	var stateData map[string]interface{}
	if err := json.NewDecoder(file).Decode(&stateData); err != nil {
		return nil, fmt.Errorf("failed to decode progress file: %w", err)
	}

	return stateData, nil
}

func combineStatus(dbStatus []SymbolStatus, stateData map[string]interface{}) SystemStatus {
	symbolMap := make(map[string]*SymbolStatus)
	
	// 将数据库状态放入map
	for i := range dbStatus {
		symbolMap[dbStatus[i].Symbol] = &dbStatus[i]
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
				status := SymbolStatus{
					Symbol:        symbol,
					RecordCount:   0,
					Status:        "pending",
				}
				
				if lastDate, ok := symbolData["last_date"].(string); ok {
					status.LastProcessed = lastDate
				}
				if processed, ok := symbolData["processed"].(float64); ok {
					status.ProcessedDays = int(processed)
				}
				
				symbolMap[symbol] = &status
			}
		}
	}

	// 转换为slice并排序
	var symbols []SymbolStatus
	totalRecords := int64(0)
	symbolsWithData := 0

	for _, status := range symbolMap {
		symbols = append(symbols, *status)
		totalRecords += status.RecordCount
		if status.RecordCount > 0 {
			symbolsWithData++
		}
	}

	sort.Slice(symbols, func(i, j int) bool {
		return symbols[i].Symbol < symbols[j].Symbol
	})

	return SystemStatus{
		TotalSymbols:    len(symbols),
		SymbolsWithData: symbolsWithData,
		TotalRecords:    totalRecords,
		DatabaseStatus:  "connected",
		LastUpdated:     time.Now(),
		SymbolDetails:   symbols,
	}
}

func calculateTimeSpan(earliest, latest time.Time) string {
	if earliest.IsZero() || latest.IsZero() {
		return "No data"
	}

	duration := latest.Sub(earliest)
	days := int(duration.Hours() / 24)
	
	if days > 365 {
		years := days / 365
		remainingDays := days % 365
		return fmt.Sprintf("%d years %d days", years, remainingDays)
	} else if days > 30 {
		months := days / 30
		remainingDays := days % 30
		return fmt.Sprintf("%d months %d days", months, remainingDays)
	} else {
		return fmt.Sprintf("%d days", days)
	}
}

func outputJSON(status SystemStatus) {
	output, _ := json.MarshalIndent(status, "", "  ")
	fmt.Println(string(output))
}

func outputTable(status SystemStatus) {
	fmt.Println("=== Binance Data Loader Status ===")
	fmt.Printf("Last Updated: %s\n", status.LastUpdated.Format("2006-01-02 15:04:05"))
	fmt.Printf("Database Status: %s\n", status.DatabaseStatus)
	fmt.Printf("Total Symbols: %d\n", status.TotalSymbols)
	fmt.Printf("Symbols with Data: %d\n", status.SymbolsWithData)
	fmt.Printf("Total Records: %s\n", formatNumber(status.TotalRecords))
	fmt.Println()

	if len(status.SymbolDetails) == 0 {
		fmt.Println("No symbol data found.")
		return
	}

	// 打印表头
	fmt.Printf("%-15s %-12s %-20s %-20s %-15s %-20s %s\n",
		"SYMBOL", "RECORDS", "EARLIEST", "LATEST", "TIME SPAN", "LAST PROCESSED", "STATUS")
	fmt.Println(strings.Repeat("-", 130))

	// 打印数据
	for _, symbol := range status.SymbolDetails {
		earliest := "N/A"
		latest := "N/A"
		
		if !symbol.EarliestTime.IsZero() {
			earliest = symbol.EarliestTime.Format("2006-01-02")
		}
		if !symbol.LatestTime.IsZero() {
			latest = symbol.LatestTime.Format("2006-01-02")
		}

		lastProcessed := "N/A"
		if symbol.LastProcessed != "" {
			if t, err := time.Parse(time.RFC3339, symbol.LastProcessed); err == nil {
				lastProcessed = t.Format("2006-01-02")
			} else {
				lastProcessed = symbol.LastProcessed[:10] // 假设是日期格式
			}
		}

		fmt.Printf("%-15s %-12s %-20s %-20s %-15s %-20s %s\n",
			symbol.Symbol,
			formatNumber(symbol.RecordCount),
			earliest,
			latest,
			symbol.TimeSpan,
			lastProcessed,
			symbol.Status)
	}
}

func formatNumber(num int64) string {
	if num == 0 {
		return "0"
	}
	
	str := fmt.Sprintf("%d", num)
	n := len(str)
	if n <= 3 {
		return str
	}
	
	var result []byte
	for i, digit := range str {
		if i > 0 && (n-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(digit))
	}
	
	return string(result)
}