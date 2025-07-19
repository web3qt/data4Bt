package binance

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
)

// BinanceDownloader 币安数据下载器
type BinanceDownloader struct {
	client    *http.Client
	baseURL   string
	dataPath  string
	filter    string
	interval  string
	userAgent string
	retryCount int
	retryDelay time.Duration
	logger    zerolog.Logger
}

// NewBinanceDownloader 创建新的币安下载器
func NewBinanceDownloader(cfg config.BinanceConfig, downloaderCfg config.DownloaderConfig) *BinanceDownloader {
	client := &http.Client{
		Timeout: cfg.Timeout,
	}
	
	return &BinanceDownloader{
		client:      client,
		baseURL:     cfg.BaseURL,
		dataPath:    cfg.DataPath,
		filter:      cfg.SymbolsFilter,
		interval:    cfg.Interval,
		userAgent:   downloaderCfg.UserAgent,
		retryCount:  cfg.RetryCount,
		retryDelay:  cfg.RetryDelay,
		logger:      logger.GetLogger("binance_downloader"),
	}
}

// Fetch 下载并解压数据
func (d *BinanceDownloader) Fetch(ctx context.Context, task domain.DownloadTask) ([]byte, error) {
	start := time.Now()
	defer func() {
		logger.LogPerformance("binance_downloader", "fetch", time.Since(start), map[string]interface{}{
			"symbol": task.Symbol,
			"date":   task.Date.Format("2006-01-02"),
			"url":    task.URL,
		})
	}()
	
	var lastErr error
	for attempt := 0; attempt <= d.retryCount; attempt++ {
		if attempt > 0 {
			d.logger.Warn().
				Str("symbol", task.Symbol).
				Str("url", task.URL).
				Int("attempt", attempt).
				Err(lastErr).
				Msg("Retrying download")
			
			// 等待重试延迟
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(d.retryDelay):
			}
		}
		
		data, err := d.downloadAndExtract(ctx, task.URL)
		if err == nil {
			d.logger.Debug().
				Str("symbol", task.Symbol).
				Str("date", task.Date.Format("2006-01-02")).
				Int("size", len(data)).
				Msg("Successfully downloaded and extracted data")
			return data, nil
		}
		
		lastErr = err
	}
	
	return nil, fmt.Errorf("failed to download after %d attempts: %w", d.retryCount+1, lastErr)
}

// GetSymbols 获取所有可用的交易对
func (d *BinanceDownloader) GetSymbols(ctx context.Context) ([]string, error) {
	start := time.Now()
	defer func() {
		logger.LogPerformance("binance_downloader", "get_symbols", time.Since(start))
	}()
	
	// 构造API URL来获取交易对列表
	url := d.baseURL + d.dataPath + "/"
	
	d.logger.Info().Str("url", url).Msg("Fetching symbols list")
	
	// 创建HTTP请求
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("User-Agent", d.userAgent)
	
	// 发送请求
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	
	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	
	// 解析HTML页面，提取交易对目录
	symbols := d.extractSymbolsFromHTML(string(body))
	
	// 过滤交易对
	filteredSymbols := d.filterSymbols(symbols)
	
	d.logger.Info().
		Int("total_symbols", len(symbols)).
		Int("filtered_symbols", len(filteredSymbols)).
		Str("filter", d.filter).
		Msg("Symbols fetched and filtered")
	
	return filteredSymbols, nil
}

// ValidateURL 验证下载URL是否有效
func (d *BinanceDownloader) ValidateURL(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("User-Agent", d.userAgent)
	
	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("URL not accessible, status code: %d", resp.StatusCode)
	}
	
	return nil
}

// downloadAndExtract 下载并解压ZIP文件
func (d *BinanceDownloader) downloadAndExtract(ctx context.Context, url string) ([]byte, error) {
	// 创建HTTP请求
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("User-Agent", d.userAgent)
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	
	// 发送请求
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	
	// 读取ZIP文件内容
	zipData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	
	// 解压ZIP文件
	csvData, err := d.extractCSVFromZip(zipData)
	if err != nil {
		return nil, fmt.Errorf("failed to extract CSV from ZIP: %w", err)
	}
	
	return csvData, nil
}

// extractCSVFromZip 从ZIP文件中提取CSV数据
func (d *BinanceDownloader) extractCSVFromZip(zipData []byte) ([]byte, error) {
	reader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return nil, fmt.Errorf("failed to create zip reader: %w", err)
	}
	
	// 查找CSV文件
	for _, file := range reader.File {
		if strings.HasSuffix(strings.ToLower(file.Name), ".csv") {
			// 打开文件
			rc, err := file.Open()
			if err != nil {
				return nil, fmt.Errorf("failed to open file %s: %w", file.Name, err)
			}
			defer rc.Close()
			
			// 读取文件内容
			data, err := io.ReadAll(rc)
			if err != nil {
				return nil, fmt.Errorf("failed to read file %s: %w", file.Name, err)
			}
			
			return data, nil
		}
	}
	
	return nil, fmt.Errorf("no CSV file found in ZIP archive")
}

// extractSymbolsFromHTML 从HTML页面提取交易对
func (d *BinanceDownloader) extractSymbolsFromHTML(html string) []string {
	// 使用正则表达式匹配目录链接
	// 币安数据页面的目录格式通常是: <a href="SYMBOL/">SYMBOL/</a>
	re := regexp.MustCompile(`<a href="([A-Z0-9]+)/">[A-Z0-9]+/</a>`)
	matches := re.FindAllStringSubmatch(html, -1)
	
	var symbols []string
	for _, match := range matches {
		if len(match) > 1 {
			symbols = append(symbols, match[1])
		}
	}
	
	return symbols
}

// filterSymbols 过滤交易对
func (d *BinanceDownloader) filterSymbols(symbols []string) []string {
	if d.filter == "" {
		return symbols
	}
	
	var filtered []string
	for _, symbol := range symbols {
		if strings.HasSuffix(symbol, d.filter) {
			filtered = append(filtered, symbol)
		}
	}
	
	return filtered
}

// BuildDownloadURL 构建下载URL
func (d *BinanceDownloader) BuildDownloadURL(symbol string, date time.Time) string {
	dateStr := date.Format("2006-01-02")
	filename := fmt.Sprintf("%s-%s-%s.zip", symbol, d.interval, dateStr)
	return fmt.Sprintf("%s%s/%s/%s/%s", d.baseURL, d.dataPath, symbol, d.interval, filename)
}

// GetAvailableDates 获取指定交易对的可用日期
func (d *BinanceDownloader) GetAvailableDates(ctx context.Context, symbol string) ([]time.Time, error) {
	url := fmt.Sprintf("%s%s/%s/", d.baseURL, d.dataPath, symbol)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("User-Agent", d.userAgent)
	
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	
	// 解析HTML页面，提取日期
	dates := d.extractDatesFromHTML(string(body), symbol)
	
	return dates, nil
}

// extractDatesFromHTML 从HTML页面提取日期
func (d *BinanceDownloader) extractDatesFromHTML(html, symbol string) []time.Time {
	// 匹配文件名格式: SYMBOL-1m-YYYY-MM-DD.zip
	pattern := fmt.Sprintf(`%s-%s-(\d{4}-\d{2}-\d{2})\.zip`, symbol, d.interval)
	re := regexp.MustCompile(pattern)
	matches := re.FindAllStringSubmatch(html, -1)
	
	var dates []time.Time
	for _, match := range matches {
		if len(match) > 1 {
			if date, err := time.Parse("2006-01-02", match[1]); err == nil {
				dates = append(dates, date)
			}
		}
	}
	
	return dates
}