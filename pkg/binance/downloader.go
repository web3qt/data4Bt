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

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"

	"github.com/rs/zerolog"
)

// BinanceDownloader 币安数据下载器
type BinanceDownloader struct {
	client     *http.Client
	baseURL    string
	dataPath   string
	filter     string
	interval   string
	userAgent  string
	retryCount int
	retryDelay time.Duration
	logger     zerolog.Logger
}

// NewBinanceDownloader 创建新的币安下载器
func NewBinanceDownloader(cfg config.BinanceConfig, downloaderCfg config.DownloaderConfig) *BinanceDownloader {
	client := &http.Client{
		Timeout: cfg.Timeout,
	}

	return &BinanceDownloader{
		client:     client,
		baseURL:    cfg.BaseURL,
		dataPath:   cfg.DataPath,
		filter:     cfg.SymbolsFilter,
		interval:   cfg.Interval,
		userAgent:  downloaderCfg.UserAgent,
		retryCount: cfg.RetryCount,
		retryDelay: cfg.RetryDelay,
		logger:     logger.GetLogger("binance_downloader"),
	}
}

// Fetch 下载并解压数据
func (d *BinanceDownloader) Fetch(ctx context.Context, task domain.DownloadTask) ([]byte, error) {
	// 如果任务中没有URL，则构建URL
	url := task.URL
	if url == "" {
		url = d.BuildDownloadURL(task.Symbol, task.Date)
	}

	start := time.Now()
	defer func() {
		logger.LogPerformance("binance_downloader", "fetch", time.Since(start), map[string]interface{}{
			"symbol": task.Symbol,
			"date":   task.Date.Format("2006-01-02"),
			"url":    url,
		})
	}()

	var lastErr error
	for attempt := 0; attempt <= d.retryCount; attempt++ {
		if attempt > 0 {
			d.logger.Warn().
				Str("symbol", task.Symbol).
				Str("url", url).
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

		data, err := d.downloadAndExtract(ctx, url)
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
	// 使用新的方法获取所有符号，然后过滤
	allSymbols, err := d.GetAllSymbolsFromBinance(ctx)
	if err != nil {
		return nil, err
	}

	// 过滤交易对
	filteredSymbols := d.filterSymbols(allSymbols)

	d.logger.Info().
		Int("total_symbols", len(allSymbols)).
		Int("filtered_symbols", len(filteredSymbols)).
		Str("filter", d.filter).
		Msg("USDT symbols fetched and filtered")

	return filteredSymbols, nil
}

// GetAllSymbolsFromBinance 从币安数据页面获取所有USDT交易对
func (d *BinanceDownloader) GetAllSymbolsFromBinance(ctx context.Context) ([]string, error) {
	start := time.Now()
	defer func() {
		logger.LogPerformance("binance_downloader", "get_all_symbols_from_binance", time.Since(start))
	}()

	d.logger.Info().Msg("Fetching all USDT symbols from Binance monthly data directory")

	// 从Binance月度数据目录获取USDT结尾的代币列表
	url := fmt.Sprintf("%s/?prefix=data/spot/monthly/klines/", d.baseURL)

	d.logger.Debug().
		Str("url", url).
		Msg("Requesting symbols from Binance")

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", d.userAgent)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch symbols page: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		d.logger.Error().
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Msg("Failed to fetch symbols page")
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// 从HTML中提取USDT结尾的交易对
	allSymbols := d.extractUSDTSymbolsFromHTML(string(body))

	if len(allSymbols) == 0 {
		d.logger.Warn().Msg("No USDT symbols found, using fallback list")
	}

	d.logger.Info().
		Int("total_usdt_symbols", len(allSymbols)).
		Msg("All USDT symbols fetched from Binance")

	return allSymbols, nil
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

// extractUSDTSymbolsFromHTML 从HTML中提取USDT结尾的交易对
func (d *BinanceDownloader) extractUSDTSymbolsFromHTML(html string) []string {
	// 使用正则表达式匹配目录链接，只提取USDT结尾的
	pattern := `<a[^>]*href="([^"]*/)"[^>]*>([^<]+USDT)/</a>`
	re := regexp.MustCompile(pattern)
	matches := re.FindAllStringSubmatch(html, -1)

	var symbols []string
	for _, match := range matches {
		if len(match) >= 3 {
			symbol := strings.TrimSuffix(match[2], "/")
			if symbol != "" && strings.HasSuffix(symbol, "USDT") {
				symbols = append(symbols, symbol)
			}
		}
	}

	d.logger.Debug().
		Int("usdt_symbols_found", len(symbols)).
		Msg("Extracted USDT symbols from HTML")

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
	// 按月构建URL: SYMBOL-1m-YYYY-MM.zip
	dateStr := date.Format("2006-01")
	filename := fmt.Sprintf("%s-%s-%s.zip", symbol, d.interval, dateStr)
	return fmt.Sprintf("%s/data/spot/monthly/klines/%s/%s/%s", d.baseURL, symbol, d.interval, filename)
}

// GetAvailableDates 获取指定交易对的可用月份
func (d *BinanceDownloader) GetAvailableDates(ctx context.Context, symbol string) ([]time.Time, error) {
	// 使用更高效的方法：先验证交易对存在，然后使用二分查找找到开始时间
	now := time.Now()
	currentDate := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	// 首先检查上个月的数据是否存在，确认交易对有效（当前月份可能还未完整）
	lastMonth := currentDate.AddDate(0, -1, 0)
	recentURL := d.BuildDownloadURL(symbol, lastMonth)
	if err := d.ValidateURL(ctx, recentURL); err != nil {
		// 尝试检查更早的月份
		twoMonthsAgo := currentDate.AddDate(0, -2, 0)
		olderURL := d.BuildDownloadURL(symbol, twoMonthsAgo)
		if err2 := d.ValidateURL(ctx, olderURL); err2 != nil {
			// 如果连续两个月的数据都不存在，可能交易对无效或已下线
			d.logger.Warn().
				Str("symbol", symbol).
				Err(err).
				Msg("Recent data not available for symbol")
			return nil, fmt.Errorf("symbol %s appears to be invalid or delisted", symbol)
		}
	}

	// 使用二分查找找到数据开始的时间
	minDate := time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC) // 币安历史数据开始时间
	maxDate := currentDate

	// 二分查找最早可用的数据
	var earliestDate time.Time
	for minDate.Before(maxDate) || minDate.Equal(maxDate) {
		// 计算月份差值
		minMonths := minDate.Year()*12 + int(minDate.Month())
		maxMonths := maxDate.Year()*12 + int(maxDate.Month())
		midMonths := (minMonths + maxMonths) / 2

		// 转换回日期
		midYear := midMonths / 12
		midMonth := time.Month(midMonths % 12)
		if midMonth == 0 {
			midYear--
			midMonth = 12
		}
		midDate := time.Date(midYear, midMonth, 1, 0, 0, 0, 0, time.UTC)

		// 如果中间日期和最小日期相同，说明已经找到边界
		if midDate.Equal(minDate) {
			break
		}

		midURL := d.BuildDownloadURL(symbol, midDate)
		if err := d.ValidateURL(ctx, midURL); err == nil {
			// 数据存在，尝试更早的时间
			earliestDate = midDate
			maxDate = midDate.AddDate(0, -1, 0)
		} else {
			// 数据不存在，尝试更晚的时间
			minDate = midDate.AddDate(0, 1, 0)
		}

		// 添加小延迟
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}

	// 如果没找到最早日期，从最小日期开始
	if earliestDate.IsZero() {
		earliestDate = minDate
	}

	// 生成从最早日期到当前日期的所有月份
	var availableDates []time.Time
	for checkDate := earliestDate; !checkDate.After(currentDate); checkDate = checkDate.AddDate(0, 1, 0) {
		availableDates = append(availableDates, checkDate)
	}

	d.logger.Debug().
		Str("symbol", symbol).
		Int("available_months", len(availableDates)).
		Time("earliest_date", earliestDate).
		Time("latest_date", currentDate).
		Msg("Found available dates for symbol")

	return availableDates, nil
}

// GetSymbolTimeline 获取指定交易对的完整时间线信息
func (d *BinanceDownloader) GetSymbolTimeline(ctx context.Context, symbol string) (*domain.SymbolTimeline, error) {
	start := time.Now()
	defer func() {
		logger.LogPerformance("binance_downloader", "get_symbol_timeline", time.Since(start))
	}()

	d.logger.Info().
		Str("symbol", symbol).
		Msg("Fetching complete timeline for symbol from Binance")

	// 使用现有的GetAvailableDates方法来获取可用日期
	availableDates, err := d.GetAvailableDates(ctx, symbol)
	if err != nil {
		d.logger.Warn().
			Str("symbol", symbol).
			Err(err).
			Msg("Failed to get available dates for symbol")
		return nil, fmt.Errorf("failed to get available dates for symbol %s: %w", symbol, err)
	}

	if len(availableDates) == 0 {
		d.logger.Warn().
			Str("symbol", symbol).
			Msg("No monthly data found for symbol")
		return nil, fmt.Errorf("no monthly data found for symbol %s", symbol)
	}

	// 将日期转换为月份字符串
	var availableMonths []string
	seenMonths := make(map[string]bool)

	for _, date := range availableDates {
		month := date.Format("2006-01")
		if !seenMonths[month] {
			availableMonths = append(availableMonths, month)
			seenMonths[month] = true
		}
	}

	// 按时间顺序排序
	for i := 0; i < len(availableMonths)-1; i++ {
		for j := i + 1; j < len(availableMonths); j++ {
			if availableMonths[i] > availableMonths[j] {
				availableMonths[i], availableMonths[j] = availableMonths[j], availableMonths[i]
			}
		}
	}

	// 计算时间线信息
	timeline := &domain.SymbolTimeline{
		Symbol:          symbol,
		AvailableMonths: availableMonths,
		TotalMonths:     len(availableMonths),
		Status:          "discovering",
		LastUpdated:     time.Now(),
	}

	// 设置历史开始时间和最新可用时间
	if len(availableMonths) > 0 {
		if startDate, err := time.Parse("2006-01", availableMonths[0]); err == nil {
			timeline.HistoricalStartDate = startDate
		}
		if endDate, err := time.Parse("2006-01", availableMonths[len(availableMonths)-1]); err == nil {
			timeline.LatestAvailableDate = endDate
		}
	}

	d.logger.Info().
		Str("symbol", symbol).
		Int("total_months", timeline.TotalMonths).
		Str("start_date", timeline.HistoricalStartDate.Format("2006-01")).
		Str("end_date", timeline.LatestAvailableDate.Format("2006-01")).
		Msg("Symbol timeline fetched successfully")

	return timeline, nil
}

// extractDatesFromHTML 从HTML页面提取月份
func (d *BinanceDownloader) extractDatesFromHTML(html, symbol string) []time.Time {
	// 使用新的方法获取月份字符串，然后转换为time.Time
	monthStrings := d.extractMonthsFromHTML(html, symbol)

	var dates []time.Time
	for _, monthStr := range monthStrings {
		if date, err := time.Parse("2006-01", monthStr); err == nil {
			dates = append(dates, date)
		}
	}

	return dates
}

// extractMonthsFromHTML 从HTML页面提取月份字符串
func (d *BinanceDownloader) extractMonthsFromHTML(html, symbol string) []string {
	// 匹配文件名格式: SYMBOL-1m-YYYY-MM.zip
	pattern := fmt.Sprintf(`%s-%s-(\d{4}-\d{2})\.zip`, symbol, d.interval)
	re := regexp.MustCompile(pattern)
	matches := re.FindAllStringSubmatch(html, -1)

	var months []string
	seenMonths := make(map[string]bool) // 去重

	for _, match := range matches {
		if len(match) > 1 {
			month := match[1]
			if !seenMonths[month] {
				months = append(months, month)
				seenMonths[month] = true
			}
		}
	}

	// 按时间顺序排序
	for i := 0; i < len(months)-1; i++ {
		for j := i + 1; j < len(months); j++ {
			if months[i] > months[j] {
				months[i], months[j] = months[j], months[i]
			}
		}
	}

	d.logger.Debug().
		Str("symbol", symbol).
		Int("months_found", len(months)).
		Msg("Extracted months from HTML")

	return months
}

// extractMonthsFromS3XML 从S3 XML响应中提取月份信息
func (d *BinanceDownloader) extractMonthsFromS3XML(xmlContent, symbol string) []string {
	// 匹配S3 XML中的Key元素，格式: <Key>data/spot/monthly/klines/SYMBOL/1m/SYMBOL-1m-YYYY-MM.zip</Key>
	pattern := fmt.Sprintf(`<Key>data/spot/monthly/klines/%s/%s/%s-%s-(\d{4}-\d{2})\.zip</Key>`, symbol, d.interval, symbol, d.interval)
	re := regexp.MustCompile(pattern)
	matches := re.FindAllStringSubmatch(xmlContent, -1)

	var months []string
	seenMonths := make(map[string]bool) // 去重

	for _, match := range matches {
		if len(match) > 1 {
			month := match[1]
			if !seenMonths[month] {
				months = append(months, month)
				seenMonths[month] = true
			}
		}
	}

	// 按时间顺序排序
	for i := 0; i < len(months)-1; i++ {
		for j := i + 1; j < len(months); j++ {
			if months[i] > months[j] {
				months[i], months[j] = months[j], months[i]
			}
		}
	}

	d.logger.Debug().
		Str("symbol", symbol).
		Int("months_found", len(months)).
		Msg("Extracted months from S3 XML")

	return months
}
