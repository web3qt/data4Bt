package binance

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	client            *http.Client
	baseURL           string
	listingBaseURL    string
	dataPath          string
	monthlyKlinesPath string
	marketType        string
	exchangeInfoURL   string
	filter            string
	interval          string
	userAgent         string
	retryCount        int
	retryDelay        time.Duration
	logger            zerolog.Logger
}

// NewBinanceDownloader 创建新的币安下载器
func NewBinanceDownloader(cfg config.BinanceConfig, downloaderCfg config.DownloaderConfig) *BinanceDownloader {
	log := logger.GetLogger("binance_downloader")

	marketType := normalizeMarketType(cfg.MarketType)
	monthlyKlinesPath := normalizeMonthlyKlinesPath(cfg.DataPath, marketType)
	exchangeInfoURL := strings.TrimSpace(cfg.ExchangeInfoURL)
	if exchangeInfoURL == "" {
		exchangeInfoURL = defaultExchangeInfoURL(marketType)
	}

	client := &http.Client{
		Timeout: cfg.Timeout,
	}

	// 仅在未显式配置超时时使用默认值，避免大文件下载被错误截断
	if client.Timeout == 0 {
		client.Timeout = 30 * time.Second
		log.Info().Dur("timeout", client.Timeout).Msg("Set HTTP client timeout for better responsiveness")
	}

	// 配置代理
	if cfg.ProxyURL != "" {
		proxyURL, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			log.Error().Err(err).Str("proxy_url", cfg.ProxyURL).Msg("Failed to parse proxy URL")
		} else {
			client.Transport = &http.Transport{
				Proxy: http.ProxyURL(proxyURL),
			}
			log.Info().Str("proxy_url", cfg.ProxyURL).Msg("Using proxy for HTTP requests")
		}
	}

	return &BinanceDownloader{
		client:            client,
		baseURL:           strings.TrimRight(cfg.BaseURL, "/"),
		listingBaseURL:    "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision",
		dataPath:          cfg.DataPath,
		monthlyKlinesPath: monthlyKlinesPath,
		marketType:        marketType,
		exchangeInfoURL:   exchangeInfoURL,
		filter:            cfg.SymbolsFilter,
		interval:          cfg.Interval,
		userAgent:         downloaderCfg.UserAgent,
		retryCount:        cfg.RetryCount,
		retryDelay:        cfg.RetryDelay,
		logger:            log,
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

		data, err := d.downloadAndExtract(ctx, url, task.Symbol, task.Date)
		if err == nil {
			d.logger.Debug().
				Str("symbol", task.Symbol).
				Str("date", task.Date.Format("2006-01-02")).
				Int("size", len(data)).
				Msg("Successfully downloaded and extracted data")
			return data, nil
		}

		// 404/数据不存在属于可预期情况，不应重试
		if domain.IsDataNotAvailableError(err) {
			return nil, err
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

// GetAllSymbolsFromBinance 从币安API获取符合过滤条件的交易对
func (d *BinanceDownloader) GetAllSymbolsFromBinance(ctx context.Context) ([]string, error) {
	start := time.Now()
	defer func() {
		logger.LogPerformance("binance_downloader", "get_all_symbols_from_binance", time.Since(start))
	}()

	d.logger.Info().
		Str("market_type", d.marketType).
		Str("exchange_info_url", d.exchangeInfoURL).
		Msg("Fetching symbols from Binance API")

	// 添加重试机制
	var lastErr error
	for attempt := 0; attempt <= d.retryCount; attempt++ {
		if attempt > 0 {
			d.logger.Warn().
				Str("url", d.exchangeInfoURL).
				Int("attempt", attempt).
				Err(lastErr).
				Msg("Retrying API request")
			// 等待重试延迟，响应上下文取消
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(d.retryDelay):
			}
		}

		d.logger.Debug().
			Str("url", d.exchangeInfoURL).
			Int("attempt", attempt+1).
			Msg("Requesting symbols from Binance API")

		req, err := http.NewRequestWithContext(ctx, "GET", d.exchangeInfoURL, nil)
		if err != nil {
			lastErr = fmt.Errorf("failed to create request: %w", err)
			continue
		}

		req.Header.Set("User-Agent", d.userAgent)

		resp, err := d.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("failed to fetch symbols from API: %w", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			d.logger.Error().
				Int("status_code", resp.StatusCode).
				Str("url", d.exchangeInfoURL).
				Msg("Failed to fetch symbols from API")
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("failed to read response body: %w", err)
			continue
		}

		// 解析JSON响应
		allSymbols := d.extractUSDTSymbolsFromAPI(body)

		if len(allSymbols) == 0 {
			lastErr = fmt.Errorf("no symbols found in API response")
			continue
		}

		d.logger.Info().
			Int("total_symbols", len(allSymbols)).
			Str("filter", d.filter).
			Msg("Symbols fetched from Binance API")

		return allSymbols, nil
	}

	// 如果所有重试都失败了，使用备用列表
	d.logger.Warn().
		Err(lastErr).
		Msg("Failed to get symbols from Binance API after all retries, using fallback list")
	return d.getFallbackSymbols(), nil
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
func (d *BinanceDownloader) downloadAndExtract(ctx context.Context, url string, symbol string, date time.Time) ([]byte, error) {
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
		if resp.StatusCode == http.StatusNotFound {
			// 404表示数据不存在，这是正常情况（某些月份的数据可能不存在）
			// 返回特殊的错误类型，上层可以优雅地跳过
			return nil, domain.NewDataNotAvailableError(symbol, date,
				fmt.Sprintf("data not found on server (HTTP %d)", resp.StatusCode))
		}
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
	return fmt.Sprintf("%s%s/%s/%s/%s", d.baseURL, d.monthlyKlinesPath, symbol, d.interval, filename)
}

func (d *BinanceDownloader) buildS3ListingURL(prefix string, delimiter string) string {
	values := url.Values{}
	values.Set("prefix", strings.TrimPrefix(prefix, "/"))
	if delimiter != "" {
		values.Set("delimiter", delimiter)
	}
	return fmt.Sprintf("%s?%s", strings.TrimRight(d.listingBaseURL, "/"), values.Encode())
}

func (d *BinanceDownloader) fetchS3Listing(ctx context.Context, prefix string, delimiter string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.buildS3ListingURL(prefix, delimiter), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 listing request: %w", err)
	}
	req.Header.Set("User-Agent", d.userAgent)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to request S3 listing: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected S3 listing status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read S3 listing body: %w", err)
	}

	return body, nil
}

func (d *BinanceDownloader) getAvailableDatesFromS3Listing(ctx context.Context, symbol string) ([]time.Time, error) {
	prefix := fmt.Sprintf("%s/%s/%s/", strings.TrimPrefix(d.monthlyKlinesPath, "/"), symbol, d.interval)
	body, err := d.fetchS3Listing(ctx, prefix, "")
	if err != nil {
		return nil, err
	}

	months := d.extractMonthsFromS3XML(string(body), symbol)
	if len(months) == 0 {
		return nil, fmt.Errorf("no monthly data found in S3 listing for %s", symbol)
	}

	dates := make([]time.Time, 0, len(months))
	for _, monthStr := range months {
		date, err := time.Parse("2006-01", monthStr)
		if err != nil {
			continue
		}
		dates = append(dates, date)
	}
	if len(dates) == 0 {
		return nil, fmt.Errorf("no parseable monthly dates found in S3 listing for %s", symbol)
	}

	return dates, nil
}

// GetAvailableDates 获取指定交易对的可用月份
func (d *BinanceDownloader) GetAvailableDates(ctx context.Context, symbol string) ([]time.Time, error) {
	if dates, err := d.getAvailableDatesFromS3Listing(ctx, symbol); err == nil && len(dates) > 0 {
		return dates, nil
	} else if err != nil {
		d.logger.Debug().
			Str("symbol", symbol).
			Err(err).
			Msg("Falling back to HEAD-based available date discovery")
	}

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
	minDate := defaultHistoricalStartDate(d.marketType)
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

		// 检查上下文取消，减少延迟以提高响应性
		select {
		case <-ctx.Done():
			d.logger.Info().
				Str("symbol", symbol).
				Msg("GetAvailableDates cancelled during binary search")
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
			// 继续二分查找
		}
	}

	// 如果没找到最早日期，从最小日期开始
	if earliestDate.IsZero() {
		earliestDate = minDate
	}

	// 生成从最早日期到当前日期的所有月份
	var availableDates []time.Time
	for checkDate := earliestDate; !checkDate.After(currentDate); checkDate = checkDate.AddDate(0, 1, 0) {
		// 检查上下文是否被取消
		select {
		case <-ctx.Done():
			d.logger.Info().
				Str("symbol", symbol).
				Msg("GetAvailableDates cancelled during date generation")
			return nil, ctx.Err()
		default:
		}
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
	// 匹配S3 XML中的Key元素，格式: <Key>data/.../monthly/klines/SYMBOL/1m/SYMBOL-1m-YYYY-MM.zip</Key>
	prefix := strings.TrimPrefix(d.monthlyKlinesPath, "/")
	pattern := fmt.Sprintf(
		`<Key>%s/%s/%s/%s-%s-(\d{4}-\d{2})\.zip</Key>`,
		regexp.QuoteMeta(prefix),
		regexp.QuoteMeta(symbol),
		regexp.QuoteMeta(d.interval),
		regexp.QuoteMeta(symbol),
		regexp.QuoteMeta(d.interval),
	)
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

// extractUSDTSymbolsFromAPI 从Binance API JSON响应中提取符合过滤条件的交易对
func (d *BinanceDownloader) extractUSDTSymbolsFromAPI(jsonData []byte) []string {
	type ExchangeInfo struct {
		Symbols []struct {
			Symbol string `json:"symbol"`
			Status string `json:"status"`
		} `json:"symbols"`
	}

	var exchangeInfo ExchangeInfo
	if err := json.Unmarshal(jsonData, &exchangeInfo); err != nil {
		d.logger.Error().
			Err(err).
			Msg("Failed to parse exchange info JSON")
		return nil
	}

	var filteredSymbols []string
	for _, symbolInfo := range exchangeInfo.Symbols {
		// 只获取活跃交易对，并应用后缀过滤
		if symbolInfo.Status != "TRADING" {
			continue
		}
		if d.filter != "" && !strings.HasSuffix(symbolInfo.Symbol, d.filter) {
			continue
		}
		filteredSymbols = append(filteredSymbols, symbolInfo.Symbol)
	}

	d.logger.Debug().
		Int("symbols_found", len(filteredSymbols)).
		Str("filter", d.filter).
		Str("market_type", d.marketType).
		Msg("Extracted symbols from API")

	return filteredSymbols
}

// getFallbackSymbols 返回备用的USDT交易对列表
// 当前只返回BTCUSDT用于验证流程
func (d *BinanceDownloader) getFallbackSymbols() []string {
	return []string{
		"BTCUSDT", // 只下载BTC数据进行验证
	}
}

func normalizeMarketType(raw string) string {
	marketType := strings.ToLower(strings.TrimSpace(raw))
	if marketType == "" {
		return "spot"
	}
	switch marketType {
	case "spot", "futures_um", "futures_cm":
		return marketType
	default:
		return "spot"
	}
}

func defaultExchangeInfoURL(marketType string) string {
	switch marketType {
	case "futures_um":
		return "https://fapi.binance.com/fapi/v1/exchangeInfo"
	case "futures_cm":
		return "https://dapi.binance.com/dapi/v1/exchangeInfo"
	default:
		return "https://api.binance.com/api/v3/exchangeInfo"
	}
}

func normalizeMonthlyKlinesPath(dataPath, marketType string) string {
	path := strings.TrimSpace(dataPath)
	if path == "" {
		switch marketType {
		case "futures_um":
			path = "/data/futures/um/daily/klines"
		case "futures_cm":
			path = "/data/futures/cm/daily/klines"
		default:
			path = "/data/spot/daily/klines"
		}
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	path = strings.TrimSuffix(path, "/")
	path = strings.Replace(path, "/daily/", "/monthly/", 1)
	if !strings.HasSuffix(path, "/klines") {
		path = strings.TrimSuffix(path, "/") + "/klines"
	}
	return path
}

func defaultHistoricalStartDate(marketType string) time.Time {
	switch marketType {
	case "futures_um":
		return time.Date(2019, 9, 1, 0, 0, 0, 0, time.UTC)
	case "futures_cm":
		return time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC)
	default:
		return time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC)
	}
}
