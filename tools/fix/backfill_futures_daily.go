package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/pkg/binance"
	"binance-data-loader/pkg/clickhouse"
	"binance-data-loader/pkg/parser"
)

type task struct {
	symbol string
	day    time.Time
}

type dailyFetcher struct {
	client     *http.Client
	baseURL    string
	apiBaseURL string
	retryCount int
	retryDelay time.Duration
	userAgent  string
}

func newDailyFetcher(cfg *config.Config) (*dailyFetcher, error) {
	client := &http.Client{Timeout: cfg.Binance.Timeout}
	if client.Timeout == 0 || client.Timeout > 30*time.Second {
		client.Timeout = 30 * time.Second
	}

	if cfg.Binance.ProxyURL != "" {
		proxyURL, err := url.Parse(cfg.Binance.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("parse proxy url: %w", err)
		}
		client.Transport = &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	}

	return &dailyFetcher{
		client:     client,
		baseURL:    strings.TrimRight(cfg.Binance.BaseURL, "/"),
		apiBaseURL: "https://fapi.binance.com",
		retryCount: cfg.Binance.RetryCount,
		retryDelay: cfg.Binance.RetryDelay,
		userAgent:  cfg.Downloader.UserAgent,
	}, nil
}

func (f *dailyFetcher) fetch(ctx context.Context, symbol string, day time.Time) ([]byte, error) {
	url := fmt.Sprintf(
		"%s/data/futures/um/daily/klines/%s/1m/%s-1m-%s.zip",
		f.baseURL,
		symbol,
		symbol,
		day.UTC().Format("2006-01-02"),
	)

	var lastErr error
	for attempt := 0; attempt <= f.retryCount; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(f.retryDelay):
			}
		}

		data, err := f.downloadAndExtract(ctx, url)
		if err == nil {
			return data, nil
		}
		if errors.Is(err, os.ErrNotExist) {
			apiData, apiErr := f.fetchFromAPI(ctx, symbol, day)
			if apiErr == nil {
				return apiData, nil
			}
			if errors.Is(apiErr, os.ErrNotExist) {
				return nil, err
			}
			lastErr = apiErr
			continue
		}
		lastErr = err
	}

	return nil, fmt.Errorf("download %s %s: %w", symbol, day.Format("2006-01-02"), lastErr)
}

func (f *dailyFetcher) downloadAndExtract(ctx context.Context, targetURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", f.userAgent)
	req.Header.Set("Accept-Encoding", "gzip, deflate")

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, os.ErrNotExist
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	zipData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	reader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return nil, err
	}

	for _, file := range reader.File {
		if !strings.HasSuffix(strings.ToLower(file.Name), ".csv") {
			continue
		}
		rc, err := file.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	}

	return nil, fmt.Errorf("no csv found in zip")
}

func (f *dailyFetcher) fetchFromAPI(ctx context.Context, symbol string, day time.Time) ([]byte, error) {
	startTime := day.UTC()
	endTime := startTime.Add(24 * time.Hour)
	reqURL := fmt.Sprintf(
		"%s/fapi/v1/klines?symbol=%s&interval=1m&startTime=%d&endTime=%d&limit=1500",
		f.apiBaseURL,
		symbol,
		startTime.UnixMilli(),
		endTime.UnixMilli(),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", f.userAgent)

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected api status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var payload [][]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode api response: %w", err)
	}
	if len(payload) == 0 {
		return nil, os.ErrNotExist
	}

	var builder strings.Builder
	for _, row := range payload {
		record, err := apiRowToCSV(row)
		if err != nil {
			return nil, err
		}
		builder.WriteString(record)
		builder.WriteByte('\n')
	}

	return []byte(builder.String()), nil
}

func apiRowToCSV(row []interface{}) (string, error) {
	if len(row) < 12 {
		return "", fmt.Errorf("unexpected api row length: %d", len(row))
	}

	values := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		switch v := row[i].(type) {
		case string:
			values = append(values, v)
		case float64:
			values = append(values, fmt.Sprintf("%.0f", v))
		default:
			return "", fmt.Errorf("unexpected api field type at index %d: %T", i, row[i])
		}
	}

	return strings.Join(values, ","), nil
}

func loadSymbols(ctx context.Context, cfg *config.Config, downloader *binance.BinanceDownloader, symbolsArg string) ([]string, error) {
	if strings.TrimSpace(symbolsArg) != "" {
		parts := strings.Split(symbolsArg, ",")
		symbols := make([]string, 0, len(parts))
		for _, part := range parts {
			symbol := strings.ToUpper(strings.TrimSpace(part))
			if symbol != "" {
				symbols = append(symbols, symbol)
			}
		}
		if len(symbols) == 0 {
			return nil, fmt.Errorf("no valid symbols in -symbols")
		}
		return symbols, nil
	}

	return downloader.GetSymbols(ctx)
}

func main() {
	var (
		configPath  string
		startDate   string
		endDate     string
		symbolsArg  string
		concurrency int
	)

	flag.StringVar(&configPath, "config", "configs/config-futures-um-2m.yml", "config file path")
	flag.StringVar(&startDate, "start", "", "start date (YYYY-MM-DD)")
	flag.StringVar(&endDate, "end", "", "end date (YYYY-MM-DD)")
	flag.StringVar(&symbolsArg, "symbols", "", "optional comma-separated symbols")
	flag.IntVar(&concurrency, "concurrency", 24, "worker concurrency")
	flag.Parse()

	if startDate == "" || endDate == "" {
		fmt.Fprintln(os.Stderr, "-start and -end are required")
		os.Exit(1)
	}

	startDay, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -start: %v\n", err)
		os.Exit(1)
	}
	endDay, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -end: %v\n", err)
		os.Exit(1)
	}
	if endDay.Before(startDay) {
		fmt.Fprintln(os.Stderr, "-end must be >= -start")
		os.Exit(1)
	}
	if concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "-concurrency must be > 0")
		os.Exit(1)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	repo, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect clickhouse: %v\n", err)
		os.Exit(1)
	}
	defer repo.Close()

	if err := repo.CreateTables(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "create tables: %v\n", err)
		os.Exit(1)
	}

	csvParser := parser.NewCSVParser(cfg.Parser)
	symbolDownloader := binance.NewBinanceDownloader(cfg.Binance, cfg.Downloader)
	symbols, err := loadSymbols(ctx, cfg, symbolDownloader, symbolsArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load symbols: %v\n", err)
		os.Exit(1)
	}

	fetcher, err := newDailyFetcher(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init fetcher: %v\n", err)
		os.Exit(1)
	}

	days := make([]time.Time, 0)
	for day := startDay.UTC(); !day.After(endDay.UTC()); day = day.AddDate(0, 0, 1) {
		days = append(days, day)
	}

	totalTasks := len(symbols) * len(days)
	fmt.Printf("Starting futures UM daily backfill: symbols=%d days=%d tasks=%d range=%s..%s db=%s\n",
		len(symbols), len(days), totalTasks, startDay.Format("2006-01-02"), endDay.Format("2006-01-02"), cfg.Database.ClickHouse.Database)

	taskCh := make(chan task, concurrency*2)
	errCh := make(chan error, concurrency)

	var processed atomic.Int64
	var imported atomic.Int64
	var skipped atomic.Int64
	var failed atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range taskCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				data, err := fetcher.fetch(ctx, t.symbol, t.day)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						skipped.Add(1)
						current := processed.Add(1)
						if current%200 == 0 || current == int64(totalTasks) {
							fmt.Printf("Progress: %d/%d imported=%d skipped=%d failed=%d\n",
								current, totalTasks, imported.Load(), skipped.Load(), failed.Load())
						}
						continue
					}
					failed.Add(1)
					select {
					case errCh <- fmt.Errorf("%s %s download: %w", t.symbol, t.day.Format("2006-01-02"), err):
					default:
					}
					processed.Add(1)
					return
				}

				klines, validation, err := csvParser.Parse(ctx, data, t.symbol)
				if err != nil {
					failed.Add(1)
					select {
					case errCh <- fmt.Errorf("%s %s parse: %w", t.symbol, t.day.Format("2006-01-02"), err):
					default:
					}
					processed.Add(1)
					return
				}
				if validation != nil && len(validation.Errors) > 0 && !cfg.Parser.SkipInvalidRows {
					failed.Add(1)
					select {
					case errCh <- fmt.Errorf("%s %s parse validation errors: %d", t.symbol, t.day.Format("2006-01-02"), len(validation.Errors)):
					default:
					}
					processed.Add(1)
					return
				}
				if len(klines) == 0 {
					skipped.Add(1)
					current := processed.Add(1)
					if current%200 == 0 || current == int64(totalTasks) {
						fmt.Printf("Progress: %d/%d imported=%d skipped=%d failed=%d\n",
							current, totalTasks, imported.Load(), skipped.Load(), failed.Load())
					}
					continue
				}
				klines = filterKlinesForDay(klines, t.day)
				if len(klines) == 0 {
					skipped.Add(1)
					current := processed.Add(1)
					if current%200 == 0 || current == int64(totalTasks) {
						fmt.Printf("Progress: %d/%d imported=%d skipped=%d failed=%d\n",
							current, totalTasks, imported.Load(), skipped.Load(), failed.Load())
					}
					continue
				}

				if err := repo.Save(ctx, klines); err != nil {
					failed.Add(1)
					select {
					case errCh <- fmt.Errorf("%s %s save: %w", t.symbol, t.day.Format("2006-01-02"), err):
					default:
					}
					processed.Add(1)
					return
				}

				imported.Add(1)
				current := processed.Add(1)
				if current%200 == 0 || current == int64(totalTasks) {
					fmt.Printf("Progress: %d/%d imported=%d skipped=%d failed=%d\n",
						current, totalTasks, imported.Load(), skipped.Load(), failed.Load())
				}
			}
		}()
	}

	go func() {
		defer close(taskCh)
		for _, symbol := range symbols {
			for _, day := range days {
				select {
				case <-ctx.Done():
					return
				case taskCh <- task{symbol: symbol, day: day}:
				}
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		if err == nil {
			continue
		}
		fmt.Fprintf(os.Stderr, "backfill failed: %v\n", err)
		stop()
		os.Exit(1)
	}

	if ctx.Err() != nil && !errors.Is(ctx.Err(), context.Canceled) {
		fmt.Fprintf(os.Stderr, "context error: %v\n", ctx.Err())
		os.Exit(1)
	}

	fmt.Printf("Completed: imported=%d skipped=%d failed=%d total=%d\n",
		imported.Load(), skipped.Load(), failed.Load(), totalTasks)
}

func filterKlinesForDay(klines []domain.KLine, day time.Time) []domain.KLine {
	day = day.UTC()
	nextDay := day.Add(24 * time.Hour)
	filtered := make([]domain.KLine, 0, len(klines))
	for _, kline := range klines {
		openTime := kline.OpenTime.UTC()
		if !openTime.Before(day) && openTime.Before(nextDay) {
			filtered = append(filtered, kline)
		}
	}
	return filtered
}
