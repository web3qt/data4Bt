package gaps

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"binance-data-loader/internal/domain"
	"binance-data-loader/internal/logger"
)

// DataGap 代表检测到的数据缺口
type DataGap struct {
	Symbol        string    `json:"symbol"`
	GapType       string    `json:"gap_type"`       // "missing_months", "incomplete_months", "historical_gap"
	Description   string    `json:"description"`
	MissingMonths []string  `json:"missing_months"`
	StartDate     time.Time `json:"start_date"`
	EndDate       time.Time `json:"end_date"`
	Severity      string    `json:"severity"`       // "critical", "high", "medium", "low"
}

// ActualDataRange 代表数据库中实际存在的数据时间范围
type ActualDataRange struct {
	Symbol    string    `json:"symbol"`
	StartDate time.Time `json:"start_date"`
	EndDate   time.Time `json:"end_date"`
}

// GapDetector 数据缺口检测器
type GapDetector struct {
	repository domain.KLineRepository
	downloader domain.Downloader
	logger     zerolog.Logger
}

// NewGapDetector 创建新的缺口检测器
func NewGapDetector(repository domain.KLineRepository, downloader domain.Downloader) *GapDetector {
	return &GapDetector{
		repository: repository,
		downloader: downloader,
		logger:     logger.GetLogger("gap_detector"),
	}
}

// DetectAllGaps 检测所有交易对的数据缺口
func (gd *GapDetector) DetectAllGaps(ctx context.Context, symbols []string, startDate, endDate *time.Time) ([]*DataGap, error) {
	gd.logger.Info().
		Int("symbols_count", len(symbols)).
		Msg("Starting gap detection for all symbols")

	var allGaps []*DataGap

	for i, symbol := range symbols {
		gd.logger.Info().
			Int("current", i+1).
			Int("total", len(symbols)).
			Str("symbol", symbol).
			Msg("Detecting gaps for symbol")

		gaps, err := gd.DetectSymbolGaps(ctx, symbol, startDate, endDate)
		if err != nil {
			gd.logger.Warn().
				Err(err).
				Str("symbol", symbol).
				Msg("Failed to detect gaps for symbol, skipping")
			continue
		}

		allGaps = append(allGaps, gaps...)

		// 检查上下文是否被取消
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	gd.logger.Info().
		Int("total_gaps", len(allGaps)).
		Msg("Gap detection completed")

	return allGaps, nil
}

// DetectSymbolGaps 检测单个交易对的数据缺口
func (gd *GapDetector) DetectSymbolGaps(ctx context.Context, symbol string, startDate, endDate *time.Time) ([]*DataGap, error) {
	gd.logger.Debug().Str("symbol", symbol).Msg("Starting gap detection for symbol")

	// 1. 首先获取数据库中该交易对的实际数据时间范围
	actualDataRange, err := gd.getActualDataRange(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual data range for %s: %w", symbol, err)
	}

	// 2. 获取币安可用的月份数据
	availableMonths, err := gd.downloader.GetAvailableDates(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get available dates for %s: %w", symbol, err)
	}

	if len(availableMonths) == 0 {
		gd.logger.Debug().Str("symbol", symbol).Msg("No available months for symbol")
		return nil, nil
	}

	// 3. 确定检测范围
	var detectStartDate, detectEndDate time.Time


	if actualDataRange == nil {
		// 数据库中完全没有数据，但币安有可用数据 - 这是完全缺失的情况
		gd.logger.Debug().Str("symbol", symbol).Msg("No actual data found for symbol, treating as complete gap")
		
		// 检查是否有可用的月份数据
		if len(availableMonths) > 0 {
			// 使用币安可用数据的范围作为检测范围
			detectStartDate = availableMonths[0]
			detectEndDate = availableMonths[len(availableMonths)-1]
			
			
			// 但是要限制到合理的历史范围，避免包含未来数据
			now := time.Now()
			// 使用上个月作为最大范围，确保不包含当前月份的不完整数据
			maxEndDate := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -1, 0)
			if detectEndDate.After(maxEndDate) {
				detectEndDate = maxEndDate
			}
		} else {
			// 如果没有可用月份，说明这个交易对根本不存在或不可用
			gd.logger.Debug().Str("symbol", symbol).Msg("No available months for symbol, skipping gap detection")
			return nil, nil
		}
	} else {
		// 数据库中有数据，基于实际数据范围检测缺口
		detectStartDate = actualDataRange.StartDate
		detectEndDate = actualDataRange.EndDate
	}

	// 用户指定的范围可以进一步限制检测范围
	if startDate != nil && startDate.After(detectStartDate) {
		detectStartDate = *startDate
	}
	if endDate != nil && endDate.Before(detectEndDate) {
		detectEndDate = *endDate
	}

	// 4. 获取数据库中已有的月份数据
	existingMonths, err := gd.getExistingMonths(ctx, symbol, detectStartDate, detectEndDate)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing months for %s: %w", symbol, err)
	}

	// 5. 对比分析，找出缺口（使用实际数据范围内的可用月份）
	gaps := gd.analyzeGaps(symbol, availableMonths, existingMonths, detectStartDate, detectEndDate)

	gd.logger.Debug().
		Str("symbol", symbol).
		Int("gaps_found", len(gaps)).
		Msg("Gap detection completed for symbol")

	return gaps, nil
}

// getActualDataRange 获取数据库中该交易对的实际数据时间范围
func (gd *GapDetector) getActualDataRange(ctx context.Context, symbol string) (*ActualDataRange, error) {
	// 查询数据库中该交易对的实际数据范围
	query := `
		SELECT 
			min(open_time) as start_date,
			max(open_time) as end_date
		FROM klines_1m 
		WHERE symbol = ?
	`

	result, err := gd.repository.Query(ctx, query, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to query actual data range: %w", err)
	}
	
	// Type assertion to get the actual rows
	rows, ok := result.(interface{ Next() bool; Scan(dest ...interface{}) error; Close() error })
	if !ok {
		return nil, fmt.Errorf("unexpected result type from query")
	}
	defer rows.Close()

	if !rows.Next() {
		// 没有数据
		return nil, nil
	}

	var startDate, endDate *time.Time
	if err := rows.Scan(&startDate, &endDate); err != nil {
		return nil, fmt.Errorf("failed to scan data range: %w", err)
	}

	// 如果没有数据，返回nil
	if startDate == nil || endDate == nil {
		return nil, nil
	}

	// 检查是否为零值时间（数据库没有数据时可能返回零值）
	if startDate.IsZero() || endDate.IsZero() {
		return nil, nil
	}

	return &ActualDataRange{
		Symbol:    symbol,
		StartDate: *startDate,
		EndDate:   *endDate,
	}, nil
}

// getExistingMonths 获取数据库中已存在的月份数据
func (gd *GapDetector) getExistingMonths(ctx context.Context, symbol string, startDate, endDate time.Time) (map[string]bool, error) {
	existingMonths := make(map[string]bool)

	// 查询数据库中该时间范围内有数据的月份
	query := `
		SELECT DISTINCT toYYYYMM(open_time) as month_key
		FROM klines_1m 
		WHERE symbol = ? 
		AND open_time >= ? 
		AND open_time <= ?
		ORDER BY month_key
	`

	result, err := gd.repository.Query(ctx, query, symbol, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query existing months: %w", err)
	}
	
	// Type assertion to get the actual rows
	rows, ok := result.(interface{ Next() bool; Scan(dest ...interface{}) error; Close() error })
	if !ok {
		return nil, fmt.Errorf("unexpected result type from query")
	}
	defer rows.Close()

	for rows.Next() {
		var monthKey uint32
		if err := rows.Scan(&monthKey); err != nil {
			return nil, fmt.Errorf("failed to scan month key: %w", err)
		}

		// 转换为标准格式 YYYY-MM
		monthStr := fmt.Sprintf("%d", monthKey)
		if len(monthStr) == 6 {
			formattedMonth := monthStr[:4] + "-" + monthStr[4:]
			existingMonths[formattedMonth] = true
		}
	}

	return existingMonths, nil
}

// analyzeGaps 分析缺口
func (gd *GapDetector) analyzeGaps(symbol string, availableMonths []time.Time, existingMonths map[string]bool, startDate, endDate time.Time) []*DataGap {
	var gaps []*DataGap

	gd.logger.Debug().
		Str("symbol", symbol).
		Time("startDate", startDate).
		Time("endDate", endDate).
		Int("availableMonths", len(availableMonths)).
		Int("existingMonths", len(existingMonths)).
		Msg("Starting gap analysis")

	// 转换可用月份为字符串格式
	availableMonthsMap := make(map[string]bool)
	for _, month := range availableMonths {
		if month.Before(startDate) || month.After(endDate) {
			gd.logger.Debug().
				Str("symbol", symbol).
				Time("month", month).
				Msg("Skipping month outside date range")
			continue
		}
		monthStr := month.Format("2006-01")
		availableMonthsMap[monthStr] = true
	}

	gd.logger.Debug().
		Str("symbol", symbol).
		Int("filteredAvailableMonths", len(availableMonthsMap)).
		Msg("Filtered available months")

	// 找出缺失的月份
	var missingMonths []string
	var historicalGapStart, historicalGapEnd *time.Time

	// 按时间顺序检查每个可用月份
	sortedMonths := make([]string, 0, len(availableMonthsMap))
	for month := range availableMonthsMap {
		sortedMonths = append(sortedMonths, month)
	}
	sort.Strings(sortedMonths)

	for _, month := range sortedMonths {
		if !existingMonths[month] {
			missingMonths = append(missingMonths, month)
			
			// 记录历史缺口的开始和结束时间
			if monthTime, err := time.Parse("2006-01", month); err == nil {
				if historicalGapStart == nil || monthTime.Before(*historicalGapStart) {
					historicalGapStart = &monthTime
				}
				if historicalGapEnd == nil || monthTime.After(*historicalGapEnd) {
					historicalGapEnd = &monthTime
				}
			}
		}
	}

	// 如果有缺失月份，创建缺口记录
	if len(missingMonths) > 0 {
		severity := gd.calculateSeverity(len(missingMonths), len(sortedMonths))
		description := fmt.Sprintf("缺失 %d 个月的数据", len(missingMonths))
		
		if len(missingMonths) <= 3 {
			description = fmt.Sprintf("缺失月份: %s", strings.Join(missingMonths, ", "))
		}

		gap := &DataGap{
			Symbol:        symbol,
			GapType:       "historical_gap",
			Description:   description,
			MissingMonths: missingMonths,
			Severity:      severity,
		}

		if historicalGapStart != nil {
			gap.StartDate = *historicalGapStart
		}
		if historicalGapEnd != nil {
			gap.EndDate = *historicalGapEnd
		}

		gaps = append(gaps, gap)
	}

	return gaps
}

// calculateSeverity 计算缺口严重程度
func (gd *GapDetector) calculateSeverity(missingCount, totalCount int) string {
	ratio := float64(missingCount) / float64(totalCount)
	
	switch {
	case ratio >= 0.5:
		return "critical"
	case ratio >= 0.3:
		return "high"
	case ratio >= 0.1:
		return "medium"
	default:
		return "low"
	}
}

// GenerateBackfillTasks 生成补全任务
func (gd *GapDetector) GenerateBackfillTasks(gaps []*DataGap) []domain.DownloadTask {
	var tasks []domain.DownloadTask

	for _, gap := range gaps {
		for _, monthStr := range gap.MissingMonths {
			if monthTime, err := time.Parse("2006-01", monthStr); err == nil {
				task := domain.DownloadTask{
					Symbol: gap.Symbol,
					Date:   monthTime,
				}
				tasks = append(tasks, task)
			}
		}
	}

	// 按照symbol和时间排序，确保有序处理
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Symbol != tasks[j].Symbol {
			return tasks[i].Symbol < tasks[j].Symbol
		}
		return tasks[i].Date.Before(tasks[j].Date)
	})

	gd.logger.Info().
		Int("gaps_count", len(gaps)).
		Int("tasks_count", len(tasks)).
		Msg("Generated backfill tasks")

	return tasks
}