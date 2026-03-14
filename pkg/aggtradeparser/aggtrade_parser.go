package aggtradeparser

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"binance-data-loader/internal/domain"
)

type AggTradeCSVParser struct{}

type aggTrade struct {
	price      float64
	qty        float64
	firstID    int64
	lastID     int64
	timestamp  time.Time
	buyerMaker bool
}

func NewAggTradeCSVParser() *AggTradeCSVParser {
	return &AggTradeCSVParser{}
}

func (p *AggTradeCSVParser) Parse(ctx context.Context, data []byte, symbol string) ([]domain.KLine, *domain.ValidationResult, error) {
	reader := csv.NewReader(strings.NewReader(string(data)))
	reader.ReuseRecord = true

	result := &domain.ValidationResult{
		Valid:    true,
		Errors:   []string{},
		Warnings: []string{},
	}

	var (
		klines        []domain.KLine
		currentKline  *domain.KLine
		currentSecond time.Time
	)

	flushCurrent := func() {
		if currentKline != nil {
			klines = append(klines, *currentKline)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}

		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, result, fmt.Errorf("failed to read aggTrade CSV: %w", err)
		}

		result.TotalRows++

		if isBlankRecord(record) {
			continue
		}
		if result.TotalRows == 1 && isHeaderRecord(record) {
			result.TotalRows--
			continue
		}

		trade, err := parseAggTrade(record)
		if err != nil {
			result.Valid = false
			result.InvalidRows++
			result.Errors = append(result.Errors, err.Error())
			continue
		}

		result.ValidRows++

		second := trade.timestamp.UTC().Truncate(time.Second)
		if currentKline == nil || !second.Equal(currentSecond) {
			flushCurrent()
			currentSecond = second
			currentKline = newKLine(symbol, second, trade.price)
		}

		applyTrade(currentKline, trade)
	}

	flushCurrent()

	if len(klines) == 0 {
		return nil, result, fmt.Errorf("no valid aggTrade rows parsed")
	}

	result.Valid = result.InvalidRows == 0
	return klines, result, nil
}

func (p *AggTradeCSVParser) ValidateCSV(data []byte) error {
	reader := csv.NewReader(strings.NewReader(string(data)))
	for {
		record, err := reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read aggTrade CSV: %w", err)
		}
		if isBlankRecord(record) {
			continue
		}
		if isHeaderRecord(record) {
			continue
		}
		if len(record) < 7 {
			return fmt.Errorf("invalid aggTrade CSV format: expected at least 7 columns, got %d", len(record))
		}
		return nil
	}
}

func parseAggTrade(record []string) (*aggTrade, error) {
	if len(record) < 7 {
		return nil, fmt.Errorf("invalid aggTrade record length: expected 7 fields, got %d", len(record))
	}

	price, err := strconv.ParseFloat(strings.TrimSpace(record[1]), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid aggTrade price: %w", err)
	}
	qty, err := strconv.ParseFloat(strings.TrimSpace(record[2]), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid aggTrade quantity: %w", err)
	}
	firstID, err := strconv.ParseInt(strings.TrimSpace(record[3]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid aggTrade first trade id: %w", err)
	}
	lastID, err := strconv.ParseInt(strings.TrimSpace(record[4]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid aggTrade last trade id: %w", err)
	}
	timestampMillis, err := strconv.ParseInt(strings.TrimSpace(record[5]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid aggTrade timestamp: %w", err)
	}
	buyerMaker, err := strconv.ParseBool(strings.TrimSpace(record[6]))
	if err != nil {
		return nil, fmt.Errorf("invalid aggTrade buyer maker flag: %w", err)
	}

	return &aggTrade{
		price:      price,
		qty:        qty,
		firstID:    firstID,
		lastID:     lastID,
		timestamp:  time.UnixMilli(timestampMillis).UTC(),
		buyerMaker: buyerMaker,
	}, nil
}

func newKLine(symbol string, second time.Time, price float64) *domain.KLine {
	return &domain.KLine{
		Symbol:              symbol,
		OpenTime:            second,
		CloseTime:           second.Add(time.Second - time.Millisecond),
		OpenPrice:           price,
		HighPrice:           price,
		LowPrice:            price,
		ClosePrice:          price,
		Interval:            "1s",
		CreatedAt:           time.Now(),
		Volume:              0,
		QuoteAssetVolume:    0,
		NumberOfTrades:      0,
		TakerBuyBaseVolume:  0,
		TakerBuyQuoteVolume: 0,
	}
}

func applyTrade(kline *domain.KLine, trade *aggTrade) {
	if trade.price > kline.HighPrice {
		kline.HighPrice = trade.price
	}
	if trade.price < kline.LowPrice {
		kline.LowPrice = trade.price
	}

	kline.ClosePrice = trade.price
	kline.Volume += trade.qty

	quoteQty := trade.price * trade.qty
	kline.QuoteAssetVolume += quoteQty

	tradeCount := trade.lastID - trade.firstID + 1
	if tradeCount <= 0 {
		tradeCount = 1
	}
	kline.NumberOfTrades += tradeCount

	if !trade.buyerMaker {
		kline.TakerBuyBaseVolume += trade.qty
		kline.TakerBuyQuoteVolume += quoteQty
	}
}

func isBlankRecord(record []string) bool {
	for _, field := range record {
		if strings.TrimSpace(field) != "" {
			return false
		}
	}
	return true
}

func isHeaderRecord(record []string) bool {
	if len(record) == 0 {
		return false
	}

	first := strings.ToLower(strings.TrimSpace(record[0]))
	switch first {
	case "aggregatetradeid", "aggregate tradeid", "aggregate_tradeid", "aggtradeid", "agg_trade_id":
		return true
	default:
		return false
	}
}
