package parser

import (
	"context"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
)

func TestCSVParser_Parse(t *testing.T) {
	parser := NewCSVParser(config.ParserConfig{
		ValidateData:    true,
		SkipInvalidRows: false,
	})

	tests := []struct {
		name          string
		csvData       string
		symbol        string
		expectedCount int
		expectError   bool
		expectValid   bool
	}{
		{
			name: "Valid CSV data",
			csvData: `1609459200000,29374.99,29375.00,29374.98,29375.00,0.05134800,1609459259999,1507.51637800,85,0.02540200,745.13980000,0
1609459260000,29375.00,29375.01,29374.99,29375.01,0.03421100,1609459319999,1005.12345000,67,0.01823400,535.67890000,0`,
			symbol:        "BTCUSDT",
			expectedCount: 2,
			expectError:   false,
			expectValid:   true,
		},
		{
			name: "Invalid price data",
			csvData: `1609459200000,-29374.99,29375.00,29374.98,29375.00,0.05134800,1609459259999,1507.51637800,85,0.02540200,745.13980000,0`,
			symbol:        "BTCUSDT",
			expectedCount: 0,
			expectError:   false,
			expectValid:   false,
		},
		{
			name: "Invalid record length",
			csvData: `1609459200000,29374.99,29375.00`,
			symbol:        "BTCUSDT",
			expectedCount: 0,
			expectError:   false,
			expectValid:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			klines, result, err := parser.Parse(ctx, []byte(tt.csvData), tt.symbol)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if len(klines) != tt.expectedCount {
				t.Errorf("Expected %d klines, got %d", tt.expectedCount, len(klines))
			}

			if result.Valid != tt.expectValid {
				t.Errorf("Expected valid=%v, got %v", tt.expectValid, result.Valid)
			}

			// Validate kline structure if any data parsed
			if len(klines) > 0 {
				kline := klines[0]
				if kline.Symbol != tt.symbol {
					t.Errorf("Expected symbol %s, got %s", tt.symbol, kline.Symbol)
				}
				if kline.Interval != "1m" {
					t.Errorf("Expected interval 1m, got %s", kline.Interval)
				}
			}
		})
	}
}

func TestCSVParser_ValidateCSV(t *testing.T) {
	parser := NewCSVParser(config.ParserConfig{})
	
	tests := []struct {
		name        string
		csvData     string
		expectError bool
	}{
		{
			name:        "Valid CSV format",
			csvData:     "1609459200000,29374.99,29375.00,29374.98,29375.00,0.05134800,1609459259999,1507.51637800,85,0.02540200,745.13980000,0",
			expectError: false,
		},
		{
			name:        "Invalid CSV format - too few columns",
			csvData:     "1609459200000,29374.99,29375.00",
			expectError: true,
		},
		{
			name:        "Empty data",
			csvData:     "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parser.ValidateCSV([]byte(tt.csvData))
			
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestCSVParser_parseTimestamp(t *testing.T) {
	parser := NewCSVParser(config.ParserConfig{})
	
	tests := []struct {
		name        string
		timestamp   string
		expectError bool
	}{
		{
			name:        "Valid millisecond timestamp",
			timestamp:   "1609459200000",
			expectError: false,
		},
		{
			name:        "Invalid timestamp",
			timestamp:   "invalid",
			expectError: true,
		},
		{
			name:        "Empty timestamp",
			timestamp:   "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parser.parseTimestamp(tt.timestamp)
			
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestCSVParser_validateKLine(t *testing.T) {
	parser := NewCSVParser(config.ParserConfig{ValidateData: true})
	
	validKline := &domain.KLine{
		Symbol:               "BTCUSDT",
		OpenTime:             time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		CloseTime:            time.Date(2024, 1, 1, 12, 0, 59, 999000000, time.UTC),
		OpenPrice:            29374.99,
		HighPrice:            29375.01,
		LowPrice:             29374.98,
		ClosePrice:           29375.00,
		Volume:               0.05134800,
		QuoteAssetVolume:     1507.51637800,
		NumberOfTrades:       85,
		TakerBuyBaseVolume:   0.02540200,
		TakerBuyQuoteVolume:  745.13980000,
		Interval:             "1m",
		CreatedAt:            time.Now(),
	}

	tests := []struct {
		name        string
		kline       *domain.KLine
		expectError bool
	}{
		{
			name:        "Valid kline",
			kline:       validKline,
			expectError: false,
		},
		{
			name: "Invalid high < low",
			kline: &domain.KLine{
				Symbol:      "BTCUSDT",
				OpenTime:    time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
				CloseTime:   time.Date(2024, 1, 1, 12, 0, 59, 999000000, time.UTC),
				OpenPrice:   29374.99,
				HighPrice:   29374.90, // Lower than low price
				LowPrice:    29374.98,
				ClosePrice:  29375.00,
				Volume:      0.05134800,
				Interval:    "1m",
			},
			expectError: true,
		},
		{
			name: "Negative volume",
			kline: &domain.KLine{
				Symbol:     "BTCUSDT",
				OpenTime:   time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
				CloseTime:  time.Date(2024, 1, 1, 12, 0, 59, 999000000, time.UTC),
				OpenPrice:  29374.99,
				HighPrice:  29375.01,
				LowPrice:   29374.98,
				ClosePrice: 29375.00,
				Volume:     -0.05134800, // Negative volume
				Interval:   "1m",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parser.validateKLine(tt.kline)
			
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}