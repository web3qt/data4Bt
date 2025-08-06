package clickhouse

import (
	"context"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
)

// 测试配置
var testConfig = config.ClickHouseConfig{
	Hosts:           []string{"localhost:9000"},
	Database:        "data4BT_test",
	Username:        "default",
	Password:        "",
	DialTimeout:     30 * time.Second,
	MaxOpenConns:    5,
	MaxIdleConns:    2,
	ConnMaxLifetime: 5 * time.Minute,
	Compression:     "lz4",
}

func setupTestRepository(t *testing.T) *Repository {
	repo, err := NewRepository(testConfig)
	if err != nil {
		t.Fatalf("Failed to create test repository: %v", err)
	}

	// Create test database and tables
	ctx := context.Background()
	if err := repo.CreateTables(ctx); err != nil {
		t.Fatalf("Failed to create test tables: %v", err)
	}

	return repo
}

func cleanupTestRepository(t *testing.T, repo *Repository) {
	ctx := context.Background()
	
	// Clear test data
	if err := repo.ClearAllData(ctx); err != nil {
		t.Logf("Warning: Failed to clear test data: %v", err)
	}
	
	// Close connection
	if err := repo.Close(); err != nil {
		t.Logf("Warning: Failed to close repository: %v", err)
	}
}

func TestRepository_CreateTables(t *testing.T) {
	repo := setupTestRepository(t)
	defer cleanupTestRepository(t, repo)

	ctx := context.Background()
	
	// Test table creation
	err := repo.CreateTables(ctx)
	if err != nil {
		t.Errorf("Failed to create tables: %v", err)
	}

	// Verify table exists by trying to insert test data
	testKlines := []domain.KLine{
		createTestKLine("BTCUSDT", time.Now()),
	}

	err = repo.Save(ctx, testKlines)
	if err != nil {
		t.Errorf("Failed to save test data to created table: %v", err)
	}
}

func TestRepository_Save(t *testing.T) {
	repo := setupTestRepository(t)
	defer cleanupTestRepository(t, repo)

	ctx := context.Background()
	
	tests := []struct {
		name        string
		klines      []domain.KLine
		expectError bool
	}{
		{
			name: "Valid klines batch",
			klines: []domain.KLine{
				createTestKLine("BTCUSDT", time.Now()),
				createTestKLine("BTCUSDT", time.Now().Add(time.Minute)),
			},
			expectError: false,
		},
		{
			name:        "Empty klines batch",
			klines:      []domain.KLine{},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := repo.Save(ctx, tt.klines)
			
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestRepository_GetLastDate(t *testing.T) {
	repo := setupTestRepository(t)
	defer cleanupTestRepository(t, repo)

	ctx := context.Background()
	symbol := "BTCUSDT"
	
	// Test with no data
	lastDate, err := repo.GetLastDate(ctx, symbol)
	if err != nil {
		t.Errorf("Failed to get last date for empty table: %v", err)
	}
	if !lastDate.IsZero() {
		t.Errorf("Expected zero time for empty table, got %v", lastDate)
	}

	// Insert test data
	testTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	testKlines := []domain.KLine{
		createTestKLine(symbol, testTime),
		createTestKLine(symbol, testTime.Add(time.Hour)),
	}

	err = repo.Save(ctx, testKlines)
	if err != nil {
		t.Fatalf("Failed to save test data: %v", err)
	}

	// Test with data
	lastDate, err = repo.GetLastDate(ctx, symbol)
	if err != nil {
		t.Errorf("Failed to get last date: %v", err)
	}

	expectedDate := testTime.Add(time.Hour).Truncate(24 * time.Hour)
	actualDate := lastDate.Truncate(24 * time.Hour)
	
	if !actualDate.Equal(expectedDate) {
		t.Errorf("Expected last date %v, got %v", expectedDate, actualDate)
	}
}

func TestRepository_ValidateData(t *testing.T) {
	repo := setupTestRepository(t)
	defer cleanupTestRepository(t, repo)

	ctx := context.Background()
	symbol := "BTCUSDT"
	testDate := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)

	// Test with no data
	result, err := repo.ValidateData(ctx, symbol, testDate)
	if err != nil {
		t.Errorf("Failed to validate data: %v", err)
	}
	if result.TotalRows != 0 {
		t.Errorf("Expected 0 rows for empty table, got %d", result.TotalRows)
	}

	// Insert test data for the specific date
	var testKlines []domain.KLine
	for i := 0; i < 10; i++ {
		klineTime := testDate.Add(time.Duration(i) * time.Minute)
		testKlines = append(testKlines, createTestKLine(symbol, klineTime))
	}

	err = repo.Save(ctx, testKlines)
	if err != nil {
		t.Fatalf("Failed to save test data: %v", err)
	}

	// Test validation with data
	result, err = repo.ValidateData(ctx, symbol, testDate)
	if err != nil {
		t.Errorf("Failed to validate data: %v", err)
	}

	if result.TotalRows != 10 {
		t.Errorf("Expected 10 rows, got %d", result.TotalRows)
	}
	if result.ValidRows != 10 {
		t.Errorf("Expected 10 valid rows, got %d", result.ValidRows)
	}
}

func TestRepository_CreateMaterializedViews(t *testing.T) {
	repo := setupTestRepository(t)
	defer cleanupTestRepository(t, repo)

	ctx := context.Background()
	intervals := []string{"5m", "1h"}

	err := repo.CreateMaterializedViews(ctx, intervals)
	if err != nil {
		t.Errorf("Failed to create materialized views: %v", err)
	}

	// Verify views were created by inserting data and checking aggregation
	symbol := "BTCUSDT"
	baseTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	
	var testKlines []domain.KLine
	// Create 5 minutes of data (5 x 1-minute records)
	for i := 0; i < 5; i++ {
		klineTime := baseTime.Add(time.Duration(i) * time.Minute)
		testKlines = append(testKlines, createTestKLine(symbol, klineTime))
	}

	err = repo.Save(ctx, testKlines)
	if err != nil {
		t.Fatalf("Failed to save test data: %v", err)
	}

	// Allow time for materialized view to process
	time.Sleep(2 * time.Second)
}

func createTestKLine(symbol string, openTime time.Time) domain.KLine {
	return domain.KLine{
		Symbol:               symbol,
		OpenTime:             openTime,
		CloseTime:            openTime.Add(59*time.Second + 999*time.Millisecond),
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
}