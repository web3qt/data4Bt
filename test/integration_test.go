package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/pkg/clickhouse"
	"binance-data-loader/pkg/parser"
)

// TestIntegration_DatabaseOperations tests the complete database integration
func TestIntegration_DatabaseOperations(t *testing.T) {
	// Skip if no database available
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration test (set INTEGRATION_TEST=true to run)")
	}

	// Setup test configuration
	cfg := config.ClickHouseConfig{
		Hosts:           []string{"localhost:9000"},
		Database:        "data4BT_integration_test",
		Username:        "default",
		Password:        "",
		DialTimeout:     30 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: 5 * time.Minute,
		Compression:     "lz4",
	}

	ctx := context.Background()

	// Create repository
	repo, err := clickhouse.NewRepository(cfg)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer func() {
		// Cleanup
		repo.ClearAllData(ctx)
		repo.Close()
	}()

	// Test 1: Database and table creation
	t.Run("CreateTables", func(t *testing.T) {
		err := repo.CreateTables(ctx)
		if err != nil {
			t.Fatalf("Failed to create tables: %v", err)
		}
	})

	// Test 2: Data insertion
	t.Run("SaveKLines", func(t *testing.T) {
		testKlines := []domain.KLine{
			createIntegrationTestKLine("BTCUSDT", time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)),
			createIntegrationTestKLine("BTCUSDT", time.Date(2024, 1, 15, 12, 1, 0, 0, time.UTC)),
		}

		err := repo.Save(ctx, testKlines)
		if err != nil {
			t.Fatalf("Failed to save klines: %v", err)
		}

		// Verify data was saved
		lastDate, err := repo.GetLastDate(ctx, "BTCUSDT")
		if err != nil {
			t.Fatalf("Failed to get last date: %v", err)
		}

		expectedDate := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		actualDate := lastDate.Truncate(24 * time.Hour)
		
		if !actualDate.Equal(expectedDate) {
			t.Errorf("Expected last date %v, got %v", expectedDate, actualDate)
		}
	})

	// Test 3: Data validation
	t.Run("ValidateData", func(t *testing.T) {
		testDate := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		result, err := repo.ValidateData(ctx, "BTCUSDT", testDate)
		if err != nil {
			t.Fatalf("Failed to validate data: %v", err)
		}

		if result.TotalRows == 0 {
			t.Errorf("Expected some data rows, got 0")
		}
		if result.ValidRows != result.TotalRows {
			t.Errorf("Expected all rows to be valid, got %d valid out of %d total", 
				result.ValidRows, result.TotalRows)
		}
	})

	// Test 4: Materialized views
	t.Run("CreateMaterializedViews", func(t *testing.T) {
		intervals := []string{"5m", "1h"}
		err := repo.CreateMaterializedViews(ctx, intervals)
		if err != nil {
			t.Fatalf("Failed to create materialized views: %v", err)
		}

		// Allow time for materialized views to process
		time.Sleep(2 * time.Second)
	})
}

// TestIntegration_EndToEndDataFlow tests the complete data pipeline
func TestIntegration_EndToEndDataFlow(t *testing.T) {
	// Skip if no database available
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration test (set INTEGRATION_TEST=true to run)")
	}

	// Load configuration
	cfg, err := config.Load("../../test/config_test.yml")
	if err != nil {
		t.Fatalf("Failed to load test config: %v", err)
	}

	// Override database name for testing
	cfg.Database.ClickHouse.Database = "data4BT_e2e_test"

	ctx := context.Background()

	// Create repository
	repo, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer func() {
		repo.ClearAllData(ctx)
		repo.Close()
	}()

	// Initialize database
	err = repo.CreateTables(ctx)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// Test parser with real CSV data
	parser := parser.NewCSVParser(cfg.Parser)
	
	// Sample Binance CSV data
	csvData := `1609459200000,29374.99,29375.00,29374.98,29375.00,0.05134800,1609459259999,1507.51637800,85,0.02540200,745.13980000,0
1609459260000,29375.00,29375.01,29374.99,29375.01,0.03421100,1609459319999,1005.12345000,67,0.01823400,535.67890000,0
1609459320000,29375.01,29375.02,29375.00,29375.02,0.02156700,1609459379999,633.45678000,45,0.01234500,362.89012000,0`

	klines, validationResult, err := parser.Parse(ctx, []byte(csvData), "BTCUSDT")
	if err != nil {
		t.Fatalf("Failed to parse CSV data: %v", err)
	}

	if !validationResult.Valid {
		t.Fatalf("CSV validation failed: %+v", validationResult)
	}

	if len(klines) != 3 {
		t.Fatalf("Expected 3 klines, got %d", len(klines))
	}

	// Save to database
	err = repo.Save(ctx, klines)
	if err != nil {
		t.Fatalf("Failed to save klines to database: %v", err)
	}

	// Verify data in database
	lastDate, err := repo.GetLastDate(ctx, "BTCUSDT")
	if err != nil {
		t.Fatalf("Failed to get last date: %v", err)
	}

	if lastDate.IsZero() {
		t.Errorf("Expected non-zero last date")
	}

	// Validate the saved data
	testDate := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC) // Date from test data
	result, err := repo.ValidateData(ctx, "BTCUSDT", testDate)
	if err != nil {
		t.Fatalf("Failed to validate saved data: %v", err)
	}

	if result.TotalRows != 3 {
		t.Errorf("Expected 3 rows in database, got %d", result.TotalRows)
	}
}

// TestIntegration_DatabaseConnection tests database connectivity
func TestIntegration_DatabaseConnection(t *testing.T) {
	// Skip if no database available
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration test (set INTEGRATION_TEST=true to run)")
	}

	cfg := config.ClickHouseConfig{
		Hosts:           []string{"localhost:9000"},
		Database:        "data4BT",
		Username:        "default",
		Password:        "",
		DialTimeout:     30 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: 5 * time.Minute,
	}

	repo, err := clickhouse.NewRepository(cfg)
	if err != nil {
		t.Fatalf("Failed to connect to data4BT database: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// Test database creation and table setup
	err = repo.CreateTables(ctx)
	if err != nil {
		t.Fatalf("Failed to create tables in data4BT: %v", err)
	}

	t.Logf("Successfully connected to data4BT database and created tables")
}

func createIntegrationTestKLine(symbol string, openTime time.Time) domain.KLine {
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