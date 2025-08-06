package main

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
	"binance-data-loader/pkg/binance"
	"binance-data-loader/pkg/clickhouse"
	"binance-data-loader/pkg/importer"
	"binance-data-loader/pkg/parser"
	"binance-data-loader/internal/state"
)

// TestE2E_CompleteDataFlow tests the complete data pipeline end-to-end
func TestE2E_CompleteDataFlow(t *testing.T) {
	// Load test configuration
	cfg, err := config.Load("./test/config_test.yml")
	if err != nil {
		t.Fatalf("Failed to load test config: %v", err)
	}

	// Override for testing
	cfg.Database.ClickHouse.Database = "data4BT_e2e_test"
	cfg.Scheduler.MaxConcurrentSymbols = 1

	ctx := context.Background()

	// Test Phase 1: Component Initialization
	t.Run("InitializeComponents", func(t *testing.T) {
		components, err := initializeTestComponents(cfg)
		if err != nil {
			t.Fatalf("Failed to initialize components: %v", err)
		}
		defer components.cleanup()

		// Test database connection and table creation
		err = components.repository.CreateTables(ctx)
		if err != nil {
			t.Fatalf("Failed to create tables: %v", err)
		}

		log.Printf("✅ Components initialized successfully")
	})

	// Test Phase 2: Data Source Connectivity
	t.Run("DataSourceConnectivity", func(t *testing.T) {
		downloader := binance.NewBinanceDownloader(cfg.Binance, cfg.Downloader)
		
		// Test symbol discovery
		symbols, err := downloader.GetSymbols(ctx)
		if err != nil {
			t.Fatalf("Failed to get symbols: %v", err)
		}

		if len(symbols) == 0 {
			t.Fatalf("No symbols returned from Binance")
		}

		log.Printf("✅ Found %d symbols from Binance", len(symbols))

		// Test data availability for a known symbol
		testSymbol := "BTCUSDT"
		availableDates, err := downloader.GetAvailableDates(ctx, testSymbol)
		if err != nil {
			t.Logf("⚠️  Could not get available dates for %s: %v", testSymbol, err)
		} else {
			log.Printf("✅ Found %d available months for %s", len(availableDates), testSymbol)
		}
	})

	// Test Phase 3: Complete Data Pipeline
	t.Run("CompleteDataPipeline", func(t *testing.T) {
		components, err := initializeTestComponents(cfg)
		if err != nil {
			t.Fatalf("Failed to initialize components: %v", err)
		}
		defer components.cleanup()

		// Initialize database
		err = components.repository.CreateTables(ctx)
		if err != nil {
			t.Fatalf("Failed to create tables: %v", err)
		}

		// Test with sample data (simulate successful download)
		testData := []byte(`1609459200000,29374.99,29375.00,29374.98,29375.00,0.05134800,1609459259999,1507.51637800,85,0.02540200,745.13980000,0
1609459260000,29375.00,29375.01,29374.99,29375.01,0.03421100,1609459319999,1005.12345000,67,0.01823400,535.67890000,0`)

		// Parse data
		klines, validationResult, err := components.parser.Parse(ctx, testData, "BTCUSDT")
		if err != nil {
			t.Fatalf("Failed to parse test data: %v", err)
		}

		if !validationResult.Valid {
			t.Fatalf("Test data validation failed: %+v", validationResult)
		}

		// Save to database
		err = components.repository.Save(ctx, klines)
		if err != nil {
			t.Fatalf("Failed to save to database: %v", err)
		}

		// Verify data was saved
		lastDate, err := components.repository.GetLastDate(ctx, "BTCUSDT")
		if err != nil {
			t.Fatalf("Failed to get last date: %v", err)
		}

		if lastDate.IsZero() {
			t.Errorf("Expected non-zero last date after saving data")
		}

		log.Printf("✅ Complete data pipeline test successful")
		log.Printf("   - Parsed %d records", len(klines))
		log.Printf("   - Saved to data4BT database")
		log.Printf("   - Last date: %s", lastDate.Format("2006-01-02"))
	})
}

// Test Phase 4: Database Operations
func TestE2E_DatabaseOperations(t *testing.T) {
	cfg, err := config.Load("./test/config_test.yml")
	if err != nil {
		t.Fatalf("Failed to load test config: %v", err)
	}

	cfg.Database.ClickHouse.Database = "data4BT_ops_test"
	ctx := context.Background()

	repo, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer func() {
		repo.ClearAllData(ctx)
		repo.Close()
	}()

	// Test table creation
	err = repo.CreateTables(ctx)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// Test materialized views
	intervals := []string{"5m", "1h"}
	err = repo.CreateMaterializedViews(ctx, intervals)
	if err != nil {
		t.Fatalf("Failed to create materialized views: %v", err)
	}

	log.Printf("✅ Database operations test successful")
}

type testComponents struct {
	downloader   *binance.BinanceDownloader
	parser       *parser.CSVParser
	repository   *clickhouse.Repository
	stateManager *state.FileStateManager
	importer     *importer.Importer
}

func (c *testComponents) cleanup() {
	if c.repository != nil {
		ctx := context.Background()
		c.repository.ClearAllData(ctx)
		c.repository.Close()
	}
	if c.importer != nil {
		c.importer.Close()
	}
}

func initializeTestComponents(cfg *config.Config) (*testComponents, error) {
	// Create downloader
	downloader := binance.NewBinanceDownloader(cfg.Binance, cfg.Downloader)

	// Create parser
	parser := parser.NewCSVParser(cfg.Parser)

	// Create ClickHouse repository
	repository, err := clickhouse.NewRepository(cfg.Database.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("failed to create repository: %w", err)
	}

	// Create state manager
	stateManager, err := state.NewFileStateManager(cfg.State)
	if err != nil {
		return nil, fmt.Errorf("failed to create state manager: %w", err)
	}

	// Create importer
	importer := importer.NewImporter(
		cfg.Importer,
		downloader,
		parser,
		repository,
		stateManager,
		nil, // No progress reporter for tests
	)

	return &testComponents{
		downloader:   downloader,
		parser:       parser,
		repository:   repository,
		stateManager: stateManager,
		importer:     importer,
	}, nil
}