package importer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
)

// MockDownloader for testing
type MockDownloader struct {
	shouldFail bool
	data       []byte
}

func (m *MockDownloader) Fetch(ctx context.Context, task domain.DownloadTask) ([]byte, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock download failure")
	}
	return m.data, nil
}

func (m *MockDownloader) GetSymbols(ctx context.Context) ([]string, error) {
	return []string{"BTCUSDT", "ETHUSDT"}, nil
}

func (m *MockDownloader) GetAvailableDates(ctx context.Context, symbol string) ([]time.Time, error) {
	return []time.Time{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}, nil
}

func (m *MockDownloader) GetSymbolTimeline(ctx context.Context, symbol string) (*domain.SymbolTimeline, error) {
	return &domain.SymbolTimeline{Symbol: symbol}, nil
}

func (m *MockDownloader) GetAllSymbolsFromBinance(ctx context.Context) ([]string, error) {
	return []string{"BTCUSDT", "ETHUSDT"}, nil
}

func (m *MockDownloader) ValidateURL(ctx context.Context, url string) error {
	return nil
}

// MockParser for testing
type MockParser struct {
	shouldFail     bool
	returnedKlines []domain.KLine
	validationResult *domain.ValidationResult
}

func (m *MockParser) Parse(ctx context.Context, data []byte, symbol string) ([]domain.KLine, *domain.ValidationResult, error) {
	if m.shouldFail {
		return nil, nil, fmt.Errorf("mock parse failure")
	}
	return m.returnedKlines, m.validationResult, nil
}

func (m *MockParser) ValidateCSV(data []byte) error {
	return nil
}

// MockRepository for testing
type MockRepository struct {
	shouldFail bool
	savedData  []domain.KLine
}

func (m *MockRepository) Save(ctx context.Context, klines []domain.KLine) error {
	if m.shouldFail {
		return fmt.Errorf("mock save failure")
	}
	m.savedData = append(m.savedData, klines...)
	return nil
}

func (m *MockRepository) GetLastDate(ctx context.Context, symbol string) (time.Time, error) {
	return time.Time{}, nil
}

func (m *MockRepository) GetFirstDate(ctx context.Context, symbol string) (time.Time, error) {
	return time.Time{}, nil
}

func (m *MockRepository) CreateTables(ctx context.Context) error {
	return nil
}

func (m *MockRepository) CreateMaterializedViews(ctx context.Context, intervals []string) error {
	return nil
}

func (m *MockRepository) RefreshMaterializedViews(ctx context.Context) error {
	return nil
}

func (m *MockRepository) ValidateData(ctx context.Context, symbol string, date time.Time) (*domain.ValidationResult, error) {
	return &domain.ValidationResult{Valid: true}, nil
}

func (m *MockRepository) ClearAllData(ctx context.Context) error {
	m.savedData = nil
	return nil
}

func (m *MockRepository) Close() error {
	return nil
}

// MockStateManager for testing
type MockStateManager struct {
	states map[string]*domain.ProcessingState
}

func (m *MockStateManager) GetState(symbol string) (*domain.ProcessingState, error) {
	if state, exists := m.states[symbol]; exists {
		return state, nil
	}
	return &domain.ProcessingState{
		Symbol:      symbol,
		LastDate:    time.Time{},
		Processed:   0,
		Failed:      0,
		LastUpdated: time.Now(),
	}, nil
}

func (m *MockStateManager) SaveState(state *domain.ProcessingState) error {
	if m.states == nil {
		m.states = make(map[string]*domain.ProcessingState)
	}
	m.states[state.Symbol] = state
	return nil
}

func (m *MockStateManager) GetAllStates() (map[string]*domain.ProcessingState, error) {
	return m.states, nil
}

func (m *MockStateManager) DeleteState(symbol string) error {
	delete(m.states, symbol)
	return nil
}

func (m *MockStateManager) GetTimeline(symbol string) (*domain.SymbolTimeline, error) {
	return nil, nil
}

func (m *MockStateManager) SaveTimeline(timeline *domain.SymbolTimeline) error {
	return nil
}

func (m *MockStateManager) GetAllTimelines() (map[string]*domain.SymbolTimeline, error) {
	return nil, nil
}

func (m *MockStateManager) DeleteTimeline(symbol string) error {
	return nil
}

func (m *MockStateManager) Backup() error {
	return nil
}

func (m *MockStateManager) Restore(backupPath string) error {
	return nil
}

func (m *MockStateManager) GetAllSymbolProgress() (map[string]*domain.SymbolProgressInfo, error) {
	return nil, nil
}

func (m *MockStateManager) UpdateWorkerState(workerID int, state *domain.WorkerState) error {
	return nil
}

func (m *MockStateManager) GetWorkerState(workerID int) (*domain.WorkerState, error) {
	return nil, nil
}

func (m *MockStateManager) GetAllWorkerStates() (map[int]*domain.WorkerState, error) {
	return nil, nil
}

func (m *MockStateManager) UpdateSymbolProgress(symbol string, progress *domain.SymbolProgressInfo) error {
	return nil
}

func (m *MockStateManager) GetSymbolProgress(symbol string) (*domain.SymbolProgressInfo, error) {
	return nil, nil
}

func (m *MockStateManager) GetIncompleteSymbols() ([]string, error) {
	return nil, nil
}

func TestImporter_ImportData(t *testing.T) {
	cfg := config.ImporterConfig{
		BatchSize:           10,
		BufferSize:         20,
		FlushInterval:      30 * time.Second,
		EnableDeduplication: true,
	}

	testKlines := []domain.KLine{
		createTestKLineForImporter("BTCUSDT", time.Now()),
	}

	tests := []struct {
		name           string
		tasks          []domain.DownloadTask
		shouldFailDownload bool
		shouldFailParse    bool
		shouldFailSave     bool
		expectError        bool
	}{
		{
			name: "Successful import",
			tasks: []domain.DownloadTask{
				{Symbol: "BTCUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			},
			expectError: false,
		},
		{
			name: "Download failure",
			tasks: []domain.DownloadTask{
				{Symbol: "BTCUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			},
			shouldFailDownload: true,
			expectError:        true,
		},
		{
			name: "Parse failure",
			tasks: []domain.DownloadTask{
				{Symbol: "BTCUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			},
			shouldFailParse: true,
			expectError:     true,
		},
		{
			name: "Save failure",
			tasks: []domain.DownloadTask{
				{Symbol: "BTCUSDT", Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			},
			shouldFailSave: true,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDownloader := &MockDownloader{
				shouldFail: tt.shouldFailDownload,
				data:       []byte("1609459200000,29374.99,29375.00,29374.98,29375.00,0.05134800,1609459259999,1507.51637800,85,0.02540200,745.13980000,0"),
			}
			
			mockParser := &MockParser{
				shouldFail:     tt.shouldFailParse,
				returnedKlines: testKlines,
				validationResult: &domain.ValidationResult{
					Valid:       true,
					TotalRows:   1,
					ValidRows:   1,
					InvalidRows: 0,
				},
			}
			
			mockRepository := &MockRepository{
				shouldFail: tt.shouldFailSave,
			}
			
			mockStateManager := &MockStateManager{
				states: make(map[string]*domain.ProcessingState),
			}

			importer := NewImporter(
				cfg,
				mockDownloader,
				mockParser,
				mockRepository,
				mockStateManager,
				nil, // progress reporter
			)

			ctx := context.Background()
			err := importer.ImportData(ctx, tt.tasks)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Verify state was updated on success
			if !tt.expectError && len(tt.tasks) > 0 {
				state, _ := mockStateManager.GetState(tt.tasks[0].Symbol)
				if state.Processed != 1 {
					t.Errorf("Expected processed count 1, got %d", state.Processed)
				}
			}
		})
	}
}

func TestImporter_deduplicateKLines(t *testing.T) {
	cfg := config.ImporterConfig{EnableDeduplication: true}
	importer := NewImporter(cfg, nil, nil, nil, nil, nil)

	baseTime := time.Now()
	klines := []domain.KLine{
		createTestKLineForImporter("BTCUSDT", baseTime),
		createTestKLineForImporter("BTCUSDT", baseTime), // Duplicate
		createTestKLineForImporter("BTCUSDT", baseTime.Add(time.Minute)),
	}

	deduplicated := importer.deduplicateKLines(klines)

	if len(deduplicated) != 2 {
		t.Errorf("Expected 2 deduplicated klines, got %d", len(deduplicated))
	}
}

func createTestKLineForImporter(symbol string, openTime time.Time) domain.KLine {
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
