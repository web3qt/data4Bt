package windowimport

import (
	"testing"
	"time"

	"binance-data-loader/internal/config"
	"binance-data-loader/internal/domain"
)

func TestBuildDailyArchiveURL(t *testing.T) {
	got := BuildDailyArchiveURL(
		"https://data.binance.vision",
		"/data/futures/um/daily/aggTrades",
		"BTCUSDT",
		time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC),
	)

	want := "https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-03-09.zip"
	if got != want {
		t.Fatalf("Expected %s, got %s", want, got)
	}
}

func TestBuildLatestWindowTasks(t *testing.T) {
	end := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	tasks := BuildLatestWindowTasks(
		[]string{"BTCUSDT", "ETHUSDT"},
		15,
		end,
		"https://data.binance.vision",
		"/data/futures/um/daily/aggTrades",
	)

	if len(tasks) != 2 {
		t.Fatalf("Expected 2 symbol task groups, got %d", len(tasks))
	}

	if len(tasks[0].Tasks) != 15 {
		t.Fatalf("Expected 15 daily tasks per symbol, got %d", len(tasks[0].Tasks))
	}

	first := tasks[0].Tasks[0].Date.Format("2006-01-02")
	last := tasks[0].Tasks[len(tasks[0].Tasks)-1].Date.Format("2006-01-02")
	if first != "2026-02-23" || last != "2026-03-09" {
		t.Fatalf("Unexpected task range: %s -> %s", first, last)
	}

	url := tasks[0].Tasks[0].URL
	if url != "https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-02-23.zip" {
		t.Fatalf("Unexpected first task URL: %s", url)
	}
}

func TestResolveWindowDays(t *testing.T) {
	cfg := &config.Config{}
	if got := resolveWindowDays(cfg); got != defaultLatestWindowDays {
		t.Fatalf("Expected default window days %d, got %d", defaultLatestWindowDays, got)
	}

	cfg.Scheduler.BatchDays = 5
	if got := resolveWindowDays(cfg); got != 5 {
		t.Fatalf("Expected configured window days 5, got %d", got)
	}
}

func TestFilterRemainingSymbols(t *testing.T) {
	symbols := []string{"AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT"}
	progress := map[string]*domain.SymbolProgressInfo{
		"AAAUSDT": {
			Symbol:          "AAAUSDT",
			Status:          "completed",
			Progress:        100,
			CompletedMonths: 15,
			TotalMonths:     15,
		},
		"BBBUSDT": {
			Symbol:          "BBBUSDT",
			Status:          "running",
			Progress:        60,
			CompletedMonths: 9,
			TotalMonths:     15,
		},
		"CCCUSDT": {
			Symbol:          "CCCUSDT",
			Status:          "failed",
			Progress:        20,
			CompletedMonths: 3,
			TotalMonths:     15,
		},
	}

	remaining := filterRemainingSymbols(symbols, progress)
	if len(remaining) != 3 {
		t.Fatalf("Expected 3 remaining symbols, got %d", len(remaining))
	}

	expected := []string{"BBBUSDT", "CCCUSDT", "DDDUSDT"}
	for idx, symbol := range expected {
		if remaining[idx] != symbol {
			t.Fatalf("Expected remaining[%d] to be %s, got %s", idx, symbol, remaining[idx])
		}
	}
}

func TestBuildRankingWindow(t *testing.T) {
	latestAvailableDay := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)

	startDay, endDay := BuildRankingWindow(latestAvailableDay, 7)

	if got := startDay.Format("2006-01-02"); got != "2026-03-03" {
		t.Fatalf("Expected ranking start day 2026-03-03, got %s", got)
	}
	if got := endDay.Format("2006-01-02"); got != "2026-03-09" {
		t.Fatalf("Expected ranking end day 2026-03-09, got %s", got)
	}
}

func TestBuildBackfillWindow(t *testing.T) {
	now := time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)

	startDay, endDay := BuildBackfillWindow(now, 90)

	if got := startDay.Format("2006-01-02"); got != "2025-12-11" {
		t.Fatalf("Expected backfill start day 2025-12-11, got %s", got)
	}
	if got := endDay.Format("2006-01-02"); got != "2026-03-10" {
		t.Fatalf("Expected backfill end day 2026-03-10, got %s", got)
	}
}

func TestBuildMissingWindowTasks(t *testing.T) {
	startDay := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)
	endDay := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	ranges := map[string]*domain.SymbolDateRange{
		"AAAUSDT": {
			Symbol:    "AAAUSDT",
			FirstDate: time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC),
			LastDate:  time.Date(2026, 3, 7, 23, 59, 59, 0, time.UTC),
			HasData:   true,
		},
	}

	tasks := BuildMissingWindowTasks(
		[]string{"AAAUSDT", "BBBUSDT"},
		startDay,
		endDay,
		"https://data.binance.vision",
		"/data/futures/um/daily/aggTrades",
		ranges,
	)

	if len(tasks) != 2 {
		t.Fatalf("Expected 2 symbol task groups, got %d", len(tasks))
	}

	if len(tasks[0].Tasks) != 4 {
		t.Fatalf("Expected AAAUSDT to have 4 missing daily tasks, got %d", len(tasks[0].Tasks))
	}
	if len(tasks[1].Tasks) != 7 {
		t.Fatalf("Expected BBBUSDT to have 7 missing daily tasks, got %d", len(tasks[1].Tasks))
	}

	wantDates := []string{"2026-03-03", "2026-03-04", "2026-03-08", "2026-03-09"}
	for idx, want := range wantDates {
		got := tasks[0].Tasks[idx].Date.Format("2006-01-02")
		if got != want {
			t.Fatalf("Expected AAAUSDT missing task %d to be %s, got %s", idx, want, got)
		}
	}
}

func TestBuildMissingWindowTasksWithCoveredDays(t *testing.T) {
	startDay := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)
	endDay := time.Date(2026, 3, 6, 0, 0, 0, 0, time.UTC)
	coveredDays := map[string]map[string]struct{}{
		"AAAUSDT": {
			"2026-03-03": {},
			"2026-03-05": {},
		},
	}

	tasks := BuildMissingWindowTasksWithCoveredDays(
		[]string{"AAAUSDT"},
		startDay,
		endDay,
		"https://data.binance.vision",
		"/data/futures/um/daily/aggTrades",
		coveredDays,
		nil,
	)

	if len(tasks) != 1 {
		t.Fatalf("Expected 1 symbol task group, got %d", len(tasks))
	}

	if len(tasks[0].Tasks) != 3 {
		t.Fatalf("Expected boundary days to be rebuilt, got %d tasks", len(tasks[0].Tasks))
	}

	wantDates := []string{"2026-03-03", "2026-03-04", "2026-03-06"}
	for idx, want := range wantDates {
		got := tasks[0].Tasks[idx].Date.Format("2006-01-02")
		if got != want {
			t.Fatalf("Expected missing task %d to be %s, got %s", idx, want, got)
		}
	}
}

func TestMergeWindowCoveredDaysPrefersExactStateCoverage(t *testing.T) {
	startDay := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)
	endDay := time.Date(2026, 3, 6, 0, 0, 0, 0, time.UTC)

	dbCoveredDays := map[string]map[string]struct{}{
		"AAAUSDT": {
			"2026-03-03": {},
			"2026-03-04": {},
			"2026-03-05": {},
			"2026-03-06": {},
		},
		"BBBUSDT": {
			"2026-03-03": {},
			"2026-03-04": {},
			"2026-03-05": {},
			"2026-03-06": {},
		},
	}

	states := map[string]*domain.ProcessingState{
		"AAAUSDT": {
			Symbol:             "AAAUSDT",
			Processed:          2,
			CompletedTaskDates: []string{"2026-03-03", "2026-03-05"},
		},
	}

	coveredDays, exactCoverageSymbols := MergeWindowCoveredDays(
		[]string{"AAAUSDT", "BBBUSDT"},
		startDay,
		endDay,
		states,
		dbCoveredDays,
	)

	if _, exists := exactCoverageSymbols["AAAUSDT"]; !exists {
		t.Fatalf("Expected AAAUSDT to use exact state coverage")
	}
	if _, exists := exactCoverageSymbols["BBBUSDT"]; exists {
		t.Fatalf("Did not expect BBBUSDT to use exact state coverage")
	}

	aaaDays := coveredDays["AAAUSDT"]
	if len(aaaDays) != 2 {
		t.Fatalf("Expected AAAUSDT exact coverage to keep 2 completed dates, got %d", len(aaaDays))
	}
	if _, exists := aaaDays["2026-03-03"]; !exists {
		t.Fatalf("Expected AAAUSDT exact coverage to include 2026-03-03")
	}
	if _, exists := aaaDays["2026-03-05"]; !exists {
		t.Fatalf("Expected AAAUSDT exact coverage to include 2026-03-05")
	}

	bbbDays := coveredDays["BBBUSDT"]
	if len(bbbDays) != 4 {
		t.Fatalf("Expected BBBUSDT to fall back to DB daily coverage, got %d dates", len(bbbDays))
	}
}

func TestMergeWindowCoveredDaysTreatsLegacyIncompleteStateAsUnknown(t *testing.T) {
	startDay := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)
	endDay := time.Date(2026, 3, 6, 0, 0, 0, 0, time.UTC)

	dbCoveredDays := map[string]map[string]struct{}{
		"AAAUSDT": {
			"2026-03-03": {},
			"2026-03-04": {},
			"2026-03-05": {},
			"2026-03-06": {},
		},
	}

	states := map[string]*domain.ProcessingState{
		"AAAUSDT": {
			Symbol:    "AAAUSDT",
			LastDate:  time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC),
			Processed: 3,
			Status:    "failed",
		},
	}

	coveredDays, exactCoverageSymbols := MergeWindowCoveredDays(
		[]string{"AAAUSDT"},
		startDay,
		endDay,
		states,
		dbCoveredDays,
	)

	if len(coveredDays["AAAUSDT"]) != 0 {
		t.Fatalf("Expected legacy incomplete state to force a full rebuild of the window")
	}
	if _, exists := exactCoverageSymbols["AAAUSDT"]; exists {
		t.Fatalf("Did not expect legacy incomplete state to be treated as exact coverage")
	}
}

func TestBuildMissingWindowTasksWithCoveredDaysSkipsExactBoundaryDays(t *testing.T) {
	startDay := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)
	endDay := time.Date(2026, 3, 6, 0, 0, 0, 0, time.UTC)
	coveredDays := map[string]map[string]struct{}{
		"AAAUSDT": {
			"2026-03-03": {},
			"2026-03-05": {},
			"2026-03-06": {},
		},
	}
	exactCoverageSymbols := map[string]struct{}{
		"AAAUSDT": {},
	}

	tasks := BuildMissingWindowTasksWithCoveredDays(
		[]string{"AAAUSDT"},
		startDay,
		endDay,
		"https://data.binance.vision",
		"/data/futures/um/daily/aggTrades",
		coveredDays,
		exactCoverageSymbols,
	)

	if len(tasks) != 1 {
		t.Fatalf("Expected 1 symbol task group, got %d", len(tasks))
	}

	if len(tasks[0].Tasks) != 1 {
		t.Fatalf("Expected 1 missing daily task, got %d", len(tasks[0].Tasks))
	}

	wantDates := []string{"2026-03-04"}
	for idx, want := range wantDates {
		got := tasks[0].Tasks[idx].Date.Format("2006-01-02")
		if got != want {
			t.Fatalf("Expected missing task %d to be %s, got %s", idx, want, got)
		}
	}
}

func TestResolveConfiguredSymbols(t *testing.T) {
	cfg := &config.Config{}
	cfg.Binance.ExplicitSymbols = []string{
		" BTCUSDT ",
		"ETHUSDT",
		"",
		"BTCUSDT",
		"  SOLUSDT",
	}

	got := ResolveConfiguredSymbols(cfg)
	want := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}

	if len(got) != len(want) {
		t.Fatalf("Expected %d symbols, got %d", len(want), len(got))
	}

	for idx, symbol := range want {
		if got[idx] != symbol {
			t.Fatalf("Expected symbol %d to be %s, got %s", idx, symbol, got[idx])
		}
	}
}

func TestResolveWindowRangeUsesConfiguredDates(t *testing.T) {
	cfg := &config.Config{}
	cfg.Scheduler.StartDate = "2026-02-09"
	cfg.Scheduler.EndDate = "2026-03-09"
	cfg.Scheduler.BatchDays = 999

	startDay, endDay, err := ResolveWindowRange(cfg, time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if got := startDay.Format("2006-01-02"); got != "2026-02-09" {
		t.Fatalf("Expected configured start day 2026-02-09, got %s", got)
	}
	if got := endDay.Format("2006-01-02"); got != "2026-03-09" {
		t.Fatalf("Expected configured end day 2026-03-09, got %s", got)
	}
}
