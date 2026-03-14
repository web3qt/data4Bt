package windowimport

import (
	"testing"
	"time"
)

func TestDiffDailySecondCounts(t *testing.T) {
	startDay := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	endDay := time.Date(2026, 2, 3, 0, 0, 0, 0, time.UTC)

	stored := map[string]map[string]int{
		"AAAUSDT": {
			"2026-02-01": 10,
			"2026-02-02": 5,
		},
		"BBBUSDT": {
			"2026-02-02": 7,
		},
	}
	official := map[string]map[string]int{
		"AAAUSDT": {
			"2026-02-01": 10,
			"2026-02-02": 8,
		},
		"BBBUSDT": {
			"2026-02-02": 0,
			"2026-02-03": 4,
		},
	}

	mismatches := DiffDailySecondCounts([]string{"AAAUSDT", "BBBUSDT"}, startDay, endDay, stored, official)
	if len(mismatches) != 3 {
		t.Fatalf("Expected 3 mismatches, got %d", len(mismatches))
	}

	check := func(idx int, symbol, day string, storedCount, officialCount int) {
		t.Helper()
		if mismatches[idx].Symbol != symbol {
			t.Fatalf("Expected mismatch %d symbol %s, got %s", idx, symbol, mismatches[idx].Symbol)
		}
		if got := mismatches[idx].Day.Format("2006-01-02"); got != day {
			t.Fatalf("Expected mismatch %d day %s, got %s", idx, day, got)
		}
		if mismatches[idx].StoredCount != storedCount || mismatches[idx].OfficialCount != officialCount {
			t.Fatalf(
				"Expected mismatch %d counts stored=%d official=%d, got stored=%d official=%d",
				idx,
				storedCount,
				officialCount,
				mismatches[idx].StoredCount,
				mismatches[idx].OfficialCount,
			)
		}
	}

	check(0, "AAAUSDT", "2026-02-02", 5, 8)
	check(1, "BBBUSDT", "2026-02-02", 7, 0)
	check(2, "BBBUSDT", "2026-02-03", 0, 4)
}

func TestBuildRepairTasks(t *testing.T) {
	mismatches := []DailyCountMismatch{
		{
			Symbol:        "BBBUSDT",
			Day:           time.Date(2026, 2, 3, 0, 0, 0, 0, time.UTC),
			StoredCount:   0,
			OfficialCount: 4,
		},
		{
			Symbol:        "AAAUSDT",
			Day:           time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC),
			StoredCount:   5,
			OfficialCount: 8,
		},
		{
			Symbol:        "AAAUSDT",
			Day:           time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			StoredCount:   0,
			OfficialCount: 10,
		},
	}

	tasks := BuildRepairTasks(
		mismatches,
		"https://data.binance.vision",
		"/data/futures/um/daily/aggTrades",
	)
	if len(tasks) != 2 {
		t.Fatalf("Expected 2 symbol task groups, got %d", len(tasks))
	}

	if tasks[0].Symbol != "AAAUSDT" {
		t.Fatalf("Expected first task group to be AAAUSDT, got %s", tasks[0].Symbol)
	}
	if len(tasks[0].Tasks) != 2 {
		t.Fatalf("Expected AAAUSDT to have 2 repair tasks, got %d", len(tasks[0].Tasks))
	}
	if got := tasks[0].Tasks[0].Date.Format("2006-01-02"); got != "2026-02-01" {
		t.Fatalf("Expected first AAAUSDT repair day 2026-02-01, got %s", got)
	}
	if got := tasks[0].Tasks[1].Date.Format("2006-01-02"); got != "2026-02-02" {
		t.Fatalf("Expected second AAAUSDT repair day 2026-02-02, got %s", got)
	}

	if tasks[1].Symbol != "BBBUSDT" {
		t.Fatalf("Expected second task group to be BBBUSDT, got %s", tasks[1].Symbol)
	}
	if len(tasks[1].Tasks) != 1 {
		t.Fatalf("Expected BBBUSDT to have 1 repair task, got %d", len(tasks[1].Tasks))
	}
	if got := tasks[1].Tasks[0].URL; got != "https://data.binance.vision/data/futures/um/daily/aggTrades/BBBUSDT/BBBUSDT-aggTrades-2026-02-03.zip" {
		t.Fatalf("Unexpected repair URL: %s", got)
	}
}
