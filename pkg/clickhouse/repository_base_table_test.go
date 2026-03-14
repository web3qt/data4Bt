package clickhouse

import (
	"errors"
	"testing"
)

func TestNormalizeBaseTable(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "default_empty",
			input: "",
			want:  "klines_1m",
		},
		{
			name:  "explicit_1s",
			input: "klines_1s",
			want:  "klines_1s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeBaseTable(tt.input); got != tt.want {
				t.Fatalf("Expected %s, got %s", tt.want, got)
			}
		})
	}
}

func TestShouldFallbackToDefaultDatabase(t *testing.T) {
	if !shouldFallbackToDefaultDatabase(errors.New("code: 81, message: Database data4BT_futures_um_1s does not exist")) {
		t.Fatal("Expected missing-database error to trigger fallback")
	}

	if shouldFallbackToDefaultDatabase(errors.New("dial tcp 127.0.0.1:9000: connect: connection refused")) {
		t.Fatal("Did not expect network error to trigger fallback")
	}
}
