package aggtradeparser

import (
	"context"
	"testing"
)

func TestAggTradeCSVParser_Parse(t *testing.T) {
	parser := NewAggTradeCSVParser()

	csvData := `Aggregate tradeId,Price,Quantity,First tradeId,Last tradeId,Timestamp,Was the buyer the maker
26129,100.0,0.5,1,1,1700000000123,true
26130,101.0,0.2,2,3,1700000000456,false

26131,99.0,1.0,4,4,1700000001123,false
`

	klines, result, err := parser.Parse(context.Background(), []byte(csvData), "BTCUSDT")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(klines) != 2 {
		t.Fatalf("Expected 2 klines, got %d", len(klines))
	}

	if !result.Valid {
		t.Fatalf("Expected validation result to be valid, got %+v", result)
	}

	first := klines[0]
	if first.Interval != "1s" {
		t.Fatalf("Expected interval 1s, got %s", first.Interval)
	}
	if first.OpenPrice != 100.0 || first.HighPrice != 101.0 || first.LowPrice != 100.0 || first.ClosePrice != 101.0 {
		t.Fatalf("Unexpected OHLC for first kline: %+v", first)
	}
	if first.Volume != 0.7 {
		t.Fatalf("Expected first volume 0.7, got %f", first.Volume)
	}
	if first.NumberOfTrades != 3 {
		t.Fatalf("Expected first number_of_trades 3, got %d", first.NumberOfTrades)
	}
	if first.TakerBuyBaseVolume != 0.2 {
		t.Fatalf("Expected first taker buy base volume 0.2, got %f", first.TakerBuyBaseVolume)
	}

	second := klines[1]
	if second.OpenPrice != 99.0 || second.ClosePrice != 99.0 {
		t.Fatalf("Unexpected second kline: %+v", second)
	}
	if second.Volume != 1.0 || second.TakerBuyQuoteVolume != 99.0 {
		t.Fatalf("Unexpected second aggregates: %+v", second)
	}
}
