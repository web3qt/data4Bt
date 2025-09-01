package domain

import (
	"fmt"
	"time"
)

// DataNotAvailableError 表示数据不存在的错误（可跳过的错误）
// 这种错误通常发生在：
// - 币安返回404（某月数据不存在）
// - 交易对在某个时间段尚未上市
// - 历史数据缺失等正常情况
type DataNotAvailableError struct {
	Symbol string
	Date   time.Time
	Reason string
}

func (e *DataNotAvailableError) Error() string {
	if !e.Date.IsZero() {
		return fmt.Sprintf("data not available for %s %s: %s", 
			e.Symbol, e.Date.Format("2006-01"), e.Reason)
	}
	return fmt.Sprintf("data not available for %s: %s", e.Symbol, e.Reason)
}

// NewDataNotAvailableError 创建数据不存在错误
func NewDataNotAvailableError(symbol string, date time.Time, reason string) *DataNotAvailableError {
	return &DataNotAvailableError{
		Symbol: symbol,
		Date:   date,
		Reason: reason,
	}
}

// IsDataNotAvailableError 检查错误是否为数据不存在错误（可跳过）
func IsDataNotAvailableError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*DataNotAvailableError)
	return ok
}

// IsRecoverableError 检查错误是否为可恢复错误（应该跳过而不是终止程序）
func IsRecoverableError(err error) bool {
	if err == nil {
		return false
	}
	
	// 目前只有数据不存在错误是可恢复的
	// 后续可以扩展其他可恢复错误类型
	return IsDataNotAvailableError(err)
}