package logger

import (
	"testing"
)

func TestLoggerInitialization(t *testing.T) {
	// Test that we can import the package
	logger := GetLogger("test")
	if logger.GetLevel().String() == "" {
		t.Error("Expected logger to have a valid level")
	}
}

func TestGetLogger(t *testing.T) {
	logger1 := GetLogger("test1")
	logger2 := GetLogger("test2")
	
	// Should be able to get loggers - just check they return valid loggers
	// We can't directly compare zerolog.Logger instances, so we just ensure they're usable
	logger1.Info().Msg("test1 message")
	logger2.Info().Msg("test2 message")
}

func TestLogFunctions(t *testing.T) {
	logger := GetLogger("test")
	
	// Test that we can call log functions without panicking
	logger.Debug().Msg("Debug message")
	logger.Info().Msg("Info message")
	logger.Warn().Msg("Warn message")
	logger.Error().Msg("Error message")
	
	// If we get here without panicking, the tests pass
}
