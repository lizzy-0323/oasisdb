package logger

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// setupTestLogger creates a logger with observer for testing
func setupTestLogger() (*zap.Logger, *observer.ObservedLogs) {
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)
	return logger, recorded
}

// TestDebugLogging tests the Debug function
func TestDebugLogging(t *testing.T) {
	// Save original logger
	originalLogger := defaultLogger
	defer func() { defaultLogger = originalLogger }()

	// Setup test logger with debug level
	core, recorded := observer.New(zapcore.DebugLevel)
	defaultLogger = zap.New(core)

	// Test debug logging
	Debug("test debug message", "key", "value")

	// Check recorded logs
	logs := recorded.All()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	entry := logs[0]
	if entry.Level != zapcore.DebugLevel {
		t.Errorf("Expected debug level, got %v", entry.Level)
	}
	if entry.Message != "test debug message" {
		t.Errorf("Expected 'test debug message', got '%s'", entry.Message)
	}
	if len(entry.Context) != 1 || entry.Context[0].Key != "key" || entry.Context[0].String != "value" {
		t.Errorf("Expected context field 'key'='value', got %v", entry.Context)
	}
}

// TestInfoLogging tests the Info function
func TestInfoLogging(t *testing.T) {
	// Save original logger
	originalLogger := defaultLogger
	defer func() { defaultLogger = originalLogger }()

	// Setup test logger
	core, recorded := observer.New(zapcore.InfoLevel)
	defaultLogger = zap.New(core)

	// Test info logging
	Info("test info message", "key1", "value1", "key2", 42)

	// Check recorded logs
	logs := recorded.All()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	entry := logs[0]
	if entry.Level != zapcore.InfoLevel {
		t.Errorf("Expected info level, got %v", entry.Level)
	}
	if entry.Message != "test info message" {
		t.Errorf("Expected 'test info message', got '%s'", entry.Message)
	}
	if len(entry.Context) != 2 {
		t.Errorf("Expected 2 context fields, got %d", len(entry.Context))
	}
}

// TestWarnLogging tests the Warn function
func TestWarnLogging(t *testing.T) {
	// Save original logger
	originalLogger := defaultLogger
	defer func() { defaultLogger = originalLogger }()

	// Setup test logger
	core, recorded := observer.New(zapcore.WarnLevel)
	defaultLogger = zap.New(core)

	// Test warn logging
	Warn("test warning message")

	// Check recorded logs
	logs := recorded.All()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	entry := logs[0]
	if entry.Level != zapcore.WarnLevel {
		t.Errorf("Expected warn level, got %v", entry.Level)
	}
	if entry.Message != "test warning message" {
		t.Errorf("Expected 'test warning message', got '%s'", entry.Message)
	}
}

// TestErrorLogging tests the Error function
func TestErrorLogging(t *testing.T) {
	// Save original logger
	originalLogger := defaultLogger
	defer func() { defaultLogger = originalLogger }()

	// Setup test logger
	core, recorded := observer.New(zapcore.ErrorLevel)
	defaultLogger = zap.New(core)

	// Test error logging
	Error("test error message", "error", "something went wrong")

	// Check recorded logs
	logs := recorded.All()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	entry := logs[0]
	if entry.Level != zapcore.ErrorLevel {
		t.Errorf("Expected error level, got %v", entry.Level)
	}
	if entry.Message != "test error message" {
		t.Errorf("Expected 'test error message', got '%s'", entry.Message)
	}
}

// TestWithMethod tests the With function for creating child loggers
func TestWithMethod(t *testing.T) {
	// Save original logger
	originalLogger := defaultLogger
	defer func() { defaultLogger = originalLogger }()

	// Setup test logger
	core, recorded := observer.New(zapcore.InfoLevel)
	defaultLogger = zap.New(core)

	// Test With method
	childLogger := With("service", "test", "version", "1.0")
	childLogger.Info("test message with context")

	// Check recorded logs
	logs := recorded.All()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	entry := logs[0]
	if entry.Level != zapcore.InfoLevel {
		t.Errorf("Expected info level, got %v", entry.Level)
	}
	if entry.Message != "test message with context" {
		t.Errorf("Expected 'test message with context', got '%s'", entry.Message)
	}

	// Check that the With fields are included
	contextFields := make(map[string]interface{})
	for _, field := range entry.Context {
		switch field.Type {
		case zapcore.StringType:
			contextFields[field.Key] = field.String
		}
	}

	if contextFields["service"] != "test" {
		t.Errorf("Expected service field to be 'test', got '%v'", contextFields["service"])
	}
	if contextFields["version"] != "1.0" {
		t.Errorf("Expected version field to be '1.0', got '%v'", contextFields["version"])
	}
}

// TestWithMethodChaining tests chaining With calls
func TestWithMethodChaining(t *testing.T) {
	// Save original logger
	originalLogger := defaultLogger
	defer func() { defaultLogger = originalLogger }()

	// Setup test logger
	core, recorded := observer.New(zapcore.InfoLevel)
	defaultLogger = zap.New(core)

	// Test chaining With calls
	logger1 := With("service", "auth")
	logger2 := logger1.With("user", "john")
	logger2.Info("user authenticated")

	// Check recorded logs
	logs := recorded.All()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	entry := logs[0]
	if len(entry.Context) < 2 {
		t.Errorf("Expected at least 2 context fields, got %d", len(entry.Context))
	}

	// Check that both fields are present
	contextFields := make(map[string]interface{})
	for _, field := range entry.Context {
		if field.Type == zapcore.StringType {
			contextFields[field.Key] = field.String
		}
	}

	if contextFields["service"] != "auth" {
		t.Errorf("Expected service field to be 'auth', got '%v'", contextFields["service"])
	}
	if contextFields["user"] != "john" {
		t.Errorf("Expected user field to be 'john', got '%v'", contextFields["user"])
	}
}

// TestDefaultLoggerInitialization tests that the default logger is properly initialized
func TestDefaultLoggerInitialization(t *testing.T) {
	if defaultLogger == nil {
		t.Error("Default logger should not be nil after package initialization")
	}

	// Test that we can log without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Logging should not panic, but got: %v", r)
		}
	}()

	// These should not panic
	Info("test initialization")
	Debug("test debug")
	Warn("test warn")
	Error("test error")
}

// TestLogLevels tests that log levels work correctly
func TestLogLevels(t *testing.T) {
	// Save original logger
	originalLogger := defaultLogger
	defer func() { defaultLogger = originalLogger }()

	tests := []struct {
		name      string
		level     zapcore.Level
		logFunc   func(string, ...interface{})
		shouldLog bool
	}{
		{"Debug with Info level", zapcore.InfoLevel, Debug, false},
		{"Info with Info level", zapcore.InfoLevel, Info, true},
		{"Warn with Info level", zapcore.InfoLevel, Warn, true},
		{"Error with Info level", zapcore.InfoLevel, Error, true},
		{"Debug with Debug level", zapcore.DebugLevel, Debug, true},
		{"Info with Warn level", zapcore.WarnLevel, Info, false},
		{"Warn with Warn level", zapcore.WarnLevel, Warn, true},
		{"Error with Warn level", zapcore.WarnLevel, Error, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test logger with specific level
			core, recorded := observer.New(tt.level)
			defaultLogger = zap.New(core)

			// Call the log function
			tt.logFunc("test message")

			// Check if log was recorded
			logs := recorded.All()
			if tt.shouldLog && len(logs) == 0 {
				t.Errorf("Expected log to be recorded, but none found")
			}
			if !tt.shouldLog && len(logs) > 0 {
				t.Errorf("Expected no log to be recorded, but found %d", len(logs))
			}
		})
	}
}

// TestFieldTypes tests logging with different field types
func TestFieldTypes(t *testing.T) {
	// Save original logger
	originalLogger := defaultLogger
	defer func() { defaultLogger = originalLogger }()

	// Setup test logger
	core, recorded := observer.New(zapcore.InfoLevel)
	defaultLogger = zap.New(core)

	// Test various field types
	Info("test message",
		"string", "test",
		"int", 42,
		"float", 3.14,
		"bool", true,
		"nil", nil,
	)

	// Check recorded logs
	logs := recorded.All()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	entry := logs[0]
	if len(entry.Context) != 5 {
		t.Errorf("Expected 5 context fields, got %d", len(entry.Context))
	}

	// Verify field types are handled correctly
	contextFields := make(map[string]zapcore.Field)
	for _, field := range entry.Context {
		contextFields[field.Key] = field
	}

	if contextFields["string"].String != "test" {
		t.Errorf("Expected string field to be 'test', got '%s'", contextFields["string"].String)
	}
	if contextFields["int"].Integer != 42 {
		t.Errorf("Expected int field to be 42, got %d", contextFields["int"].Integer)
	}
	if contextFields["bool"].Integer != 1 { // zap stores bool as 1/0
		t.Errorf("Expected bool field to be 1, got %d", contextFields["bool"].Integer)
	}
}

// TestConcurrentLogging tests that logging is safe for concurrent use
func TestConcurrentLogging(t *testing.T) {
	// Save original logger
	originalLogger := defaultLogger
	defer func() { defaultLogger = originalLogger }()

	// Setup test logger
	core, recorded := observer.New(zapcore.InfoLevel)
	defaultLogger = zap.New(core)

	// Number of goroutines and messages per goroutine
	numGoroutines := 10
	messagesPerGoroutine := 10

	// Channel to coordinate goroutines
	done := make(chan bool, numGoroutines)

	// Start concurrent logging
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer func() { done <- true }()

			for j := 0; j < messagesPerGoroutine; j++ {
				Info("concurrent message", "goroutine", goroutineID, "message", j)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Check that all messages were logged
	logs := recorded.All()
	expectedLogs := numGoroutines * messagesPerGoroutine
	if len(logs) != expectedLogs {
		t.Errorf("Expected %d log entries, got %d", expectedLogs, len(logs))
	}
}

// TestEmptyMessage tests logging with empty message
func TestEmptyMessage(t *testing.T) {
	// Save original logger
	originalLogger := defaultLogger
	defer func() { defaultLogger = originalLogger }()

	// Setup test logger
	core, recorded := observer.New(zapcore.InfoLevel)
	defaultLogger = zap.New(core)

	// Test empty message
	Info("", "key", "value")

	// Check recorded logs
	logs := recorded.All()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	entry := logs[0]
	if entry.Message != "" {
		t.Errorf("Expected empty message, got '%s'", entry.Message)
	}
	if len(entry.Context) != 1 {
		t.Errorf("Expected 1 context field, got %d", len(entry.Context))
	}
}

// TestNoFields tests logging without any fields
func TestNoFields(t *testing.T) {
	// Save original logger
	originalLogger := defaultLogger
	defer func() { defaultLogger = originalLogger }()

	// Setup test logger
	core, recorded := observer.New(zapcore.InfoLevel)
	defaultLogger = zap.New(core)

	// Test logging without fields
	Info("message without fields")

	// Check recorded logs
	logs := recorded.All()
	if len(logs) != 1 {
		t.Fatalf("Expected 1 log entry, got %d", len(logs))
	}

	entry := logs[0]
	if entry.Message != "message without fields" {
		t.Errorf("Expected 'message without fields', got '%s'", entry.Message)
	}
	if len(entry.Context) != 0 {
		t.Errorf("Expected 0 context fields, got %d", len(entry.Context))
	}
}
