package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var defaultLogger *zap.Logger

func init() {
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	logger, err := config.Build()
	if err != nil {
		panic(err)
	}
	defaultLogger = logger
}

// Debug logs a debug message with fields
func Debug(msg string, fields ...interface{}) {
	defaultLogger.Sugar().Debugw(msg, fields...)
}

// Info logs an info message with fields
func Info(msg string, fields ...interface{}) {
	defaultLogger.Sugar().Infow(msg, fields...)
}

// Warn logs a warning message with fields
func Warn(msg string, fields ...interface{}) {
	defaultLogger.Sugar().Warnw(msg, fields...)
}

// Error logs an error message with fields
func Error(msg string, fields ...interface{}) {
	defaultLogger.Sugar().Errorw(msg, fields...)
}

// Fatal logs a fatal message with fields and exits
func Fatal(msg string, fields ...interface{}) {
	defaultLogger.Sugar().Fatalw(msg, fields...)
}

// With creates a child logger with fields
func With(fields ...interface{}) *zap.SugaredLogger {
	return defaultLogger.Sugar().With(fields...)
}
