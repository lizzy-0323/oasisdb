package logger

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	DebugLevel = "debug"
	InfoLevel  = "info"
	WarnLevel  = "warn"
	ErrorLevel = "error"
	FatalLevel = "fatal"
)

var defaultLogger *zap.Logger

func init() {
	// Initialize with default production config
	// In main function, we will override the logger with the config file
	InitLogger(InfoLevel, "")
}

// InitLogger initializes the logger with specified level and file path
func InitLogger(level, filePath string) {
	// Parse log level
	var zapLevel zapcore.Level
	switch strings.ToLower(level) {
	case DebugLevel:
		zapLevel = zapcore.DebugLevel
	case InfoLevel:
		zapLevel = zapcore.InfoLevel
	case WarnLevel:
		zapLevel = zapcore.WarnLevel
	case ErrorLevel:
		zapLevel = zapcore.ErrorLevel
	case FatalLevel:
		zapLevel = zapcore.FatalLevel
	default:
		zapLevel = zapcore.InfoLevel
	}

	// Configure encoder
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

	// Configure output
	var core zapcore.Core
	if filePath != "" {
		// Write to file
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}
		fileWriter := zapcore.AddSync(file)
		core = zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig),
			fileWriter,
			zapLevel,
		)
	} else {
		// Write to stdout
		core = zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderConfig),
			zapcore.AddSync(os.Stdout),
			zapLevel,
		)
	}

	// Build logger
	defaultLogger = zap.New(core, zap.AddCaller())
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
