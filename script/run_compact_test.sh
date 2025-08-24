#!/bin/bash

# OasisDB Compact Test Runner
# This script runs the compact test and log monitor simultaneously

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🏁 OasisDB Compact Test Runner"
echo "=============================="
echo "Project root: $PROJECT_ROOT"
echo "Script dir: $SCRIPT_DIR"

# Check if OasisDB is running
echo "🔍 Checking if OasisDB server is running..."
if ! curl -s http://localhost:8080/ > /dev/null 2>&1; then
    echo "❌ OasisDB server is not running on http://localhost:8080"
    echo "Please start the server first:"
    echo "  cd $PROJECT_ROOT"
    echo "  ./bin/oasisdb"
    exit 1
fi

echo "✅ OasisDB server is running"

# Check for log file
LOG_FILE="$PROJECT_ROOT/oasisdb.log"
if [[ ! -f "$LOG_FILE" ]]; then
    echo "⚠️ Log file not found at $LOG_FILE"
    echo "The log monitor will wait for it to be created."
fi

# Make scripts executable
chmod +x "$SCRIPT_DIR/compact_test.py"
chmod +x "$SCRIPT_DIR/log_monitor.py"

# Check Python dependencies
echo "🔍 Checking Python dependencies..."
python3 -c "import numpy, sys, os, json, threading, time" 2>/dev/null || {
    echo "❌ Missing Python dependencies. Please install:"
    echo "  pip install numpy"
    exit 1
}

echo "✅ Python dependencies OK"

# Function to cleanup background processes
cleanup() {
    echo "🧹 Cleaning up background processes..."
    if [[ -n "$LOG_MONITOR_PID" ]]; then
        kill $LOG_MONITOR_PID 2>/dev/null || true
    fi
    if [[ -n "$COMPACT_TEST_PID" ]]; then
        kill $COMPACT_TEST_PID 2>/dev/null || true
    fi
    wait 2>/dev/null
    echo "✅ Cleanup completed"
}

# Set trap for cleanup
trap cleanup EXIT INT TERM

# Start log monitor in background
echo "🔍 Starting log monitor..."
cd "$PROJECT_ROOT"
python3 "$SCRIPT_DIR/log_monitor.py" "$LOG_FILE" &
LOG_MONITOR_PID=$!

# Give log monitor time to start
sleep 2

# Start compact test
echo "🚀 Starting compact test..."
python3 "$SCRIPT_DIR/compact_test.py" &
COMPACT_TEST_PID=$!

echo "📊 Both processes started:"
echo "  Log Monitor PID: $LOG_MONITOR_PID"
echo "  Compact Test PID: $COMPACT_TEST_PID"
echo ""
echo "Press Ctrl+C to stop both processes"
echo "=============================="

# Wait for compact test to finish
wait $COMPACT_TEST_PID
COMPACT_EXIT_CODE=$?

echo ""
echo "=============================="
echo "📋 Test Summary:"
echo "  Compact test exit code: $COMPACT_EXIT_CODE"

if [[ $COMPACT_EXIT_CODE -eq 0 ]]; then
    echo "✅ Compact test completed successfully"
else
    echo "❌ Compact test failed or was interrupted"
fi

# Stop log monitor
if [[ -n "$LOG_MONITOR_PID" ]]; then
    echo "⏹️ Stopping log monitor..."
    kill $LOG_MONITOR_PID 2>/dev/null || true
fi

echo ""
echo "📄 Log file location: $LOG_FILE"
echo "🔍 You can manually review the logs with:"
echo "  tail -f $LOG_FILE"
echo "  grep -i 'compact\|error' $LOG_FILE"

echo ""
echo "🏁 Test runner completed"
