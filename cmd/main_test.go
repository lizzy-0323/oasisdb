package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create stdout pipe: %v", err)
	}

	os.Stdout = writer
	defer func() {
		os.Stdout = oldStdout
	}()

	fn()

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close stdout writer: %v", err)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read stdout: %v", err)
	}
	return string(output)
}

func TestPrintBannerWritesAsciiArt(t *testing.T) {
	output := captureStdout(t, printBanner)

	if !strings.Contains(output, "OasisDB") {
		t.Fatalf("expected banner output, got %q", output)
	}
	if !strings.Contains(output, "___") {
		t.Fatalf("expected banner art, got %q", output)
	}
}

func TestMainReturnsWhenConfigFileIsMissing(t *testing.T) {
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	defer func() {
		_ = os.Chdir(oldWD)
	}()

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("failed to change working directory: %v", err)
	}

	_ = captureStdout(t, main)
}
