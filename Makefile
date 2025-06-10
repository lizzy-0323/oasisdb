all: clean build test lint help

clean:
	@echo "Cleaning..."
	rm -rf bin/oasisdb

engine:
	@echo "Building vector search engine..."
	cd internal/engine && mkdir -p build && cd build && cmake .. && make
	
test:
	@echo "Running tests..."
	go test -v -coverprofile=coverage.out ./...

build: engine
	@echo "Building oasisdb..."
	mkdir -p bin && go build -o bin/oasisdb cmd/main.go

lint:
	@echo "Running linter..."
	golangci-lint run

help:
	@echo "Available targets:"
	@echo "  all: Clean, build, test, lint"
	@echo "  clean: Clean up the build directory"
	@echo "  build: Build the application"
	@echo "  test: Run tests"
	@echo "  lint: Run linter"
	@echo "  help: Show this help message"

.PHONY: all test clean engine build lint help