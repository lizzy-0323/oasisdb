all: clean build test lint run help

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

run: build
	@echo "Running oasisdb..."
	./bin/oasisdb
lint:
	@echo "Running linter..."
	golangci-lint run

help:
	@echo "Available targets:"
	@echo "  all: Clean, build, test, lint, run"
	@echo "  clean: Clean up the build directory"
	@echo "  build: Build the application"
	@echo "  test: Run tests"
	@echo "  lint: Run linter"
	@echo "  run: Run the application"
	@echo "  help: Show this help message"

.PHONY: all test clean engine build lint run help