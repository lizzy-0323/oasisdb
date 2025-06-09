all: clean build test lint

clean:
	@echo "Cleaning..."
	rm -rf bin/oasisdb

engine:
	@echo "Building vector search engine..."
	cd internal/engine && mkdir -p build && cd build && cmake .. && make
	
test:
	@echo "Running tests..."
	go test -v ./...

build: engine
	@echo "Building oasisdb..."
	mkdir -p bin && go build -o bin/oasisdb cmd/main.go

lint:
	@echo "Running linter..."
	golangci-lint run

.PHONY: all test clean engine build lint