all: clean build test lint run release help

BINARY_NAME := oasisdb
OS := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)
MAIN_PACKAGE := ./cmd

BUILD_FLAGS :=-v

GOCMD := go
GOBUILD := $(GOCMD) build
GOTEST := $(GOCMD) test
GOCLEAN := $(GOCMD) clean
GOLINT := golangci-lint run

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf bin/${BINARY_NAME}
	rm -rf internal/engine/build

engine:
	@echo "Building vector search engine..."
	cd internal/engine && mkdir -p build && cd build && cmake .. && make
	
test:
	@echo "Running tests..."
	$(GOTEST) $(BUILD_FLAGS) -coverprofile=coverage.out ./...

build: engine
	@echo "Building ${BINARY_NAME}..."
	mkdir -p bin
	GOOS=${OS} GOARCH=${ARCH} $(GOBUILD) -o bin/${BINARY_NAME} ${MAIN_PACKAGE}

docker-build:
	@echo "Building docker image..."
	docker build -t ${BINARY_NAME}:latest -f Dockerfile .

run: build
	@echo "Running ${BINARY_NAME} locally..."
	./bin/${BINARY_NAME}

docker-run: docker-build
	@echo "Running ${BINARY_NAME} in Docker..."
	docker run --rm -p 8080:8080 ${BINARY_NAME}:latest

lint:
	@echo "Running linter..."
	$(GOLINT)

help:
	@echo "Available targets:"
	@echo "  all: Clean, build, test, lint, run, release"
	@echo "  clean: Clean up the build directory"
	@echo "  build: Build the application"
	@echo "  test: Run tests"
	@echo "  lint: Run linter"
	@echo "  docker-build: Build Docker image"
	@echo "  docker-run: Run container exposing port 8080"
	@echo "  run: Run the application"
	@echo "  help: Show this help message"

.PHONY: all test clean engine build lint run docker-build docker-run release help