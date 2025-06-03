all: clean build

clean:
	@echo "Cleaning..."
	rm -rf build/oasisdb

engine:
	@echo "Building vector search engine..."
	cd internal/engine && mkdir -p build && cd build && cmake .. && make
	
test:
	@echo "Running tests..."
	go test -v ./...

build: engine test
	@echo "Building oasisdb..."
	mkdir -p build && go build -o build/oasisdb cmd/main.go

.PHONY: all clean engine build