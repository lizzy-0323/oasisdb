# syntax=docker/dockerfile:1

# -------- Builder Stage --------
FROM golang:1.22-bullseye AS builder

# Install build dependencies for C++ engine (cmake, build-essential, clang, libomp)
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential cmake clang 

# Set working directory
WORKDIR /app

# Cache Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binary using project Makefile (includes C++ engine)
RUN make build

# -------- Runtime Stage --------
FROM debian:bullseye-slim AS runtime
LABEL maintainer="Chris Lee <liziyi0323xxx@gmail.com>"

WORKDIR /app

# Copy default config (can be overridden)
COPY --from=builder /app/conf.yaml ./conf.yaml

COPY --from=builder /app/bin/oasisdb /usr/local/bin/oasisdb

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/oasisdb"]