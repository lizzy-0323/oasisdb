#!/bin/bash

ARCH_LIST=("amd64" "arm64")
OS_LIST=("linux" "darwin")

echo "Creating release..."
for os in "${OS_LIST[@]}"; do
    for arch in "${ARCH_LIST[@]}"; do
        echo "Building $os/$arch..."
        make build GOOS=$os GOARCH=$arch BINARY_NAME=oasisdb-$os-$arch
    done
done



        





