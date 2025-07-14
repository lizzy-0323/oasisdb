#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

ARCH_LIST=("amd64" "arm64")
OS_LIST=("linux" "darwin")
BINARY_NAME="oasisdb"

tagName=$1
echo "release tag: $tagName"

if [ $# -lt 1 ]; then
    echo "Usage: $0 <tag_name>"
    exit 1
fi

# Build release artifacts
function build_binary(){
    binary_name="${BINARY_NAME}"
    echo "Building $binary_name..."

    release_dir=$1
    echo "build release artifacts to $release_dir"

    # Create tmp dir for make binaries
    mkdir -p "output"
    for os in "${OS_LIST[@]}"; do
        for arch in "${ARCH_LIST[@]}"; do
            echo "Building $os/$arch..."
            make build GOOS=$os GOARCH=$arch BINARY_NAME=$binary_name

            if [ $? -ne 0 ]; then
                echo "Failed to build $os/$arch"
                exit 1
            fi

            cp bin/$binary_name output/
            tar cvfz ${release_dir}/${binary_name}-${os}-${arch}.tar.gz -C output $binary_name
            echo "Built ${binary_name}-${os}-${arch}.tar.gz"
            rm output/$binary_name
        done
    done

    # Create checksum
    pushd "${release_dir}"
    for release_file in *; do
        echo "Creating checksum for $release_file"
        shasum -a 256 "$release_file" >> checksums.txt
    done
    popd

    rmdir "output"    
}

function create_release(){
    additional_release_artifacts=""

    # Build cli binaries for all supported platforms
    release_artifact_dir=$(mktemp -d)
    build_binary "$release_artifact_dir"

    additional_release_artifacts=("$release_artifact_dir"/*)

    # Create github releases
    gh release create "$tagName" \
        --title "$tagName" \
        --notes "$tagName - See CHANGELOG.md for details" \
        --draft "${additional_release_artifacts[@]}"
    
    rm -rf "$release_artifact_dir"
}

create_release $tagName



        





