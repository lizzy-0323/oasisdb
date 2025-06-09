#!/bin/bash

# Remove all data directories
echo "Removing all data directories..."

# Find and remove all sstfile directories
find . -type d -name "sstfile" -exec rm -rf {} +

# Find and remove all indexfile directories
find . -type d -name "indexfile" -exec rm -rf {} +

# Find and remove all walfile directories
find . -type d -name "walfile" -exec rm -rf {} +

echo "All data directories have been removed."
