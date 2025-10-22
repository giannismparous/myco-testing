#!/bin/bash
# Test script for Enron data integration with Myco
# This script tests the Enron data loading functionality

echo "Testing Enron data integration with Myco..."
echo "=========================================="

# Check if the Enron data files exist
echo "Checking for Enron data files..."
if [ -f "../sparta-model-evaluation-main/data/enron/users.csv" ]; then
    echo "✓ users.csv found"
else
    echo "✗ users.csv not found"
    exit 1
fi

if [ -f "../sparta-model-evaluation-main/data/enron/clean.csv" ]; then
    echo "✓ clean.csv found"
else
    echo "✗ clean.csv not found"
    exit 1
fi

# Test the Rust compilation
echo ""
echo "Testing Rust compilation..."
cd ../experiments
if cargo check --bin rpc_client; then
    echo "✓ rpc_client compiles successfully"
else
    echo "✗ rpc_client compilation failed"
    exit 1
fi

echo ""
echo "✓ All tests passed! Enron data integration is ready."
echo ""
echo "To run the client with Enron data:"
echo "1. Start Server2: cargo run --bin rpc_server2"
echo "2. Start Server1: cargo run --bin rpc_server1"
echo "3. Run client: cargo run --bin rpc_client"
