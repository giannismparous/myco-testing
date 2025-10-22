# Myco

This is a pure Rust implementation of [Myco](https://eprint.iacr.org/2025/687.pdf), an asynchronous metadata-private messaging system that achieves polylogarithmic accesses and strong cryptographic guarantees.

> **⚠️ CAUTION**
> This implementation is an academic proof-of-concept prototype, has not received careful code review, and is not ready for production use.

## Prerequisites

Install Just:
```bash
# macOS
brew install just

# Cross-platform 
cargo install just
```

> **ⓘ NOTE**
> Just is an optional command runner that simplifies running common commands. If you prefer not to use it, you can run the cargo commands directly as listed in the Justfile.

### Running Bucket Capacity Simulations

```bash
# Run the Tree-Myco simulation
just simulation-tree

# Run the Matrix-Myco simulation
just simulation-matrix
```

## Running the Client-Server Setup (Latency)

### Start Server2
```bash
# Default address (0.0.0.0:3004)
just server2

# Custom address
just server2 192.168.1.100:8080
```

### Start Server1
```bash
# Default addresses
# - Server1: 0.0.0.0:3002
# - Server2: https://localhost:3004
just server1

# Custom addresses
just server1 "https://192.168.1.100:8080" 0.0.0.0:5000
```

### Run Client
```bash
# Default addresses
# - Server1: https://localhost:3002
# - Server2: https://localhost:3004
just client

# Custom addresses
just client "https://192.168.1.100:5000" "https://192.168.1.101:8080"
```

## Running the Throughput Experiment

### Start Server2 Throughput Test
```bash
# Default address (0.0.0.0:3004)
just server2-tput

# Custom address
just server2-tput 192.168.1.100:8080
```

### Start Server1 Throughput Test
```bash
# Default Server2 address (https://127.0.0.1:3004)
just server1-tput

# Custom Server2 address
just server1-tput "https://192.168.1.100:8080"
```

## Testing
Run the test suite with:
```bash
just test
```

### Acknowledgements

This code is written by Darya Kaviani ([@daryakaviani](https://github.com/daryakaviani)) and Bhargav Annem ([@0xWOLAND](https://github.com/0xWOLAND)), with contributions from Ratan Kaliani ([@ratankaliani](https://github.com/ratankaliani)).