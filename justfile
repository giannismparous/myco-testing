# Default recipe to show available commands
default:
    @just --list

# Simulation commands
simulation-tree:
    cargo run --release --bin simulation

simulation-matrix:
    cargo run --release --bin matrix_simulation

# Client-Server Setup (Latency)
server2 addr="0.0.0.0:3004":
    cargo run --release --bin rpc_server2 {{addr}}

server1 addr="0.0.0.0:3002" server2_addr="https://localhost:3004":
    cargo run --release --bin rpc_server1 {{addr}} {{server2_addr}}

client server1_addr="http://localhost:3002" server2_addr="http://localhost:3004":
    cargo run --release --bin rpc_client {{server1_addr}} {{server2_addr}}

# Run components separately
run-server2:
    cargo run --release --bin rpc_server2 127.0.0.1:3004

run-server1:
    cargo run --release --bin rpc_server1 127.0.0.1:3002 https://127.0.0.1:3004

run-client:
    cargo run --release --bin rpc_client 127.0.0.1:3002 127.0.0.1:3004

# Throughput Experiments
server2-tput addr="0.0.0.0:3004":
    cargo run --release --bin rpc_server2_tput {{addr}}

server1-tput server2_addr="https://127.0.0.1:3004":
    cargo run --release --bin rpc_server1_tput {{server2_addr}}

# Testing
test:
    cargo test --release
