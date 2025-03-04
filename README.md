# Fiber Autopilot

Fiber Autopilot continuously analyzes peers in the network graph and recommends potential peers for the operated node to open channels with. The recommendations are currently based on the following heuristic strategies: `Centrality`, `Richness`, and `Random`.


## Run

``` sh
# Run
RUST_LOG=info,fiber_autopilot=info cargo run -c config/testnet.toml
```

## Mock test

1. Generate mock data

``` sh
cd mock-data-generator
uv run main.py ../tmp/graph.json
cd -
```

2. Run mock test

``` sh
RUST_LOG=info cargo run -c config/mock-test.toml
```
