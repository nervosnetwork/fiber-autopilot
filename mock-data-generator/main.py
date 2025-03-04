import networkx as nx
from networkx.readwrite import json_graph
import sys
import json
import random
import numpy as np

# define weight range
WEIGHT_MIN = 100
WEIGHT_MAX = 1_000_000
NODES = 1000


def main():
    # Generate a random graph
    G = nx.barabasi_albert_graph(NODES, 2)
    # Generate a random weight with pareto distribution
    rng = np.random.default_rng()
    weights = rng.pareto(a=2.5, size=len(G.edges())) * WEIGHT_MIN
    for edge, weight in zip(G.edges(), weights):
        G[edge[0]][edge[1]]["weight"] = weight
    # Convert the graph to a JSON object
    data = json_graph.node_link_data(G, edges="links")
    # get path from args
    if len(sys.argv) < 2:
        print("Usage: uv run main.py <path>")
        sys.exit(1)
    path = sys.argv[1]
    # Save the graph to a file
    with open(path, "w") as f:
        json.dump(data, f)
    print(f"Graph saved to {path}")



if __name__ == "__main__":
    main()
