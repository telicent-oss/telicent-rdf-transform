#!/usr/bin/env python3
"""
Basic example demonstrating the rdf_transform library.

Run from the repository root:
    python examples/basic_mapper/run.py

Or from the examples/basic_mapper directory:
    python run.py
"""

from pathlib import Path

from rdf_transform import MapperConfig, load_mapping_graphs, transform_rdf


def main():
    # Paths relative to this script's location
    script_dir = Path(__file__).parent
    config_path = script_dir / "config.yaml"
    input_path = script_dir / "input.ttl"
    output_path = script_dir / "output.ttl"

    print("Basic Mapper Example")
    print("=" * 50)

    # Load configuration
    config = MapperConfig.from_yaml(config_path)
    print(f"Loaded config: {len(config.queries)} queries")

    # Load mapping graphs (paths are relative to the config file location)
    mapping_graph = load_mapping_graphs(config)
    if mapping_graph is not None:
        print(f"Loaded mappings: {len(mapping_graph)} triples")

    # Read input
    with open(input_path, "rb") as f:
        input_data = f.read()

    # Transform
    output_data, metrics = transform_rdf(
        input_data=input_data,
        input_format="text/turtle",
        output_format="text/turtle",
        config=config,
        mapping_graph=mapping_graph,
    )

    # Write output
    with open(output_path, "wb") as f:
        f.write(output_data)

    print(f"Transformed: {metrics['input_triples']} -> {metrics['output_triples']} triples")
    print(f"Time: {metrics['total_time']:.3f}s")
    print(f"Output written to: {output_path}")


if __name__ == "__main__":
    main()
