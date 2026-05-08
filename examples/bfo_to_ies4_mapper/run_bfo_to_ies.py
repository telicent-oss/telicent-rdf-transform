#!/usr/bin/env python3
"""
BFO to IES4 mapper example.

Run from the root directory:
    python -m examples.bfo_to_ies4_mapper.run_bfo_to_ies
"""

from pathlib import Path

from rdf_transform import MapperConfig, load_mapping_graph, transform_rdf


def main():
    script_dir = Path(__file__).parent
    config_path = script_dir / "mapping-config.yaml"

    # Choose input file
    # input_file = "process.ttl"
    # input_file = "info_content_entity.ttl"
    # input_file = "object_material_entity.ttl"
    input_file = "temporal_region.ttl"

    input_path = script_dir / "data/fixtures" / input_file
    output_path = script_dir / "data/golden"/ input_file.replace(".ttl", "_ies.ttl")

    # Load configuration
    config = MapperConfig.from_yaml(str(config_path))
    print(f"Loaded config: {len(config.queries)} queries")

    # Load mapping graph
    mapping_graph = None
    if config.mapping_file:
        mapping_file_path = config_path.parent / config.mapping_file
        mapping_graph = load_mapping_graph(
            source=str(mapping_file_path),
            format=config.mapping_file_format,
            config=config,
        )
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

    print(f"Transformed: {metrics['input_triples']} to {metrics['output_triples']} triples")
    print(f"Output written to: {output_path}")


if __name__ == "__main__":
    main()
