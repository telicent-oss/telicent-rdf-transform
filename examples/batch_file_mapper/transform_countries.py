#!/usr/bin/env python3
"""
Example demonstrating batch directory processing with the rdf_transform library.

This script transforms all .ttl files in the input/ directory from IES4 ontology
to IES Next ontology, writing results to output/.

Run from the repository root:
    python examples/batch_file_mapper/transform_countries.py

Or from this directory:
    python transform_countries.py
"""

from pathlib import Path

from mapper import transform_directory

from rdf_transform import MapperConfig, load_mapping_graph


def main():
    # Paths relative to this script's location
    script_dir = Path(__file__).parent
    config_path = script_dir / "config.yaml"
    input_dir = script_dir / "input"
    output_dir = script_dir / "output"

    print("Batch File Mapper Example - Directory Transformation")
    print("=" * 55)

    # Load configuration
    config = MapperConfig.from_yaml(config_path)
    print(f"Loaded config: {len(config.queries)} queries")

    # Load mapping graph (path is relative to config file location)
    mapping_graph = None
    if config.mapping_file:
        mapping_file_path = config_path.parent / config.mapping_file
        mapping_graph = load_mapping_graph(
            source=str(mapping_file_path),
            format=config.mapping_file_format,
            config=config,
        )
        print(f"Loaded mappings: {len(mapping_graph)} triples")

    # Transform all .ttl files in input directory
    print(f"\nProcessing files in: {input_dir}")
    print("-" * 55)

    results = transform_directory(
        input_dir=input_dir,
        output_dir=output_dir,
        config=config,
        mapping_graph=mapping_graph,
        pattern="*.ttl",
    )

    # Print results for each file
    total_input = 0
    total_output = 0
    total_time = 0.0

    for filename, metrics in results.items():
        in_triples = metrics["input_triples"]
        out_triples = metrics["output_triples"]
        time_taken = metrics["total_time"]
        print(f"  {filename}: {in_triples} -> {out_triples} triples ({time_taken:.3f}s)")
        total_input += metrics["input_triples"]
        total_output += metrics["output_triples"]
        total_time += metrics["total_time"]

    print("-" * 55)
    print(f"Total: {total_input} -> {total_output} triples across {len(results)} files")
    print(f"Total time: {total_time:.3f}s")
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    main()
