#!/usr/bin/env python3
"""Map an IES4 ontology extension to IES-Next using mapping-config.yaml.

Run from the repository root:
    python examples/ies4_extension_to_iesnext_mapper/run.py <extension.ttl> <output.ttl>
"""

import argparse
import sys
from pathlib import Path

from rdf_transform import CheckError, MapperConfig, load_mapping_graphs, transform_rdf


def main() -> None:
    parser = argparse.ArgumentParser(description="Map an IES4 ontology extension to IES-Next.")
    parser.add_argument("extension", type=Path, help="IES4 ontology extension in Turtle")
    parser.add_argument("output", type=Path, help="where to write the IES-Next extension in Turtle")
    args = parser.parse_args()

    config = MapperConfig.from_yaml(Path(__file__).parent / "mapping-config.yaml")
    print(f"Loaded config: {len(config.queries)} queries, {len(config.checks)} checks")

    mapping_graph = load_mapping_graphs(config)
    if mapping_graph is not None:
        print(f"Loaded mappings: {len(mapping_graph)} triples")

    try:
        output_data, metrics = transform_rdf(
            input_data=args.extension.read_bytes(),
            input_format="text/turtle",
            output_format="text/turtle",
            config=config,
            mapping_graph=mapping_graph,
        )
    except CheckError as error:
        sys.exit(f"The extension cannot be mapped:\n{error}")
    args.output.write_bytes(output_data)

    print(f"Transformed: {metrics['input_triples']} -> {metrics['output_triples']} triples")
    print(f"Output written to: {args.output}")


if __name__ == "__main__":
    main()
