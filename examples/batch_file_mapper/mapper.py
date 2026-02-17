"""Batch file processing utilities for RDF transformations.

This module provides file-based batch transformation using the rdf_transform core library.
"""

from pathlib import Path

from rdflib import Graph

from rdf_transform import MapperConfig, transform_rdf


def transform_file(
    input_path: str | Path,
    output_path: str | Path,
    config: MapperConfig,
    mapping_graph: Graph | None = None,
    input_format: str = "text/turtle",
    output_format: str = "text/turtle",
) -> dict[str, float]:
    """Transform a single RDF file using SPARQL CONSTRUCT queries.

    Args:
        input_path: Path to input RDF file
        output_path: Path to output RDF file
        config: Mapper configuration with queries and namespaces
        mapping_graph: Optional ontology mapping graph
        input_format: MIME type for input (default: 'text/turtle')
        output_format: MIME type for output (default: 'text/turtle')

    Returns:
        Dictionary with transformation metrics (parse_time, query_time, etc.)

    Raises:
        FileNotFoundError: If input file doesn't exist
        ValueError: If transformation produces empty output
        Exception: If transformation fails
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Read input file
    with open(input_path, "rb") as f:
        input_data = f.read()

    # Transform
    output_data, metrics = transform_rdf(
        input_data=input_data,
        input_format=input_format,
        output_format=output_format,
        config=config,
        mapping_graph=mapping_graph,
    )

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write output file
    with open(output_path, "wb") as f:
        f.write(output_data)

    return metrics


def transform_directory(
    input_dir: str | Path,
    output_dir: str | Path,
    config: MapperConfig,
    mapping_graph: Graph | None = None,
    input_format: str = "text/turtle",
    output_format: str = "text/turtle",
    pattern: str = "*.ttl",
) -> dict[str, dict[str, float]]:
    """Transform all matching RDF files in a directory.

    Args:
        input_dir: Directory containing input RDF files
        output_dir: Directory for output RDF files
        config: Mapper configuration with queries and namespaces
        mapping_graph: Optional ontology mapping graph
        input_format: MIME type for input (default: 'text/turtle')
        output_format: MIME type for output (default: 'text/turtle')
        pattern: Glob pattern for files to process (default: '*.ttl')

    Returns:
        Dictionary mapping file names to their transformation metrics

    Raises:
        FileNotFoundError: If input directory doesn't exist
        Exception: If transformation fails for any file
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    if not input_dir.is_dir():
        raise ValueError(f"Input path is not a directory: {input_dir}")

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    results = {}

    # Process all matching files
    for input_file in input_dir.glob(pattern):
        if not input_file.is_file():
            continue

        # Construct output path
        output_file = output_dir / input_file.name

        # Transform
        metrics = transform_file(
            input_path=input_file,
            output_path=output_file,
            config=config,
            mapping_graph=mapping_graph,
            input_format=input_format,
            output_format=output_format,
        )

        results[input_file.name] = metrics

    return results
