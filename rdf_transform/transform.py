"""Pure RDF transformation logic - no logging, no Record handling.

This module contains the core SPARQL CONSTRUCT transformation logic
separated from the Kafka/Record infrastructure layer.
"""

import time

from rdflib import Graph

from rdf_transform.config import MapperConfig, SPARQLQuery
from rdf_transform.formats import MIME_TO_RDFLIB_FORMAT


def parse_rdf(data: str | bytes, format: str, config: MapperConfig) -> Graph:
    """Parse RDF data into an RDFLib graph.

    Args:
        data: RDF data (string or bytes)
        format: RDFLib format string (e.g., 'turtle', 'xml', 'json-ld')
        config: Mapper configuration with namespaces

    Returns:
        RDFLib Graph containing the parsed data

    Raises:
        Exception: If parsing fails
    """
    graph = Graph()

    # Bind configured namespaces for output serialization
    for prefix, uri in config.namespaces.items():
        graph.bind(prefix, uri)

    # Handle different data types
    if isinstance(data, bytes):
        data = data.decode("utf-8")

    graph.parse(data=data, format=format)
    return graph


def serialize_rdf(graph: Graph, output_format: str) -> bytes:
    """Serialize an RDFLib graph to bytes.

    Args:
        graph: RDFLib Graph to serialize
        output_format: Output MIME type (e.g., 'text/turtle')

    Returns:
        Serialized RDF data as bytes
    """
    rdf_format = MIME_TO_RDFLIB_FORMAT[output_format]
    serialized = graph.serialize(format=rdf_format)
    if isinstance(serialized, str):
        return serialized.encode("utf-8")
    return serialized


def load_mapping_graph(source: str, format: str, config: MapperConfig) -> Graph:
    """Load ontology mappings containing owl:equivalentProperty/Class definitions.

    Args:
        source: File path, URL, or file-like object containing RDF mappings
        format: MIME type (e.g., 'text/turtle', 'application/rdf+xml')
        config: Mapper configuration with namespaces

    Returns:
        Graph containing the ontology mappings

    Raises:
        Exception: If loading fails
    """
    mapping = Graph()

    # Bind namespaces for mappings
    for prefix, uri in config.namespaces.items():
        mapping.bind(prefix, uri)

    # Convert MIME type to rdflib format
    rdflib_format = MIME_TO_RDFLIB_FORMAT.get(format, "turtle")
    mapping.parse(source, format=rdflib_format)
    return mapping


def merge_mapping_with_input(input_graph: Graph, mapping_graph: Graph | None, config: MapperConfig) -> Graph:
    """Merge the mapping graph with the input graph for query processing.

    Args:
        input_graph: Input RDF graph
        mapping_graph: Optional mapping graph to merge
        config: Mapper configuration with namespaces

    Returns:
        New graph containing both input data and mappings
    """
    if mapping_graph is None:
        return input_graph

    # Create a new graph with both input and mapping triples
    merged = Graph()

    # Bind namespaces
    for prefix, uri in config.namespaces.items():
        merged.bind(prefix, uri)

    # Add all triples from input
    for triple in input_graph:
        merged.add(triple)

    # Add all triples from mapping
    for triple in mapping_graph:
        merged.add(triple)

    return merged


def add_prefixes_to_query(query: str, config: MapperConfig) -> str:
    """Add namespace prefixes to a SPARQL query if not already present.

    Args:
        query: SPARQL query string
        config: Mapper configuration with namespaces

    Returns:
        Query with prefixes added (or unchanged if PREFIX declarations already exist)
    """
    # Check if query already has PREFIX declarations
    if "PREFIX" in query.upper():
        return query

    # Build prefix declarations
    prefix_lines = []
    for prefix, uri in config.namespaces.items():
        prefix_lines.append(f"PREFIX {prefix}: <{uri}>")

    if prefix_lines:
        return "\n".join(prefix_lines) + "\n\n" + query

    return query


def is_update_query(query: str) -> bool:
    """Check if a query is a SPARQL UPDATE (DELETE/INSERT) rather than CONSTRUCT.

    Detects based on SPARQL keywords - UPDATE queries use DELETE/INSERT,
    while read queries use CONSTRUCT/SELECT/ASK/DESCRIBE.

    Args:
        query: SPARQL query string

    Returns:
        True if query is an UPDATE query (contains DELETE or INSERT keywords)
    """
    # Remove PREFIX declarations and comments for cleaner detection
    lines = []
    for line in query.split("\n"):
        stripped = line.strip().upper()
        if not stripped.startswith("PREFIX") and not stripped.startswith("#"):
            lines.append(stripped)
    query_body = " ".join(lines)

    # Check for UPDATE keywords (DELETE or INSERT as statement start)
    return "DELETE" in query_body or "INSERT" in query_body


def execute_construct_query(graph: Graph, query_config: SPARQLQuery, config: MapperConfig) -> Graph:
    """Execute a single SPARQL CONSTRUCT query.

    Args:
        graph: Input RDFLib Graph
        query_config: Query configuration
        config: Mapper configuration with namespaces

    Returns:
        RDFLib Graph with query results

    Raises:
        Exception: If query execution fails
    """
    # Add namespace prefixes to the query if needed
    prefixed_query = add_prefixes_to_query(query_config.query, config)

    # Execute the CONSTRUCT query
    result = graph.query(prefixed_query)

    # Create a new graph and add all triples from the result
    result_graph = Graph()
    for triple in result:
        result_graph.add(triple)  # type: ignore[arg-type]

    # Bind namespaces to the result graph for clean serialization
    for prefix, uri in config.namespaces.items():
        result_graph.bind(prefix, uri)

    return result_graph


def execute_update_query(graph: Graph, query_config: SPARQLQuery, config: MapperConfig) -> None:
    """Execute a SPARQL UPDATE (DELETE/INSERT) query on a graph in place.

    Args:
        graph: RDFLib Graph to modify in place
        query_config: Query configuration
        config: Mapper configuration with namespaces

    Raises:
        Exception: If query execution fails
    """
    # Add namespace prefixes to the query if needed
    prefixed_query = add_prefixes_to_query(query_config.query, config)

    # Execute the UPDATE query - modifies graph in place
    graph.update(prefixed_query)


def apply_queries(input_graph: Graph, mapping_graph: Graph | None, config: MapperConfig) -> Graph:
    """Apply all SPARQL queries to transform the input graph.

    Supports both CONSTRUCT queries and UPDATE (DELETE/INSERT) queries.
    Query type is auto-detected from SPARQL keywords.

    For UPDATE queries: modifies working graph in place.
    For CONSTRUCT queries: results are collected and merged into output.

    Queries are executed in their configured order. UPDATE queries modify the
    working graph in place, while CONSTRUCT query results are merged into the
    final output. This allows interleaving UPDATE and CONSTRUCT queries as needed.

    The mappings are removed from the final output.

    Args:
        input_graph: Input RDFLib Graph
        mapping_graph: Optional mapping graph for ontology mappings
        config: Mapper configuration with queries and namespaces

    Returns:
        Output RDFLib Graph with transformed data

    Raises:
        Exception: If query execution fails
    """
    # Create working graph as copy of input + mappings
    working_graph = merge_mapping_with_input(input_graph, mapping_graph, config)

    # Check if there are any CONSTRUCT queries
    has_construct = any(not is_update_query(q.query) for q in config.queries)

    if has_construct:
        output_graph = Graph()
        for prefix, uri in config.namespaces.items():
            output_graph.bind(prefix, uri)
    else:
        output_graph = None

    # Execute queries in order - UPDATE modifies working graph, CONSTRUCT adds to output
    for query_config in config.queries:
        if is_update_query(query_config.query):
            execute_update_query(working_graph, query_config, config)
        else:
            result_graph = execute_construct_query(working_graph, query_config, config)
            assert output_graph is not None  # has_construct ensures this
            for triple in result_graph:
                output_graph.add(triple)

    # If no CONSTRUCT queries, use the working graph as output
    if output_graph is None:
        output_graph = working_graph

    # Remove mapping triples from output (they were only needed for queries)
    if mapping_graph is not None:
        for triple in mapping_graph:
            output_graph.remove(triple)

    return output_graph


def transform_rdf(
    input_data: bytes,
    input_format: str,
    output_format: str,
    config: MapperConfig,
    mapping_graph: Graph | None = None,
) -> tuple[bytes, dict[str, float]]:
    """Transform RDF data by applying SPARQL CONSTRUCT queries.

    This is the main entry point for the transformation logic.

    Args:
        input_data: Input RDF data as bytes
        input_format: MIME type for input (e.g., 'text/turtle', 'application/rdf+xml')
        output_format: MIME type for output (e.g., 'text/turtle')
        config: Mapper configuration
        mapping_graph: Optional mapping graph for ontology mappings

    Returns:
        Tuple of (output_data_bytes, timing_metrics)
        timing_metrics contains: parse_time, query_time, serialize_time, total_time

    Raises:
        ValueError: If input or output graph is empty
        Exception: If parsing or transformation fails
    """
    start_time = time.time()
    timing_metrics = {}

    # Convert MIME type to rdflib format for parsing
    rdflib_input_format = MIME_TO_RDFLIB_FORMAT.get(input_format, "turtle")

    # Parse input
    parse_start = time.time()
    input_graph = parse_rdf(input_data, rdflib_input_format, config)
    timing_metrics["parse_time"] = time.time() - parse_start

    if len(input_graph) == 0:
        raise ValueError("Input graph is empty")

    # Apply queries
    query_start = time.time()
    output_graph = apply_queries(input_graph, mapping_graph, config)
    timing_metrics["query_time"] = time.time() - query_start

    if len(output_graph) == 0:
        raise ValueError("Output graph is empty after transformations")

    # Serialize output
    serialize_start = time.time()
    output_data = serialize_rdf(output_graph, output_format)
    timing_metrics["serialize_time"] = time.time() - serialize_start

    timing_metrics["total_time"] = time.time() - start_time
    timing_metrics["input_triples"] = len(input_graph)
    timing_metrics["output_triples"] = len(output_graph)

    return output_data, timing_metrics
