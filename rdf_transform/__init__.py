"""RDF transformation core library.

This package provides pure RDF transformation logic using SPARQL CONSTRUCT queries.
It has minimal dependencies (rdflib only) and no coupling to execution modes.
"""

from rdf_transform.config import MapperConfig, SPARQLQuery
from rdf_transform.formats import MIME_TO_RDFLIB_FORMAT, get_rdflib_format_from_mime
from rdf_transform.transform import (
    add_prefixes_to_query,
    apply_queries,
    execute_construct_query,
    load_mapping_graph,
    merge_mapping_with_input,
    parse_rdf,
    serialize_rdf,
    transform_rdf,
)

__all__ = [
    # Configuration
    "MapperConfig",
    "SPARQLQuery",
    # Main transformation function
    "transform_rdf",
    # Utility functions
    "load_mapping_graph",
    "parse_rdf",
    "serialize_rdf",
    "apply_queries",
    "execute_construct_query",
    "merge_mapping_with_input",
    "add_prefixes_to_query",
    # Format utilities
    "get_rdflib_format_from_mime",
    "MIME_TO_RDFLIB_FORMAT",
]
