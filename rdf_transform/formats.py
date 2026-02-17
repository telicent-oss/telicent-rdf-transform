"""RDF format and MIME type utilities."""

# MIME type to RDFLib format mapping
MIME_TO_RDFLIB_FORMAT = {
    "application/rdf+xml": "xml",
    "text/turtle": "turtle",
    "application/n-triples": "nt",
    "application/n-quads": "nquads",
    "application/ld+json": "json-ld",
    "application/trig": "trig",
}


def get_rdflib_format_from_mime(mime_type: str) -> str:
    """Convert MIME type to RDFLib format string.

    Args:
        mime_type: MIME type string (e.g., 'text/turtle')

    Returns:
        RDFLib format string (defaults to 'turtle' if not found)
    """
    # Remove any charset or other parameters from content type
    clean_mime = mime_type.split(";")[0].strip()

    # Map MIME type to RDFLib format
    rdf_format = MIME_TO_RDFLIB_FORMAT.get(clean_mime)
    if not rdf_format:
        return "turtle"  # Default fallback

    return rdf_format
