"""Batch processing mode for RDF transformations.

This package provides file-based batch transformation utilities
for processing RDF data without Kafka dependencies.
"""

from .mapper import transform_directory, transform_file

__all__ = [
    "transform_file",
    "transform_directory",
]
