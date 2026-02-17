"""Telicent Kafka streaming mapper for RDF transformations.

This package provides Kafka integration for real-time RDF stream processing
using the Telicent platform.
"""

from .labels import create_security_label
from .mapper import mapping_function

__all__ = [
    "mapping_function",
    "create_security_label",
]
