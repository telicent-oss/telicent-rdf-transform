"""Configuration system for SPARQL CONSTRUCT queries."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class SPARQLQuery:
    """Represents a single SPARQL CONSTRUCT query configuration."""

    name: str
    """Unique identifier for this query"""

    query: str
    """The SPARQL CONSTRUCT query string"""

    description: str = ""
    """Optional description of what this query does"""

    enabled: bool = True
    """Whether this query should be executed"""

    order: int = 0
    """Execution order (lower numbers run first)"""


@dataclass
class MapperConfig:
    """Configuration for the RDF transformation pipeline."""

    queries: list[SPARQLQuery]
    """List of SPARQL CONSTRUCT queries to execute"""

    namespaces: dict[str, str]
    """Namespace prefixes and their URIs"""

    mapping_file: str | None = None
    """Optional path to RDF mapping file with owl:equivalentProperty/Class mappings"""

    mapping_file_format: str = "text/turtle"
    """Format of the mapping file as MIME type (default: text/turtle)"""

    @classmethod
    def from_yaml(cls, path: str | Path) -> "MapperConfig":
        """Load configuration from a YAML file.

        Args:
            path: Path to the YAML configuration file

        Returns:
            MapperConfig instance

        Raises:
            FileNotFoundError: If the config file doesn't exist
            yaml.YAMLError: If the YAML is invalid
        """
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(config_path) as f:
            data = yaml.safe_load(f)

        # Parse queries
        queries = []
        for q_data in data.get("queries", []):
            query = SPARQLQuery(
                name=q_data["name"],
                query=q_data["query"],
                description=q_data.get("description", ""),
                enabled=q_data.get("enabled", True),
                order=q_data.get("order", 0),
            )
            queries.append(query)

        # Sort queries by order
        queries.sort(key=lambda q: q.order)

        # Filter to only enabled queries
        enabled_queries = [q for q in queries if q.enabled]

        return cls(
            queries=enabled_queries,
            namespaces=data.get("namespaces", {}),
            mapping_file=data.get("mapping_file", None),
            mapping_file_format=data.get("mapping_file_format", "text/turtle"),
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MapperConfig":
        """Create configuration from a dictionary.

        Args:
            data: Dictionary containing configuration

        Returns:
            MapperConfig instance
        """
        queries = []
        for q_data in data.get("queries", []):
            query = SPARQLQuery(
                name=q_data["name"],
                query=q_data["query"],
                description=q_data.get("description", ""),
                enabled=q_data.get("enabled", True),
                order=q_data.get("order", 0),
            )
            queries.append(query)

        queries.sort(key=lambda q: q.order)
        enabled_queries = [q for q in queries if q.enabled]

        return cls(
            queries=enabled_queries,
            namespaces=data.get("namespaces", {}),
            mapping_file=data.get("mapping_file", None),
            mapping_file_format=data.get("mapping_file_format", "text/turtle"),
        )
