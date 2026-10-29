"""Configuration system for SPARQL CONSTRUCT queries."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

REMOVED_KEYS = ("mapping_file", "mapping_file_format")


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
class MappingFile:
    """One RDF file merged into the working graph for the queries to use."""

    path: str
    """File path or URL. A relative path is resolved against MapperConfig.base_dir"""

    format: str = "text/turtle"
    """MIME type of the file (default: text/turtle)"""


@dataclass
class MapperConfig:
    """Configuration for the RDF transformation pipeline."""

    queries: list[SPARQLQuery]
    """List of SPARQL CONSTRUCT queries to execute"""

    namespaces: dict[str, str]
    """Namespace prefixes and their URIs"""

    mapping_files: list[MappingFile] = field(default_factory=list)
    """Optional RDF files merged into the working graph before the queries run and removed from the output"""

    base_dir: Path = field(default_factory=Path.cwd)
    """Directory that relative mapping file paths are resolved against"""

    @classmethod
    def from_yaml(cls, path: str | Path) -> "MapperConfig":
        """Load configuration from a YAML file.

        Relative mapping file paths are resolved against the directory of the YAML file.

        Args:
            path: Path to the YAML configuration file

        Returns:
            MapperConfig instance

        Raises:
            FileNotFoundError: If the config file doesn't exist
            ValueError: If the configuration uses a removed key or a malformed mapping_files entry
            yaml.YAMLError: If the YAML is invalid
        """
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(config_path) as f:
            data = yaml.safe_load(f)

        return cls.from_dict(data, base_dir=config_path.parent)

    @classmethod
    def from_dict(cls, data: dict[str, Any], base_dir: str | Path | None = None) -> "MapperConfig":
        """Create configuration from a dictionary.

        Args:
            data: Dictionary containing configuration
            base_dir: Directory that relative mapping file paths are resolved against (default: current directory)

        Returns:
            MapperConfig instance

        Raises:
            ValueError: If the configuration uses a removed key or a malformed mapping_files entry
        """
        for key in REMOVED_KEYS:
            if key in data:
                raise ValueError(
                    f"'{key}' is no longer supported. Use 'mapping_files', a list of entries "
                    "with 'path' and an optional 'format'."
                )

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
            mapping_files=_parse_mapping_files(data.get("mapping_files", [])),
            base_dir=Path(base_dir) if base_dir is not None else Path.cwd(),
        )


def _parse_mapping_files(entries: Any) -> list[MappingFile]:
    if not isinstance(entries, list):
        raise ValueError("'mapping_files' must be a list of entries with 'path' and an optional 'format'")
    mapping_files = []
    for entry in entries:
        if not isinstance(entry, dict) or "path" not in entry:
            raise ValueError(f"Each 'mapping_files' entry needs a 'path' and an optional 'format', got: {entry!r}")
        unknown = set(entry) - {"path", "format"}
        if unknown:
            raise ValueError(f"Unknown keys in 'mapping_files' entry {entry['path']!r}: {sorted(unknown)}")
        mapping_files.append(MappingFile(path=str(entry["path"]), format=entry.get("format", "text/turtle")))
    return mapping_files
