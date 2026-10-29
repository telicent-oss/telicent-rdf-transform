"""Configuration system for SPARQL CONSTRUCT queries."""

import string
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from rdflib.plugins.sparql import prepareQuery

REMOVED_KEYS = ("mapping_file", "mapping_file_format")
CHECK_KEYS = {"name", "query", "message", "description", "enabled", "order"}


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
class Check:
    """A SPARQL SELECT query that must return no rows, run on the output after the queries."""

    name: str
    """Unique identifier for this check"""

    query: str
    """SELECT query returning one row per offending statement"""

    message: str
    """Message reported for each row; each {name} placeholder names a variable the query selects"""

    description: str = ""
    """Optional description of what this check guards against"""

    enabled: bool = True
    """Whether this check should be run"""

    order: int = 0
    """Execution order (lower numbers run first)"""


@dataclass
class MapperConfig:
    """Configuration for the RDF transformation pipeline."""

    queries: list[SPARQLQuery]
    """List of SPARQL CONSTRUCT queries to execute"""

    namespaces: dict[str, str]
    """Namespace prefixes and their URIs"""

    mapping_files: list[MappingFile] = field(default_factory=list)
    """Optional RDF files merged into the working graph before the queries run and removed from the output"""

    checks: list[Check] = field(default_factory=list)
    """Optional checks run on the output; any row returned fails the transformation"""

    base_dir: Path = field(default_factory=Path.cwd)
    """Directory that relative mapping file paths are resolved against"""

    def __post_init__(self) -> None:
        for check in self.checks:
            validate_check(check, self.namespaces)

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
            ValueError: If the configuration uses a removed key, a malformed mapping_files or checks entry,
                or a check whose query or message is invalid
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
            ValueError: If the configuration uses a removed key, a malformed mapping_files or checks entry,
                or a check whose query or message is invalid
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
            checks=_parse_checks(data.get("checks", [])),
            base_dir=Path(base_dir) if base_dir is not None else Path.cwd(),
        )


def check_variables(check: Check, namespaces: dict[str, str]) -> list[str]:
    """Parse a check's query and return the names of the variables it selects.

    Args:
        check: The check whose query to parse
        namespaces: Prefixes the query may use without declaring them

    Returns:
        The selected variable names, without the leading question mark

    Raises:
        ValueError: If the query is not valid SPARQL or is not a SELECT query
    """
    try:
        parsed = prepareQuery(check.query, initNs=namespaces)
    except Exception as error:
        raise ValueError(f"Check {check.name!r}: query does not parse: {error}") from error
    if parsed.algebra.name != "SelectQuery":
        raise ValueError(f"Check {check.name!r} must be a SPARQL SELECT query")
    return [str(variable) for variable in parsed.algebra["PV"]]


def check_placeholders(check: Check) -> list[str]:
    """Return the placeholder names in a check's message.

    Args:
        check: The check whose message to parse

    Returns:
        The names inside each {name}, in order of appearance

    Raises:
        ValueError: If the message has unbalanced braces or a placeholder that is not a plain {name}
    """
    try:
        fields = list(string.Formatter().parse(check.message))
    except ValueError as error:
        raise ValueError(f"Check {check.name!r}: message is malformed: {error}") from error
    names = []
    for _, name, spec, conversion in fields:
        if name is None:
            continue
        if not name.isidentifier() or spec or conversion:
            raise ValueError(f"Check {check.name!r}: message placeholders must be plain {{name}}, got {{{name}}}")
        names.append(name)
    return names


def validate_check(check: Check, namespaces: dict[str, str]) -> None:
    """Raise ValueError unless the check's query is a SELECT and every message placeholder is a selected variable."""
    variables = check_variables(check, namespaces)
    unknown = [name for name in check_placeholders(check) if name not in variables]
    if unknown:
        raise ValueError(f"Check {check.name!r}: message placeholders {unknown} are not selected by its query")


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


def _parse_checks(entries: Any) -> list[Check]:
    if not isinstance(entries, list):
        raise ValueError("'checks' must be a list of entries with 'name', 'query' and 'message'")
    checks = []
    for entry in entries:
        if not isinstance(entry, dict) or not {"name", "query", "message"} <= set(entry):
            raise ValueError(f"Each 'checks' entry needs 'name', 'query' and 'message', got: {entry!r}")
        unknown = set(entry) - CHECK_KEYS
        if unknown:
            raise ValueError(f"Unknown keys in check {entry['name']!r}: {sorted(unknown)}")
        checks.append(
            Check(
                name=entry["name"],
                query=entry["query"],
                message=entry["message"],
                description=entry.get("description", ""),
                enabled=entry.get("enabled", True),
                order=entry.get("order", 0),
            )
        )
    checks.sort(key=lambda c: c.order)
    return [c for c in checks if c.enabled]
