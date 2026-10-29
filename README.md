# telicent-rdf-transform

A python library for transforming RDF data from one form to another; in most cases: data from one ontology to another. Configuration of this transformation is provided as a series of SPARQL queries. This library has been built so it can be used in multiple contexts: streaming, batch files, APIs, etc.

The library itself (telicent-rdf-transform) is accompanied by [`/examples`](https://github.com/telicent-oss/telicent-rdf-transform/tree/main/examples) demonstrating different use cases and [documentation](https://github.com/telicent-oss/telicent-rdf-transform/tree/main/docs) which can be found on [github](https://github.com/telicent-oss/telicent-rdf-transform/tree/main).

## Features

- **Generic & Configurable**: Define 1-to-many SPARQL queries via YAML configuration
- **Ontology Mapping Support**: Optional resource-to-resource mapping files utilising `owl:equivalentProperty`/`owl:equivalentClass`
- **Ordered Execution**: Queries run in specified order with individual enable/disable controls
- **Multiple RDF Formats**: Supports Turtle, N-Triples, JSON-LD, RDF/XML, N-Quads, and TriG
- **Performance Monitoring**: Detailed timing metrics for all operations

## Quick Start

### Prerequisites

- Python 3.12 or higher

### Installation
```bash
# Run the development setup script
./dev_setup.sh

# Activate the virtual environment
source .venv/bin/activate
```

### Core Library Usage (Python API)

Use the transformation logic directly in any Python application:

```python
from rdf_transform import MapperConfig, load_mapping_graphs, transform_rdf

# Load configuration
config = MapperConfig.from_yaml("config.example.yaml")

# Optionally load the mapping files named in the config (None if there are none)
mapping = load_mapping_graphs(config)

# Read input RDF
with open("input.ttl", "rb") as f:
    input_data = f.read()

# Transform
output_data, metrics = transform_rdf(
    input_data=input_data,
    input_format="text/turtle",
    output_format="text/turtle",
    config=config,
    mapping_graph=mapping
)

# Use the results
print(f"Transformed {metrics['input_triples']} → {metrics['output_triples']} triples")
with open("output.ttl", "wb") as f:
    f.write(output_data)
```

## Configuration

Create a YAML config file to define your transformation.

### Queries

The core of the configuration is a list of SPARQL queries to execute. Both CONSTRUCT and UPDATE (DELETE/INSERT) queries are supported:

```yaml
queries:
  - name: "transform_properties"        # unique identifier (required)
    description: "Map old props to new" # optional commentary on the purpose of query. This is not used in code and is only explanatory
    enabled: true                       # set to false to skip (default: true)
    order: 1                            # execution order, lower runs first (default: 0)
    query: |
      CONSTRUCT {
        ?s ?targetProp ?o .
      }
      WHERE {
        ?s ?sourceProp ?o .
        ?sourceProp owl:equivalentProperty ?targetProp .
      }
```

Queries are sorted by `order` and only enabled queries are executed. Queries execute in order regardless of type - UPDATE queries modify the working graph in place, while CONSTRUCT query results are merged into the final output.

### Mapping Files (Optional)

Any RDF the queries need alongside the input can be named under `mapping_files`. The usual case is a file that maps each old term to its new one with `owl:equivalentProperty` and `owl:equivalentClass`, but an ontology or any other supporting RDF can be listed too.

Every listed file is merged with the input data before the queries run, so your SPARQL queries can match its triples. All of those triples are removed from the output afterwards. A relative path is resolved against the directory of the config file; a URL is used as given.

```yaml
mapping_files:
  - path: ontology_mappings.ttl
    format: text/turtle  # default if not specified
  - path: supporting_ontology.ttl
```

An example of a mapping file is provided below:

```turtle
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix old: <http://example.org/old-ontology#> .
@prefix new: <http://example.org/new-ontology#> .

# Class mappings
old:Person  owl:equivalentClass new:Human .
old:Company owl:equivalentClass new:Organization .

# Property mappings
old:hasName   owl:equivalentProperty new:name .
old:worksFor  owl:equivalentProperty new:employedBy .
```

This is useful when you want to keep your mapping definitions separate from your SPARQL queries, or when the same mappings are shared across multiple configurations.

### Checks (Optional)

A check is a SPARQL SELECT query that must return no rows. Each row it does return is one failure, reported with the check's `message`. For every variable the query selects, such as `?subject`, the message may contain `{subject}`, which is replaced with the value that row binds to it. Values are written as they would be in Turtle: a URI with its configured prefix, such as `ex:Thing`, and a literal in quotes, such as `"text"`.

Checks run after all queries. A check's query sees the transformed output and the mapping files if provided, so a check can consult the mappings without those triples counting as output. The [mapping guide](docs/MAPPING_GUIDE.md#checks) shows how a check reads the mapping files.

```yaml
checks:
  - name: "old_terms_remain"
    description: "Every term from the old ontology should have been replaced"
    message: "{subject} {predicate} {object} .: {object} is still an old-ontology term"
    query: |
      SELECT ?subject ?predicate ?object
      WHERE {
        ?subject ?predicate ?object .
        FILTER(STRSTARTS(STR(?object), "http://example.org/old-ontology#"))
      }
```

If any check returns rows, `transform_rdf` raises `CheckError`, a `ValueError` whose message lists every failure, one per line, in check order and sorted within each check. Checks take `enabled` and `order` like queries. When the configuration loads, each check's query is parsed, must be a SELECT, and every placeholder in its message must be one of the variables it selects; otherwise loading fails naming the check. A variable that a row leaves unbound, for example inside `OPTIONAL`, is written as `UNDEF`.

### Namespaces (Optional)

Namespace prefixes which shall be used in the subsequently defined SPARQL queries. For RDF serializations that support prefixes (e.g. text/turtle), these will be used as PREFIX declarations. Note, you can still make prefix declarations in the SPARQL queries.

```yaml
namespaces:
  rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  rdfs: "http://www.w3.org/2000/01/rdf-schema#"
  xsd: "http://www.w3.org/2001/XMLSchema#"
```

### Error Handling

The library raises exceptions in the following cases:

- **Invalid RDF syntax**: `rdflib.exceptions.ParserError` with details about the parsing failure
- **Empty input**: `ValueError` if the input graph contains no triples
- **Empty output**: `ValueError` if all triples are filtered out after transformation
- **Missing mapping file**: `FileNotFoundError` if a configured mapping file path doesn't exist
- **Invalid SPARQL**: `rdflib` exceptions with query syntax error details
- **Failed checks**: `CheckError`, a `ValueError`, listing one message per offending statement

Queries that produce no results are not errors - they simply contribute no triples to the output.

---
For detailed configuration examples and SPARQL patterns, see **[docs/MAPPING_GUIDE.md](https://github.com/telicent-oss/telicent-rdf-transform/tree/main/docs)**.

## How it Works

When `transform_rdf()` is called, the following steps occur:

1. **Parse input** - The input RDF data is parsed into a working graph.

2. **Merge mapping files** - The triples of every configured mapping file are added to the working graph, so your SPARQL queries can utilise them.

3. **Execute queries** - Enabled queries run in `order` sequence:
   - **UPDATE queries** (DELETE/INSERT) modify the working graph in place
   - **CONSTRUCT queries** generate new triples that are collected into a separate output graph

4. **Remove mapping triples** - The mapping files' triples are removed from the output, so they don't pollute your transformed data.

5. **Run checks** - Each configured check query runs against the output, with the mapping triples reachable as a named graph. Any row returned stops the transformation with a `CheckError`.

6. **Serialize output** - The final graph is serialized to the requested output format.

The working graph approach means UPDATE queries can prepare data for subsequent CONSTRUCT queries, enabling multi-stage transformations.

### Glossary

| Term | Definition |
|------|------------|
| **Working graph** | The in-memory RDF graph containing input data merged with mapping triples. UPDATE queries modify this graph. |
| **Mapping graph** | RDF graph holding the triples of every configured mapping file. |
| **Output graph** | The final RDF graph containing results from CONSTRUCT queries (or the modified working graph if only UPDATE queries are used). |
| **Triple** | A single RDF statement consisting of subject, predicate, and object. |

## Examples

The [`/examples`](https://github.com/telicent-oss/telicent-rdf-transform/tree/main/examples) directory contains four examples demonstrating different use cases:

### basic_mapper

A minimal example showing single-file transformation. Good starting point to understand the library.

```bash
cd examples/basic_mapper
python run.py
```

See [examples/basic_mapper/README.md](https://github.com/telicent-oss/telicent-rdf-transform/blob/main/examples/basic_mapper/README.md) for details.

### batch_file_mapper

Demonstrates batch processing of multiple RDF files in a directory using the `transform_directory` utility.

```bash
cd examples/batch_file_mapper
python transform_countries.py
```

See [examples/batch_file_mapper/README.md](https://github.com/telicent-oss/telicent-rdf-transform/blob/main/examples/batch_file_mapper/README.md) for details.

### ies4_to_iesnext_mapper

A work-in-progress configuration for transforming IES4 ontology data to IES Next. Includes comprehensive SPARQL queries, mapping files, and test fixtures. The configuration for this mapper is being actively developed alongside the development of the IES Next ontology stack.

See [examples/ies4_to_iesnext_mapper/](https://github.com/telicent-oss/telicent-rdf-transform/tree/main/examples/ies4_to_iesnext_mapper) for the configuration and test files.

### telicent_mapper

To be used with the [Telicent CORE](https://telicent.io/the-core-platform/) platform for real-time transformation of RDF streams.

#### Run

```bash
cd examples/telicent_mapper
cp example.env .env
# Edit .env with your Kafka settings
python -m telicent_mapper.mapper
```

#### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MAPPER_NAME` | Name of the mapper | `rdf_transform_mapper` |
| `BOOTSTRAP_SERVERS` | Kafka bootstrap servers | Required |
| `SOURCE_TOPIC` | Input Kafka topic | `knowledge` |
| `TARGET_TOPIC` | Output Kafka topic | `knowledge` |
| `CONFIG_PATH` | Path to YAML config | `mapping-config.yaml` |
| `RDF_OUTPUT_FORMAT` | Output format (MIME type) | `text/turtle` |
| `SECURITY_LABEL_AND_GROUP` | Security label and_group | `urn:telicent:groups:datasets:mapped` |
---
See [examples/telicent_mapper/](https://github.com/telicent-oss/telicent-rdf-transform/tree/main/examples/telicent_mapper) for configuration and test files.

## Architecture

The project is organized into a core library package:

```
rdf_transform/        # Core transformation library (rdflib only)
├── __init__.py       # Public API exports
├── config.py         # Configuration models (MapperConfig, SPARQLQuery)
├── transform.py      # Transformation functions (transform_rdf, etc.)
└── formats.py        # Format utilities (RDF MIME type handling)
```

See **[docs/ARCHITECTURE.md](https://github.com/telicent-oss/telicent-rdf-transform/blob/main/docs/ARCHITECTURE.md)** for detailed architecture documentation with diagrams, data flows, and design patterns.

## Development

### Running Tests
Tests for development can be found in [`/tests`](https://github.com/telicent-oss/telicent-rdf-transform/tree/main/tests).
```bash
# Run all tests (core library + examples)
pytest

# Run with verbose output
pytest -v

# Run specific test directories
pytest tests/                                              # Core library tests
pytest examples/ies4_to_iesnext_mapper/                    # IES4 mapper tests
pytest examples/telicent_mapper/tests/                     # Telicent mapper tests

# Run specific test file
pytest tests/test_transform.py

# Run specific test class or function
pytest tests/test_transform.py::TestTransformRDF
pytest tests/test_transform.py::TestTransformRDF::test_basic_transform
```

### Code Quality

```bash
# Linting and formatting
ruff check .

# Type checking
mypy rdf_transform/

# Pre-commit hooks
pre-commit run --all-files
```

## Documentation

- **[docs/ARCHITECTURE.md](https://github.com/telicent-oss/telicent-rdf-transform/blob/main/docs/ARCHITECTURE.md)** - Architecture diagrams, structure, and design patterns
- **[docs/MAPPING_GUIDE.md](https://github.com/telicent-oss/telicent-rdf-transform/blob/main/docs/MAPPING_GUIDE.md)** - SPARQL patterns and advanced mapping techniques

## API Reference

### Core Functions

| Function | Description |
|----------|-------------|
| `transform_rdf(input_data, input_format, output_format, config, mapping_graph)` | Main entry point. Transforms RDF data and returns `(output_bytes, metrics)` |
| `load_mapping_graphs(config)` | Load every mapping file named in the config into one graph, or `None` if there are none |
| `load_mapping_graph(source, format, config)` | Load one RDF file or URL into a graph |
| `run_checks(output_graph, mapping_graph, config)` | Run the configured checks and return one message per failing row |
| `parse_rdf(data, format, config)` | Parse RDF data into an rdflib Graph |
| `serialize_rdf(graph, output_format)` | Serialize a Graph to bytes |

### Configuration Classes

| Class | Description |
|-------|-------------|
| `MapperConfig` | Main configuration container. Use `MapperConfig.from_yaml(path)` to load |
| `SPARQLQuery` | Individual query definition with name, query, enabled, and order fields |
| `MappingFile` | One `mapping_files` entry, with `path` and `format` |
| `Check` | One `checks` entry, with `name`, `query`, `message`, `enabled` and `order` |

### Exceptions

- **`ValueError`**: Raised when input graph is empty or output graph is empty after transformation
- **`FileNotFoundError`**: Raised when a mapping file path cannot be found
- **`CheckError`**: Raised when a check returns rows; `messages` holds one entry per row
- **`rdflib.exceptions.ParserError`**: Raised when RDF parsing fails (invalid syntax)


## License

Copyright Telicent Ltd. All rights reserved.

## References

- [Telicent Mappers Documentation](https://github.com/telicent-oss/telicent-lib/blob/main/docs/mappers.md)
- [RDFLib Documentation](https://rdflib.readthedocs.io/)
- [SPARQL 1.1 Query Language](https://www.w3.org/TR/sparql11-query/)
- [OWL 2 Web Ontology Language](https://www.w3.org/TR/owl2-overview/)
