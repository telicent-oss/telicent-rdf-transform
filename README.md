# telicent-rdf-transform

A python library for transforming RDF data from one form to another; in most cases: data from one ontology to another. Configuration of this transformation is provided as a series of SPARQL queries. This library has been built so it can be used in multiple contexts: streaming, batch files, APIs, etc.

Within this repo we provide the library (see `/rdf_transform`) and examples (see `/examples`) demonstrating different use cases.

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
from rdf_transform import MapperConfig, load_mapping_graph, transform_rdf

# Load configuration
config = MapperConfig.from_yaml("config.example.yaml")

# Optionally load ontology mappings
mapping = load_mapping_graph("mappings.example.ttl", "text/turtle", config)

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

### Mapping File (Optional)

For simple 1:1 mappings between RDF resources (e.g., mapping one class or property URI to another), you can define these maps using a separate RDF file which utilises `owl:equivalentProperty` or `owl:equivalentClass` assertions.

When provided, the mapping file is temporarily merged with the input data before queries run, allowing your SPARQL queries to reference these relationships. These mapping triples are later, automatically removed from the final output.

```yaml
mapping_file: ontology_mappings.ttl
mapping_file_format: text/turtle  # default if not specified
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
- **Missing mapping file**: `FileNotFoundError` if the configured mapping file path doesn't exist
- **Invalid SPARQL**: `rdflib` exceptions with query syntax error details

Queries that produce no results are not errors - they simply contribute no triples to the output.

---
For detailed configuration examples and SPARQL patterns, see **[docs/MAPPING_GUIDE.md](docs/MAPPING_GUIDE.md)**.

## How it Works

When `transform_rdf()` is called, the following steps occur:

1. **Parse input** - The input RDF data is parsed into a working graph.

2. **Merge mapping file** - If a mapping file is configured, its `owl:equivalentProperty` and `owl:equivalentClass` triples are added to the working graph. This allows your SPARQL queries to reference these `owl:equivalentProperty`/`owl:equivalentClass` relationships.

3. **Execute queries** - Enabled queries run in `order` sequence:
   - **UPDATE queries** (DELETE/INSERT) modify the working graph in place
   - **CONSTRUCT queries** generate new triples that are collected into a separate output graph

4. **Remove mapping triples** - The mapping file triples are removed from the output, so they don't pollute your transformed data.

5. **Serialize output** - The final graph is serialized to the requested output format.

The working graph approach means UPDATE queries can prepare data for subsequent CONSTRUCT queries, enabling multi-stage transformations.

### Glossary

| Term | Definition |
|------|------------|
| **Working graph** | The in-memory RDF graph containing input data merged with mapping triples. UPDATE queries modify this graph. |
| **Mapping graph** | RDF graph loaded from the mapping file, containing `owl:equivalentProperty` and `owl:equivalentClass` assertions. |
| **Output graph** | The final RDF graph containing results from CONSTRUCT queries (or the modified working graph if only UPDATE queries are used). |
| **Triple** | A single RDF statement consisting of subject, predicate, and object. |

## Examples

The `/examples` directory contains four examples demonstrating different use cases:

### basic_mapper

A minimal example showing single-file transformation. Good starting point to understand the library.

```bash
cd examples/basic_mapper
python run.py
```

See [examples/basic_mapper/README.md](examples/basic_mapper/README.md) for details.

### batch_file_mapper

Demonstrates batch processing of multiple RDF files in a directory using the `transform_directory` utility.

```bash
cd examples/batch_file_mapper
python transform_countries.py
```

See [examples/batch_file_mapper/README.md](examples/batch_file_mapper/README.md) for details.

### ies4_to_iesnext_mapper

A work-in-progress configuration for transforming IES4 ontology data to IES Next. Includes comprehensive SPARQL queries, mapping files, and test fixtures. The configuration for this mapper is being actively developed alongside the development of the IES Next ontology stack.

See [examples/ies4_to_iesnext_mapper/](examples/ies4_to_iesnext_mapper/) for the configuration and test files.

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
See [examples/telicent_mapper/](examples/telicent_mapper/) for configuration and test files.

## Architecture

The project is organized into a core library package:

```
rdf_transform/        # Core transformation library (rdflib only)
├── __init__.py       # Public API exports
├── config.py         # Configuration models (MapperConfig, SPARQLQuery)
├── transform.py      # Transformation functions (transform_rdf, etc.)
└── formats.py        # Format utilities (RDF MIME type handling)
```

See **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** for detailed architecture documentation with diagrams, data flows, and design patterns.

## Development

### Running Tests

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

- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** - Architecture diagrams, structure, and design patterns
- **[docs/MAPPING_GUIDE.md](docs/MAPPING_GUIDE.md)** - SPARQL patterns and advanced mapping techniques

## API Reference

### Core Functions

| Function | Description |
|----------|-------------|
| `transform_rdf(input_data, input_format, output_format, config, mapping_graph)` | Main entry point. Transforms RDF data and returns `(output_bytes, metrics)` |
| `load_mapping_graph(source, format, config)` | Load ontology mappings from a file path or URL |
| `parse_rdf(data, format, config)` | Parse RDF data into an rdflib Graph |
| `serialize_rdf(graph, output_format)` | Serialize a Graph to bytes |

### Configuration Classes

| Class | Description |
|-------|-------------|
| `MapperConfig` | Main configuration container. Use `MapperConfig.from_yaml(path)` to load |
| `SPARQLQuery` | Individual query definition with name, query, enabled, and order fields |

### Exceptions

- **`ValueError`**: Raised when input graph is empty or output graph is empty after transformation
- **`FileNotFoundError`**: Raised when mapping file path cannot be found
- **`rdflib.exceptions.ParserError`**: Raised when RDF parsing fails (invalid syntax)


## License

Copyright Telicent Ltd. All rights reserved.

## References

- [Telicent Mappers Documentation](https://github.com/telicent-oss/telicent-lib/blob/main/docs/mappers.md)
- [RDFLib Documentation](https://rdflib.readthedocs.io/)
- [SPARQL 1.1 Query Language](https://www.w3.org/TR/sparql11-query/)
- [OWL 2 Web Ontology Language](https://www.w3.org/TR/owl2-overview/)
