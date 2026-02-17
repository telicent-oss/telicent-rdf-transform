# Architecture: Multi-Modal RDF Transformation Pipeline

## Project Structure

```
telicent-rdf-transform/
│
├── rdf_transform/                  CORE LIBRARY
│   ├── __init__.py                 Pure RDF transformation logic
│   ├── config.py                   No Kafka, no I/O, just transforms
│   ├── transform.py                Dependencies: rdflib only
│   └── formats.py
│
├── examples/                       EXAMPLE IMPLEMENTATIONS
│   ├── basic_mapper/               Single-file transformation example
│   ├── batch_file_mapper/          Directory batch processing example
│   ├── ies4_to_iesnext_mapper/     IES4 to IES Next configuration
│   └── telicent_mapper/            Telicent Kafka streaming example
│
├── tests/                          TESTS
│   ├── test_transform.py           Core library unit tests
│   ├── fixtures/                   Test input data
│   └── golden/                     Expected outputs
│
└── docs/                           DOCUMENTATION
    ├── ARCHITECTURE.md
    ├── MAPPING_GUIDE.md
```

## Key Principles

### 1. Core Library Isolation
- `rdf_transform/` contains pure transformation logic
- No I/O, no Kafka, no file handling - just RDF transforms
- Dependencies: rdflib only

### 2. Examples as Reference Implementations
- Each example in `examples/` is self-contained
- Examples demonstrate different integration patterns
- Can be copied and adapted for new use cases

### 3. Dependency Isolation

```
examples/telicent_mapper/
    ↓ depends on
rdf_transform/ + telicent_lib

examples/batch_file_mapper/
    ↓ depends on
rdf_transform/ only

rdf_transform/
    ↓ depends on
rdflib only (minimal deps)
```

## Architecture Diagrams

### High-Level View

```
┌─────────────────────────────────────────────────────────────────┐
│                        YOUR APPLICATION                         │
│  (batch scripts, REST APIs, Kafka pipelines, CLI tools, etc.)   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ imports
                              ▼
                ┌─────────────────────────┐
                │   Core RDF Transform    │
                │                         │
                │   rdf_transform/        │
                │    - config.py          │
                │    - transform.py       │
                │    - formats.py         │
                │                         │
                │   Dependencies:         │
                │    • rdflib only        │
                └─────────────────────────┘
```

### Example Implementations

```
examples/
  ├── basic_mapper/           Single-file transformation
  ├── batch_file_mapper/      Directory batch processing
  ├── ies4_to_iesnext_mapper/ Production IES4→IESNext config
  └── telicent_mapper/        Kafka streaming integration

Each example imports from rdf_transform/ and adds its own integration logic.
```

### Layered Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 3: Applications                                       │
│ • Production Kafka pipeline                                 │
│ • Batch file processing                                     │
│ • Custom integrations (REST APIs, CLI tools, etc.)          │
└─────────────────────────────────────────────────────────────┘
                            ▲
                            │
┌─────────────────────────────────────────────────────────────┐
│ Layer 2: RDF Transform Core                                 │
│ • Configuration loading (YAML → MapperConfig)               │
│ • RDF parsing/serialization (bytes → Graph → bytes)         │
│ • SPARQL query execution (Graph → transformed Graph)        │
│ • Ontology mapping (OWL equivalence handling)               │
│ • Format detection (MIME → rdflib format)                   │
└─────────────────────────────────────────────────────────────┘
                            ▲
                            │
┌─────────────────────────────────────────────────────────────┐
│ Layer 1: Foundation Libraries                               │
│ • rdflib (RDF graph handling)                               │
│ • PyYAML (config parsing)                                   │
│ • Python stdlib                                             │
└─────────────────────────────────────────────────────────────┘
```

## Usage

### As a Library
```python
from rdf_transform import MapperConfig, transform_rdf, load_mapping_graph

config = MapperConfig.from_yaml("config.yaml")
mapping = load_mapping_graph("mappings.ttl", "text/turtle", config)
output, metrics = transform_rdf(input_data, "text/turtle", "text/turtle", config, mapping)
```

### Reference Implementations
The `examples/` directory contains reference implementations for different use cases:
- `basic_mapper/` - Simple single-file transformation
- `batch_file_mapper/` - Batch directory processing
- `ies4_to_iesnext_mapper/` - WIP IES4 to IES Next configuration
- `telicent_mapper/` - Kafka streaming integration

## Data Flow

### Core Transform Process
```
Input RDF (bytes)
     │
     ▼
┌────────────────────────────┐
│ rdf_transform/             │
│ 1. Parse input → Graph     │
│ 2. Merge mapping file      │
│ 3. Execute SPARQL queries  │
│ 4. Remove mapping triples  │
│ 5. Serialize → bytes       │
└────────┬───────────────────┘
         │
         ▼
Output RDF (bytes) + metrics
```

The core library handles pure transformation. Integration code (file I/O, Kafka, HTTP) wraps this core function.

## Testing

Tests for the `rdf_transform` core library are in `tests/`, while tests for individual examples are in their respective folders.
