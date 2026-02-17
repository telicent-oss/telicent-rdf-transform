# Batch File Mapper Example

Demonstrates batch directory processing with the `rdf_transform` library, transforming multiple RDF files from IES4 ontology to IES Next ontology.

## Contents

- `transform_countries.py` - Script to run the batch transformation
- `config.yaml` - Configuration with SPARQL UPDATE queries
- `mappings.ttl` - RDF mapping file with `owl:equivalentProperty` and `owl:equivalentClass` assertions
- `mapper.py` - Utility functions for batch processing (`transform_file`, `transform_directory`)
- `input/` - Directory containing sample input files:
  - `countries.ttl` - UK, USA, and Australia country data
  - `regions.ttl` - Northern Europe, Northern America, and Australia/New Zealand region data
- `output/` - Directory where transformed files are written (created on first run)

## Running the Example

From the repository root:

```bash
python examples/batch_file_mapper/transform_countries.py
```

Or from this directory:

```bash
python transform_countries.py
```

### Expected output

```
Batch File Mapper Example - Directory Transformation
=======================================================
Loaded config: 4 queries
Loaded mappings: 712 triples

Processing files in: .../examples/batch_file_mapper/input
-------------------------------------------------------
  regions.ttl: 24 -> 24 triples (0.182s)
  countries.ttl: 48 -> 48 triples (0.063s)
-------------------------------------------------------
Total: 72 -> 72 triples across 2 files
Total time: 0.245s
Output directory: .../examples/batch_file_mapper/output
```

## How It Works

The example uses `transform_directory()` from `mapper.py` to:

1. Find all `.ttl` files in the `input/` directory
2. Transform each file using `config.yaml`, which also utilises a mapping file (`mappings.ttl`)
3. Write transformed files to the `output/` directory
4. Report metrics for each file processed

## Batch Processing Utilities

The `mapper.py` module provides two utility functions:

```python
from examples.batch_file_mapper.mapper import transform_file, transform_directory
from rdf_transform import MapperConfig, load_mapping_graph

config = MapperConfig.from_yaml("config.yaml")
mapping = load_mapping_graph("mappings.ttl", "text/turtle", config)

# Transform a single file
metrics = transform_file("input.ttl", "output.ttl", config, mapping)

# Transform all .ttl files in a directory
results = transform_directory("data/", "output/", config, mapping, pattern="*.ttl")
```
