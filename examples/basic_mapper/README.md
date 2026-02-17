# Basic Mapper Example

A minimal example demonstrating how to use the `rdf_transform` library to transform RDF data using SPARQL CONSTRUCT queries and optional mapping files.

## Contents

- `config.yaml` - Configuration with SPARQL queries and namespace definitions
- `mappings.ttl` - Optional mapping file with `owl:equivalentProperty` and `owl:equivalentClass` assertions
- `input.ttl` - Sample input RDF data
- `run.py` - Script to run the transformation

## Running the Example

From the repository root:

```bash
python examples/basic_mapper/run.py
```

Or from this directory:

```bash
python run.py
```

## How It Works

1. **Input data** (`input.ttl`) contains RDF using a "source" ontology
2. **Mapping file** (`mappings.ttl`) defines equivalences between source and target ontology terms
3. **Config** (`config.yaml`) defines SPARQL queries that use these mappings to transform the data
4. **Output** is RDF using the "target" ontology

### Expected Transformation:

**Input:**
```turtle
:john a src:Person ;
    src:hasName "John Smith" ;
    src:worksFor :acme .
```

**Output:**
```turtle
:john a tgt:Human ;
    tgt:name "John Smith" ;
    tgt:employedBy :acme .
```
