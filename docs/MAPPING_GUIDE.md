# RDF Mapping Guide

This guide provides detailed examples and SPARQL patterns for transforming RDF data from one form to another.

## Overview

This guide covers:

- Writing SPARQL CONSTRUCT queries for data transformation
- Using SPARQL UPDATE (DELETE/INSERT) queries to modify data in place
- Working with optional mapping files for simple 1:1 resource mappings
- Advanced patterns for complex transformations

## How It Works

### 1. Create a Mapping File

Create an RDF file (Turtle format recommended) defining your mappings:

**mappings.example.ttl:**
```turtle
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix src: <http://example.org/source#> .
@prefix tgt: <http://example.org/target#> .

# Property mappings
src:birthDate   owl:equivalentProperty  tgt:dateOfBirth .
src:age         owl:equivalentProperty  tgt:age .
src:fullName    owl:equivalentProperty  tgt:name .

# Class mappings
src:Person      owl:equivalentClass     tgt:Human .
src:Company     owl:equivalentClass     tgt:Organization .
```

### 2. Configure Mapping Files

If required, list the mapping file in your config YAML file:

```yaml
# config.example.yaml
mapping_files:
  - path: mappings.example.ttl
    format: text/turtle  # Optional, defaults to text/turtle

# Namespaces - primarily for output formatting, secondarily for auto-prefixing queries
# If your queries include PREFIX declarations, these are only needed for clean output
namespaces:
  src: "http://example.org/source#"
  tgt: "http://example.org/target#"
  owl: "http://www.w3.org/2002/07/owl#"

queries:
  # ... your queries
```

If no mapping files are configured, the transformation runs normally without mappings. More than one file can be listed, for example a supporting ontology next to the mappings file. All are merged before the queries run and removed from the output.

### 3. Write Generic SPARQL Queries

Create queries that use `owl:equivalentProperty` and `owl:equivalentClass`.

Your complete config file would look like:
```yaml
namespaces:
  src: "http://example.org/source#"
  tgt: "http://example.org/target#"
  owl: "http://www.w3.org/2002/07/owl#"

queries:
  # Generic property mapping
  - name: "map_properties"
    enabled: true
    order: 1
    query: |
      CONSTRUCT {
        ?subject ?targetProp ?object .
      }
      WHERE {
        ?subject ?sourceProp ?object .
        ?sourceProp owl:equivalentProperty ?targetProp .
      }

  # Generic class mapping
  - name: "map_classes"
    enabled: true
    order: 2
    query: |
      CONSTRUCT {
        ?subject a ?targetClass .
      }
      WHERE {
        ?subject a ?sourceClass .
        ?sourceClass owl:equivalentClass ?targetClass .
      }
```

## Example Use Cases

### Use Case 1: Simple Property Transformation

**Input Data:**
```turtle
@prefix : <http://example.org/data#> .
@prefix src: <http://example.org/source#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

:john src:birthDate "1990-01-15"^^xsd:date .
:john src:age 34 .
```

**Mapping File:**
```turtle
src:birthDate owl:equivalentProperty tgt:dateOfBirth .
src:age       owl:equivalentProperty tgt:age .
```

**SPARQL Query:**
```sparql
CONSTRUCT {
  ?person ?targetProp ?value .
}
WHERE {
  ?person ?sourceProp ?value .
  ?sourceProp owl:equivalentProperty ?targetProp .
}
```

**Output:**
```turtle
@prefix : <http://example.org/data#> .
@prefix tgt: <http://example.org/target#> .

:john tgt:dateOfBirth "1990-01-15"^^xsd:date .
:john tgt:age 34 .
```

### Use Case 2: Class Transformation

**Input Data:**
```turtle
@prefix : <http://example.org/data#> .
@prefix src: <http://example.org/source#> .

:john a src:Person .
:acme a src:Company .
```

**Mapping File:**
```turtle
src:Person  owl:equivalentClass tgt:Human .
src:Company owl:equivalentClass tgt:Organization .
```

**Output:**
```turtle
:john a tgt:Human .
:acme a tgt:Organization .
```

### Use Case 3: Combined Transformation

Transform both classes and properties in a single query:

```sparql
CONSTRUCT {
  ?subject a ?targetClass .
  ?subject ?targetProp ?object .
}
WHERE {
  ?subject a ?sourceClass .
  ?subject ?sourceProp ?object .
  ?sourceClass owl:equivalentClass ?targetClass .
  ?sourceProp owl:equivalentProperty ?targetProp .
}
```

## Advanced Patterns

### Pattern 1: Selective Mapping by Entity Type

Only map properties for specific types of entities:

```sparql
CONSTRUCT {
  ?person ?targetProp ?value .
}
WHERE {
  ?person a src:Person .          # Only for Person entities
  ?person ?sourceProp ?value .
  ?sourceProp owl:equivalentProperty ?targetProp .
}
```

### Pattern 2: UPDATE Queries (DELETE/INSERT)

UPDATE queries modify the working graph in place. Use them to delete old triples and insert new ones in a single operation:

```sparql
DELETE {
  ?subject ?sourceProp ?object .
}
INSERT {
  ?subject ?targetProp ?object .
}
WHERE {
  ?subject ?sourceProp ?object .
  ?sourceProp owl:equivalentProperty ?targetProp .
}
```

This is useful when you want to replace properties rather than add new ones alongside the originals.

### Pattern 3: Preserve Unmapped Data

Copy through data that doesn't have mappings:

```sparql
CONSTRUCT {
  ?s ?p ?o .
}
WHERE {
  ?s ?p ?o .

  # Only keep triples with no mapping
  FILTER NOT EXISTS {
    ?p owl:equivalentProperty ?anyProp .
  }
}
```

### Pattern 4: Multiple Mapping Files

You can merge multiple concepts by adding more mappings to your file:

```turtle
# Property mappings
src:birthDate owl:equivalentProperty tgt:dateOfBirth .
src:worksFor  owl:equivalentProperty tgt:employedBy .

# Class mappings
src:Person    owl:equivalentClass tgt:Human .

# Inverse relationships (if needed)
tgt:employedBy owl:inverseOf tgt:employs .
```

### Pattern 5: UNION for Different Entity Types

```sparql
CONSTRUCT {
  ?subject ?targetProp ?object .
}
WHERE {
  {
    ?subject a src:Person .
    ?subject ?sourceProp ?object .
    ?sourceProp owl:equivalentProperty ?targetProp .
  }
  UNION
  {
    ?subject a src:Company .
    ?subject ?sourceProp ?object .
    ?sourceProp owl:equivalentProperty ?targetProp .
  }
}
```

## How the Mapper Processes Mappings

1. **Startup**: Mapper loads mapping file into a `mapping_graph`
2. **Per Record**:
   - Parse input RDF data into `input_graph`
   - Merge `mapping_graph` + `input_graph` → `query_graph`
   - Execute SPARQL queries against `query_graph`
   - Queries can reference mapping triples via `owl:equivalentProperty`/`Class`
3. **Output**: Only constructed triples are returned (mappings are not included in output)

## Debugging Tips

### Test Mapping File Separately

Validate your mapping file syntax using Python:

```python
from rdflib import Graph

g = Graph()
g.parse("mappings.example.ttl", format="turtle")
print(f"Loaded {len(g)} triples")
```

### Query the Merged Graph

To see what mappings are available to your queries, run a SELECT query:

```python
from rdflib import Graph

# Load your input + mappings
g = Graph()
g.parse("input.ttl", format="turtle")
g.parse("mappings.ttl", format="turtle")

# Query to see available mappings
results = g.query("""
    SELECT ?sourceProp ?targetProp
    WHERE {
        ?sourceProp owl:equivalentProperty ?targetProp .
    }
""")
for row in results:
    print(f"{row.sourceProp} -> {row.targetProp}")
```

## Best Practices

1. **One Triple Per Mapping**: Keep mappings simple and atomic
2. **Document Mappings**: Add `rdfs:comment` annotations
3. **Version Control**: Track mapping file changes in git
4. **Namespace Consistency**: Use consistent prefixes across mapping file and config
5. **Test Incrementally**: Add a few mappings at a time and test

## Common Issues

### Issue: No Output Triples

**Cause**: Namespace mismatch between data, mapping file, and queries

**Solution**: Ensure all use the same namespace URIs:
```turtle
# In mapping file
@prefix src: <http://example.org/source#> .

# In your config file
namespaces:
  src: "http://example.org/source#"

# In data
:john src:birthDate "1990-01-15" .  # Must use same src: prefix
```

### Issue: Mapping File Not Loaded

**Cause**: File path not found or format incorrect

**Solution**: Ensure the path is correct (relative to working directory or use absolute path). The library will raise a `FileNotFoundError` if the mapping file cannot be found.

### Issue: Queries Still Use Old Approach

**Cause**: Mixing old URI replacement approach with new mapping approach

**Solution**: Choose one approach:
- **Old**: Use `REPLACE(STR(?uri), "old", "new")`
- **New**: Use `owl:equivalentProperty` with mapping file

Don't mix both.

## Migration from URI Replacement

**Before (URI replacement in queries):**
```sparql
CONSTRUCT {
  ?s2 ?p2 ?o2 .
}
WHERE {
  ?s ?p ?o .
  BIND(IRI(REPLACE(STR(?s), "src#", "tgt#")) AS ?s2)
  BIND(IRI(REPLACE(STR(?p), "src#", "tgt#")) AS ?p2)
  BIND(IRI(REPLACE(STR(?o), "src#", "tgt#")) AS ?o2)
}
```

**After (mapping file approach):**

**mappings.example.ttl:**
```turtle
src:property1 owl:equivalentProperty tgt:property1 .
src:Class1    owl:equivalentClass tgt:Class1 .
```

**config.example.yaml:**
```sparql
CONSTRUCT {
  ?s ?targetProp ?o .
}
WHERE {
  ?s ?sourceProp ?o .
  ?sourceProp owl:equivalentProperty ?targetProp .
}
```

## Checks

A check is a SELECT query in the `checks` section of the config that must return no rows once the queries have run. Use checks to make a transformation fail loudly instead of passing problems through: a term from the source ontology still present in the output, a statement with no counterpart in the target, or a mapping entry that points at the wrong kind of thing.

Each row a check returns becomes one message, built from the check's `message`: wherever the message says `{subject}`, the value the row binds to the query variable `?subject` is written in, with its configured prefix.

A check normally looks only at the output. This one reports every statement whose object is still a term from the source ontology:

```yaml
checks:
  - name: "source_terms_remain"
    message: "{subject} {predicate} {object} .: {object} was not mapped"
    query: |
      SELECT ?subject ?predicate ?object
      WHERE {
        ?subject ?predicate ?object .
        FILTER(STRSTARTS(STR(?object), "http://example.org/source#"))
      }
```

The mapping files are not part of the output, so that check never sees them. A check that needs to read them wraps the pattern in `GRAPH <urn:rdf-transform:mapping> { ... }`, which is the name the mapping files are kept under. This one finds a source property that the mapping file maps to a class rather than to a property:

```yaml
checks:
  - name: "property_mapped_to_a_class"
    message: "{subject} rdfs:subPropertyOf {property} .: {property} is mapped to the class {class}, not to a property"
    query: |
      SELECT ?subject ?property ?class
      WHERE {
        ?subject rdfs:subPropertyOf ?property .
        GRAPH <urn:rdf-transform:mapping> {
          ?property owl:equivalentClass ?class .
          FILTER NOT EXISTS {
            ?property owl:equivalentProperty ?anyProperty .
          }
        }
      }
```

A failing check raises `CheckError` from `transform_rdf`, with every message from every check, so one run reports everything that is wrong.
