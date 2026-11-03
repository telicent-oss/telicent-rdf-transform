# IES4 Extension to IES-Next Mapper

> **Work in progress.** This example is being developed alongside the IES-Next ontologies, which are themselves release candidates. Its queries, its checks and the shape of its output will change, and some of its decisions are provisional. Do not rely on it for a finished mapping: review every result, and expect to finish the mapping by hand.

Ports an ontology extension written against IES4 to IES-Next. The input is the extension itself, not instance data: the `rdfs:subClassOf`, `rdfs:subPropertyOf`, `rdfs:domain`, `rdfs:range` and `rdf:type` statements that define your local classes and properties, as described in the IES4 guide on [extending IES](https://github.com/IES-Org/ont-ies/blob/main/docs/user-guides/extending-ies.md).

IES-Next changes how extensions are written. The [IES-Core documentation](https://github.com/IES-Org/ies-core/blob/main/docs/ies-core.md#extending-ies) reserves RDFS subclassing for extensions that bring new relationships or attributes. An extension that only adds a narrower kind of an existing thing is a taxonomy entry: an instance of `skos:Concept` linked to its parent with `skos:broader`. Instance data keeps `rdf:type` as an IES class and names the concept with `ies_core:category`. New relationships and attributes stay RDFS properties, declared with `rdfs:subPropertyOf` under an IES-Next property, with their domain and range mapped.

`mapping-config.yaml` holds the whole transformation: SPARQL UPDATE queries that restructure the extension the IES-Core way and replace every IES4 term with its IES-Next equivalent, and checks that stop the run when a statement has no IES-Next form. Your own terms keep their names and move to the namespace you choose for the IES-Next version of your extension; their labels, comments and any other statements about them are left as they are.

This is not a complete solution. The mapper does the bulk of the work, but in most cases the result needs an ontologist to review and finish it. The rules that decide whether a class stays a class or becomes a concept are fixed and may not suit every extension, powersets are handled provisionally, and an extension can be turned away outright. The cases turned away are listed in [Extensions that cannot be mapped](#extensions-that-cannot-be-mapped).

Instance data that uses the extension is a separate step. `ies4_to_iesnext_mapper` maps IES4 data, but it does not yet know about extension concepts: a record typed with a local class that became a concept keeps that class as its `rdf:type` instead of gaining an `ies_core:category`.

## Contents

- `mapping-config.yaml` - Includes the queries that do the mapping and the checks that stop it
- `run.py` - Runs the configuration on one file
- `fixtures/` - Extensions written following the IES4 guide
- `golden/` - The expected IES-Next output for each fixture
- `fixtures/rejected/` - Extensions that cannot be mapped, one case each
- `golden/errors/` - The message the checks stop with for each of those
- `ontologies/` - Copies of the IES-Top and IES-Core ontologies at v0.1.0 (RC1), which the checks read
- `test_ies4_extension_to_iesnext_mappings.py` - Tests

The IES4 to IES-Next equivalences come from [ies4_to_iesnext_mapper/resource-mapping.ttl](../ies4_to_iesnext_mapper/resource-mapping.ttl), shared with the data mapper so there is one list to maintain. The IES-Next ontologies under `ontologies/` are listed as mapping files too, so the checks can tell a SKOS concept from a class; `ontologies/README.md` records their source and version.

## Running it

From the repository root:

```bash
python examples/ies4_extension_to_iesnext_mapper/run.py my_extension.ttl my_extension_iesnext.ttl
```

Input:

```turtle
ont:PassengerShip a rdfs:Class ;
    rdfs:subClassOf ies:Ship ;
    rdfs:label "Passenger Ship" .
```

Output:

```turtle
ont_next:PassengerShip a skos:Concept ;
    skos:broader ies_tport:Ship ;
    rdfs:label "Passenger Ship" .
```

A property keeps its shape and has its IES4 terms replaced. Input:

```turtle
ont:motherOf a owl:ObjectProperty ;
    rdfs:subPropertyOf ies:parentOf ;
    rdfs:domain ies:Person ;
    rdfs:range ies:Person .
```

Output:

```turtle
ont_next:motherOf a owl:ObjectProperty ;
    rdfs:subPropertyOf ies_social:parentOf ;
    rdfs:domain ies_core:Person ;
    rdfs:range ies_core:Person .
```

When a check fails nothing is written. The run prints every failure and exits with a non-zero status:

```
The extension cannot be mapped:
ont_next:ragConfidence rdfs:subPropertyOf ies:confidence .: ies:confidence is not a property in IES-Next. It is now a class: ies_sec:AssessToBeTrue. There is no IES-Next property for ont_next:ragConfidence to specialise; remodel it against ies_sec:AssessToBeTrue.
```

## What happens to each local term

A local term is any term outside the IES4 namespace, the W3C vocabularies and the IES-Next namespaces, so the queries work on any extension. Every local term ends up in one of the roles below.

| Role | How it is recognised | Becomes |
|------|----------------------|---------|
| Class | It is the `rdfs:domain` or `rdfs:range` of a local property, is typed with an IES4 class, is the `rdf:type` of another local class, or is named by `ies:powertype` | Stays an RDFS class. `rdfs:subClassOf` and `rdf:type` are kept and their IES4 terms are mapped. |
| Concept | Any other class | `skos:Concept`. Each `rdfs:subClassOf` becomes `skos:broader`, pointing at the mapped IES4 parent or at the local parent, which may itself be a concept or a class. Any `rdf:type` other than `rdfs:Class` is kept. |
| Property | It is typed as a property, or is the subject of `rdfs:subPropertyOf`, `rdfs:domain` or `rdfs:range` | Stays a property. `rdfs:subPropertyOf`, `rdfs:domain` and `rdfs:range` are kept and their IES4 terms are mapped. |

`rdfs:label` and `rdfs:comment` are kept on everything.

### Your namespace

The IES-Next version of yours extensions will need to live in a new namespace. The last queries in the config move every local term from the old namespace to the new one, keeping its name: the example moves `http://example.com/local-ontology#` to `http://example.com/local-ontology-next#`, so `ont:PassengerShip` comes out as `ont_next:PassengerShip`. To use the mapper on your own extension, change the two namespace strings in those queries and bind a prefix for the new namespace under `namespaces:` so the output and the check messages use it. Note: the namespace transformation happens last, so a check reports a rejected statement with the new prefix.

See `fixtures/mixed_class_and_concept.ttl` for a class and a concept side by side.

### Powersets and powertypes: provisional

IES4 and IES-Next both use powersets, which have no neat equivalent in SKOS's way of looking at the world. As a result the target form for extensions that use powersets is not settled, and the mapper's current handling is a placeholder chosen to lose nothing rather than a considered design.

What the mapper does today: a local powerset stays an RDFS class; the classes typed with it keep that typing when they become concepts; and a powertype statement is carried over term for term. See `fixtures/faceted_powersets.ttl` and its golden file for an illustration.

The open questions:

1. IES-Core reserves RDFS classes for extensions that bring new relationships or attributes. Whether a scheme for classifying classes counts has not been decided with the IES-Core maintainers.
2. A concept typed with a powerset class, such as `ont:PassengerShip a skos:Concept, ont:ClassOfFunctionalUseOfShip`, mixes a SKOS taxonomy with IES-Top's set typing. IES-Core does not type its own concepts that way.
3. Expressing facets as `skos:Collection` may want to be considered.

### Why a class typed with an IES4 class stays a class

IES4 lets a class be typed with an IES4 class, typically a powerset. `ont:PassengerShip a ies:ClassOfAsset` says PassengerShip is a class of asset, since `ies:ClassOfAsset` is the powerset of `ies:Asset`. Read strictly, the IES-Core regime would make PassengerShip a taxonomy entry, since the typing adds no relationships or attributes. The mapper keeps it as a class instead, for two reasons.

1. The typing is an explicit IES statement by the author. Keeping it and mapping it is exactly what the data mapper does with the same statement, so both mappers treat it alike.
2. Making it a concept would mean either dropping the typing silently or inventing a convention for concepts that carry IES types, which IES-Core does not do for its own concepts.

One workaround: because any subclass of an IES4 class is already a member of that class's powerset, so an author who wants a concept can remove the typing line and run the mapper again. See `fixtures/class_typed_with_ies4_class.ttl`.

## The queries

The queries run in order on the working graph. Each one is a DELETE/INSERT whose WHERE clause states the condition in the table above.

1. `taxonomy_classes_become_concepts` rewrites every local class that is not a domain or range, not typed with an IES4 class, not the type of another local class and not named by `ies:powertype`, into a concept with `skos:broader`.
2. `map_classes_in_object_position` replaces an IES4 class in the object of `rdf:type`, `rdfs:subClassOf`, `rdfs:domain`, `rdfs:range` or `skos:broader` on a local subject, using `owl:equivalentClass`.
3. `map_properties_in_object_position` replaces an IES4 property in the object of `rdfs:subPropertyOf` on a local subject, using `owl:equivalentProperty`.
4. `map_properties_in_predicate_position` replaces an IES4 property used as a predicate, which is how `ies:powertype` becomes `ies_top:powertype`.
5. `map_classes_in_powertype_subject_position` replaces the IES4 class that a powertype statement names as the subject.
6. `move_local_subjects_to_the_new_namespace` and
7. `move_local_objects_to_the_new_namespace` move every local term from the old namespace to the new one.

Where the mapping file lists more than one IES-Next term for an IES4 term, the output contains one triple for each. Apart from powertype statements, statements whose subject is an IES4 term are left as written, so that a check can report them as the author wrote them.

## Extensions that cannot be mapped

Some things an IES4 extension can say have no IES-Next form, and the mapper does not guess. Each case below has an extension under `fixtures/rejected/` showing it, a check in `mapping-config.yaml` that catches it, and a file under `golden/errors/`, named `<fixture>_error.txt`, holding the exact message. Every message has the same shape, one line per offending statement: the statement, a colon, and the explanation. Each is built only from the extension, the mapping file and the IES-Next ontologies, which is why a check can produce it. The tests run every rejected fixture and compare the message with its golden.

| Fixture | What is wrong | Check |
|---------|---------------|-------|
| `class_declared_in_ies4_namespace.ttl` | A new class is declared inside the IES4 namespace instead of a local one | `terms_declared_in_ies4_namespace` |
| `attribute_that_became_a_class.ttl` | The extension builds on `ies:confidence`, which is a class pattern in IES-Next, so the mapping file maps it to a class and has no property for it | `property_that_became_a_class` |
| `uses_owl_equivalence.ttl` | The extension uses `owl:equivalentClass`, which is reserved for the mapping file | `owl_equivalence_in_extension` |
| `unmapped_term.ttl` | The extension builds on `ies:Entity`, which the mapping file has no entry for | `ies4_term_without_mapping_entry` |
| `class_under_concept.ttl` | A class with its own attribute subclasses `ies:CommercialOrganisation`, which is a SKOS concept in IES-Next, so there is no class to subclass | `class_under_an_iesnext_concept` |

The last case needs the checks to know which IES-Next terms are SKOS concepts and what each is narrower than. Those facts come from the IES-Core ontology under `ontologies/`, read through the mapping graph. The author has two ways out, and the message gives both: keep the class and put it under the concept's IES class, with the concept applied to the data as `ies_core:category`, or make the class a concept under the IES-Next concept and move its properties' domain up to the IES class.

Not yet covered by a fixture or check: a local class with no `rdfs:subClassOf` at all, which stays an RDFS class.

## Tests

```bash
pytest examples/ies4_extension_to_iesnext_mapper/
```
