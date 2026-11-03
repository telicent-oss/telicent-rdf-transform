# IES4 to IES-Next Mapper

> **Work in progress.** This configuration is being developed alongside the IES-Next ontologies, which are themselves release candidates. Its queries, its mapping file and the shape of its output will change. Do not rely on it for a finished mapping: review every result, and expect to finish the mapping by hand.

Maps IES4 instance data to IES-Next. `mapping-config.yaml` holds the queries: one generic query replaces classes and properties through the `owl:equivalentClass` and `owl:equivalentProperty` entries in `resource-mapping.ttl`, and the others rewrite IES4 patterns that have a different shape in IES-Next, such as crossings, durations, recurring periods and the `ies:make` relationship. The latter set of mappings are still in development.

`resource-mapping.ttl` is shared with the [extension mapper](../ies4_extension_to_iesnext_mapper/), so there is one list of IES4 to IES-Next equivalences to maintain.
