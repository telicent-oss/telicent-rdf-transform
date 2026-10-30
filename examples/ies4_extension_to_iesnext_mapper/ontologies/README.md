# IES-Next ontology files

Unmodified copies of the IES-Next ontologies, listed under `mapping_files` in `../mapping-config.yaml` so the checks can see which IES-Next terms are SKOS concepts and what each is narrower than. They are merged into the working graph before the queries run and removed from the output afterwards.

| File | Source | Tag | Commit | Date |
|------|--------|-----|--------|------|
| `ies-top.ttl` | https://github.com/IES-Org/ies-top, `spec/ies-top.ttl` | `v0.1.0_RC1` | `5701a1b` | 2025-11-05 |
| `ies-core.ttl` | https://github.com/IES-Org/ies-core, `spec/ies-core.ttl` | `v0.1.0_RC1` | `fb18375` | 2025-11-11 |

Both declare `owl:versionInfo "0.1.0 (RC1)"`. They are published under the licences in those repositories, `LICENSE.md` and `OGL_LICENSE.md` at the tag. To move to a later release, replace the files with the tagged versions and update this table.
