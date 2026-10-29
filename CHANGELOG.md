# Changelog

## [1.0.0](https://github.com/telicent-oss/telicent-rdf-transform/compare/v0.2.0...v1.0.0) (2026-10-29)


### ⚠ BREAKING CHANGES

* replace mapping_file with a mapping_files list; supporting multiple RDFs that sort a mapping

### Features

* add output checks that fail a transformation ([1a2e637](https://github.com/telicent-oss/telicent-rdf-transform/commit/1a2e6374828e48e195e53c0ef84de3847a533f62))
* replace mapping_file with a mapping_files list; supporting multiple RDFs that sort a mapping ([e5eaddf](https://github.com/telicent-oss/telicent-rdf-transform/commit/e5eaddfe309593190ae35da8ccbd77af27e6c1ad))


### Bug Fixes

* attach the mapping graph whenever supplied and bind prefixes everywhere ([8b22cfd](https://github.com/telicent-oss/telicent-rdf-transform/commit/8b22cfd7cc052e6e7bd128ce91db0652f44e52d1))
* detect GRAPH clauses inside FILTER EXISTS when checking for mapping-graph use ([88092a2](https://github.com/telicent-oss/telicent-rdf-transform/commit/88092a22822d836a2f08477e033da5720c251988))
* make check queries robust against the mapping graph setup ([308a838](https://github.com/telicent-oss/telicent-rdf-transform/commit/308a8385a22a782b19ff594d49c2c0b7c5278817))
* report identical check messages once and keep CheckError pickle-safe ([fc0f780](https://github.com/telicent-oss/telicent-rdf-transform/commit/fc0f780c97dd19d226165c70563336a24c5c0978))
* validate checks when the configuration loads ([13247f4](https://github.com/telicent-oss/telicent-rdf-transform/commit/13247f421d644be721fc517ae550b3da2dadbc81))

## [0.2.0](https://github.com/telicent-oss/telicent-rdf-transform/compare/v0.1.0...v0.2.0) (2026-05-27)


### Features

* add ies-bfo mappings ([15fe03f](https://github.com/telicent-oss/telicent-rdf-transform/commit/15fe03fb212ba2103f108a2a4cd1ffef9bed3e44))


### Miscellaneous

* moved telicent_mapper dependencies into optional-dependencies section ([12a9dca](https://github.com/telicent-oss/telicent-rdf-transform/commit/12a9dca766a5ab34d195542be28102a2343bdf65))
* moved telicent-lib and label-builder dependencies out of toml ([0ef735b](https://github.com/telicent-oss/telicent-rdf-transform/commit/0ef735be438f187428a392f95204e3e553fad936))

## 0.1.0 (2026-04-23)


### Features

* add mapping and test ([052e4fd](https://github.com/telicent-oss/telicent-rdf-transform/commit/052e4fd6324122df721e49a429f3c582ff7708cf))
* add publish to pypi artefacts ([f240e8b](https://github.com/telicent-oss/telicent-rdf-transform/commit/f240e8be8025ecd3a4d1df9bcdc637c3c0860056))
* first commit ([dafa6df](https://github.com/telicent-oss/telicent-rdf-transform/commit/dafa6dfe3a7c7f1f6556605951cae2de61d8c851))


### Bug Fixes

* namespace, creation ([15b68c0](https://github.com/telicent-oss/telicent-rdf-transform/commit/15b68c0730f55e777062af04b0752acd0bb54734))
* remove make equivalent class ([0f7ff56](https://github.com/telicent-oss/telicent-rdf-transform/commit/0f7ff568974d97a76e1ea0e924e0b791b8395574))
* typo in mapping ([fe1373c](https://github.com/telicent-oss/telicent-rdf-transform/commit/fe1373c55c24cc357dce85f018a4644f5ec5d30c))
* update dependencies ([1c4e5a2](https://github.com/telicent-oss/telicent-rdf-transform/commit/1c4e5a2fc315b57a303b867a4dcbdb44c892eba9))
* update version ([e17e09d](https://github.com/telicent-oss/telicent-rdf-transform/commit/e17e09d076a613488ba18ab69483e96930171e31))
