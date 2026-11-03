"""Tests for mapping IES4 ontology extensions to IES-Next.

Each fixture is an extension written the way the IES4 "Extending IES" guide describes. For a
fixture the mapper accepts, the golden file is the same extension expressed the way the IES-Core
"Extending IES" section describes, with SKOS concepts and collections. For a fixture under
fixtures/rejected/, the golden file under golden/errors/ is the message the checks stop with.
"""

import unittest
from pathlib import Path

from rdflib import Graph, URIRef

from rdf_transform import CheckError, MapperConfig, load_mapping_graphs, transform_rdf

EXAMPLE_DIR = Path(__file__).parent
FIXTURES_DIR = EXAMPLE_DIR / "fixtures"
GOLDEN_DIR = EXAMPLE_DIR / "golden"
LOCAL_NAMESPACE = "http://example.com/local-ontology#"
NEW_LOCAL_NAMESPACE = "http://example.com/local-ontology-next#"
FIXTURES = [
    "simple_classes.ttl",
    "relationships_and_attributes.ttl",
    "faceted_powersets.ttl",
    "entity_and_state.ttl",
    "mixed_class_and_concept.ttl",
    "class_typed_with_ies4_class.ttl",
]


class TestIES4ExtensionToIESNext(unittest.TestCase):
    config: MapperConfig
    mapping_graph: Graph | None

    @classmethod
    def setUpClass(cls) -> None:
        cls.config = MapperConfig.from_yaml(EXAMPLE_DIR / "mapping-config.yaml")
        cls.mapping_graph = load_mapping_graphs(cls.config)

    def _transform(self, fixture: Path) -> Graph:
        output_data, _ = transform_rdf(
            input_data=fixture.read_bytes(),
            input_format="text/turtle",
            output_format="text/turtle",
            config=self.config,
            mapping_graph=self.mapping_graph,
        )
        return Graph().parse(data=output_data, format="turtle")

    def _assert_matches_golden(self, name: str) -> None:
        output = self._transform(FIXTURES_DIR / name)
        golden = Graph().parse(GOLDEN_DIR / name, format="turtle")
        self.assertTrue(
            output.isomorphic(golden),
            f"Output differs from golden file {name}.\n"
            f"Expected:\n{golden.serialize(format='turtle')}\n"
            f"Got:\n{output.serialize(format='turtle')}",
        )

    def _assert_rejected(self, name: str) -> None:
        with self.assertRaises(CheckError) as raised:
            self._transform(FIXTURES_DIR / "rejected" / name)
        expected = (GOLDEN_DIR / "errors" / f"{Path(name).stem}_error.txt").read_text(encoding="utf-8")
        self.assertEqual(str(raised.exception), expected.rstrip("\n"))

    def test_simple_classes(self) -> None:
        self._assert_matches_golden("simple_classes.ttl")

    def test_relationships_and_attributes(self) -> None:
        self._assert_matches_golden("relationships_and_attributes.ttl")

    def test_faceted_powersets(self) -> None:
        self._assert_matches_golden("faceted_powersets.ttl")

    def test_entity_and_state(self) -> None:
        self._assert_matches_golden("entity_and_state.ttl")

    def test_mixed_class_and_concept(self) -> None:
        self._assert_matches_golden("mixed_class_and_concept.ttl")

    def test_class_typed_with_ies4_class(self) -> None:
        self._assert_matches_golden("class_typed_with_ies4_class.ttl")

    def test_local_terms_are_kept_under_the_new_namespace(self) -> None:
        for name in FIXTURES:
            fixture_names = _local_names(Graph().parse(FIXTURES_DIR / name, format="turtle"), LOCAL_NAMESPACE)
            output_names = _local_names(self._transform(FIXTURES_DIR / name), NEW_LOCAL_NAMESPACE)
            self.assertEqual(fixture_names, output_names, name)
            self.assertEqual(_local_names(self._transform(FIXTURES_DIR / name), LOCAL_NAMESPACE), set(), name)

    def test_rejects_class_declared_in_ies4_namespace(self) -> None:
        self._assert_rejected("class_declared_in_ies4_namespace.ttl")

    def test_rejects_attribute_that_became_a_class(self) -> None:
        self._assert_rejected("attribute_that_became_a_class.ttl")

    def test_rejects_owl_equivalence(self) -> None:
        self._assert_rejected("uses_owl_equivalence.ttl")

    def test_rejects_unmapped_term(self) -> None:
        self._assert_rejected("unmapped_term.ttl")

    def test_rejects_class_under_concept(self) -> None:
        self._assert_rejected("class_under_concept.ttl")


def _local_names(graph: Graph, namespace: str) -> set[str]:
    return {
        term[len(namespace) :]
        for triple in graph
        for term in triple
        if isinstance(term, URIRef) and term.startswith(namespace)
    }


if __name__ == "__main__":
    unittest.main()
