"""Tests for IES4 to BFO/CCO mappings.

Tests the transformation of IES4 ontology data to BFO 2.0 + CCO format,
validating SPARQL UPDATE queries against golden files.
"""

import unittest
from pathlib import Path

from rdflib import Graph

from rdf_transform import MapperConfig
from rdf_transform.transform import load_mapping_graph, transform_rdf

FIXTURES_DIR = Path(__file__).parent / "data" / "fixtures"
GOLDEN_DIR = Path(__file__).parent / "data" / "golden"
PROJECT_ROOT = Path(__file__).parent.parent.parent


class TestIES4ToBFOMappings(unittest.TestCase):
    """Tests for IES4 to BFO/CCO mappings using the main config."""

    def setUp(self):
        """Set up with the main IES4 to BFO config."""
        config_path = PROJECT_ROOT / "examples" / "ies4_to_bfo_mapper" / "mapping-config.yaml"
        self.config = MapperConfig.from_yaml(str(config_path))

        mapping_file = config_path.parent / self.config.mapping_file
        self.mapping_graph = load_mapping_graph(
            str(mapping_file),
            self.config.mapping_file_format or "turtle",
            self.config,
        )

    def _test_mapping(self, fixture_file, golden_file):
        """Run a mapping and compare output against a golden file."""
        fixture_path = FIXTURES_DIR / fixture_file
        with open(fixture_path, "rb") as f:
            input_data = f.read()

        output_data, _metrics = transform_rdf(
            input_data=input_data,
            input_format="text/turtle",
            output_format="text/turtle",
            config=self.config,
            mapping_graph=self.mapping_graph,
        )

        output_graph = Graph()
        output_graph.parse(data=output_data, format="turtle")
        print(output_data.decode("utf-8"))
        self.assertGreater(len(output_graph), 0, "Output graph should have triples")

        golden_path = GOLDEN_DIR / golden_file
        golden_graph = Graph()
        golden_graph.parse(golden_path, format="turtle")

        self.assertTrue(
            output_graph.isomorphic(golden_graph),
            f"Output differs from golden file.\n"
            f"Expected {len(golden_graph)} triples, got {len(output_graph)} triples.\n"
            f"Expected:\n{golden_graph.serialize(format='turtle')}\n"
            f"Got:\n{output_graph.serialize(format='turtle')}",
        )

    def test_person_mapping(self):
        self._test_mapping("person.ttl", "person_bfo.ttl")

    def test_full_example_mapping(self):
        self._test_mapping("full_example.ttl", "full_example_bfo.ttl")

    def test_participation_mapping(self):
        self._test_mapping("participation.ttl", "participation_bfo.ttl")

    def test_person_with_representations_mapping(self):
        self._test_mapping("person_representations.ttl", "person_representations_bfo.ttl")

    def test_event_with_period_mapping(self):
        self._test_mapping("time.ttl", "time_bfo.ttl")

    def test_state_mapping(self):
        self._test_mapping("state.ttl", "state_bfo.ttl")


if __name__ == "__main__":
    unittest.main()
