"""Tests for IES4 to IES Next complex mappings.

Tests the transformation of IES4 ontology data to IES Next format,
validating complex SPARQL CONSTRUCT queries against golden files.
"""

import unittest
from pathlib import Path

from rdflib import Graph

from rdf_transform import MapperConfig
from rdf_transform.transform import load_mapping_graph, transform_rdf

# Test directories
FIXTURES_DIR = Path(__file__).parent / "fixtures" / "complex_mappings"
GOLDEN_DIR = Path(__file__).parent / "golden" / "complex_mappings"
PROJECT_ROOT = Path(__file__).parent.parent.parent


class TestIES4ToIESNextMappings(unittest.TestCase):
    """Tests for IES4 to IES Next complex mappings using the main config."""

    def setUp(self):
        """Set up with the main IES4 to IES Next config."""
        config_path = PROJECT_ROOT / "examples" / "ies4_to_iesnext_mapper" / "mapping-config.yaml"
        self.config = MapperConfig.from_yaml(str(config_path))

        # Load the mapping graph (relative to config file's directory)
        mapping_file = config_path.parent / self.config.mapping_file
        self.mapping_graph = load_mapping_graph(
            str(mapping_file),
            self.config.mapping_file_format or "turtle",
            self.config,
        )

    def _test_complex_mapping(self, fixture_file, golden_file):
        """Test complex crossing mapping transforms IES4 Crossing to IES Next."""
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
        print(output_data)
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

    def test_crossing_mapping(self):
        self._test_complex_mapping("crossing.ttl", "crossing.ttl")

    def test_pluriverse_mapping(self):
        self._test_complex_mapping("pluriverse.ttl", "pluriverse.ttl")

    def test_duration_mapping(self):
        self._test_complex_mapping("duration.ttl", "duration.ttl")

    def test_make_mapping(self):
        self._test_complex_mapping("make.ttl", "make.ttl")


if __name__ == "__main__":
    unittest.main()
