"""Tests for BFO/CCO to IES4 mappings.

Tests the transformation of BFO 2.0 + CCO data to IES4 format,
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


class TestBFOToIES4Mappings(unittest.TestCase):
    """Tests for BFO/CCO to IES4 mappings using the reverse config."""

    def setUp(self):
        """Set up with the BFO to IES4 config."""
        config_path = PROJECT_ROOT / "examples" / "bfo_to_ies4_mapper" / "mapping-config.yaml"
        self.config = MapperConfig.from_yaml(str(config_path))

        mapping_file = config_path.parent / self.config.mapping_file
        self.mapping_graph = load_mapping_graph(
            str(mapping_file),
            self.config.mapping_file_format or "turtle",
            self.config,
        )

    def _test_mapping(self, fixture_file, golden_file):
        """Run mapping and compare output against a golden file."""
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

    def test_information_content_entity_mapping(self):
        self._test_mapping("info_content_entity.ttl", "info_content_entity_ies.ttl")

    def test_material_entity_mapping(self):
        self._test_mapping("object_material_entity.ttl", "object_material_entity_ies.ttl")

    def test_process_mapping(self):
        self._test_mapping("process.ttl", "process_ies.ttl")

    def test_temporal_region_mapping(self):
        self._test_mapping("temporal_region.ttl", "temporal_region_ies.ttl")


if __name__ == "__main__":
    unittest.main()
