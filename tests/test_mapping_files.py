"""Tests for the mapping_files configuration and loader."""

import tempfile
import unittest
from pathlib import Path

from rdflib import Graph, URIRef
from rdflib.namespace import OWL

from rdf_transform import MapperConfig, MappingFile, load_mapping_graphs

EX = "http://example.org/"
FIRST_FILE = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix ex: <http://example.org/> .

ex:oldProperty owl:equivalentProperty ex:newProperty .
"""
SECOND_FILE = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix ex: <http://example.org/> .

ex:OldClass owl:equivalentClass ex:NewClass .
"""


class TestMappingFilesConfig(unittest.TestCase):
    def test_from_dict_parses_entries(self) -> None:
        config = MapperConfig.from_dict(
            {"mapping_files": [{"path": "a.ttl"}, {"path": "b.rdf", "format": "application/rdf+xml"}]}
        )
        self.assertEqual(
            config.mapping_files,
            [MappingFile(path="a.ttl"), MappingFile(path="b.rdf", format="application/rdf+xml")],
        )

    def test_from_dict_defaults_to_no_files(self) -> None:
        self.assertEqual(MapperConfig.from_dict({}).mapping_files, [])

    def test_from_yaml_records_the_config_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config.yaml"
            config_path.write_text("mapping_files:\n  - path: mappings.ttl\n")
            config = MapperConfig.from_yaml(config_path)
        self.assertEqual(config.base_dir, Path(tmp_dir))
        self.assertEqual(config.mapping_files, [MappingFile(path="mappings.ttl")])

    def test_removed_keys_are_rejected(self) -> None:
        for key in ("mapping_file", "mapping_file_format"):
            with self.assertRaises(ValueError) as raised:
                MapperConfig.from_dict({key: "mappings.ttl"})
            self.assertIn(key, str(raised.exception))
            self.assertIn("mapping_files", str(raised.exception))

    def test_malformed_entries_are_rejected(self) -> None:
        for entries in ("mappings.ttl", ["mappings.ttl"], [{"format": "text/turtle"}], [{"path": "a.ttl", "fmt": "x"}]):
            with self.assertRaises(ValueError):
                MapperConfig.from_dict({"mapping_files": entries})


class TestLoadMappingGraphs(unittest.TestCase):
    def test_returns_none_when_no_files(self) -> None:
        self.assertIsNone(load_mapping_graphs(MapperConfig.from_dict({})))

    def test_merges_every_file_relative_to_base_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "first.ttl").write_text(FIRST_FILE)
            (Path(tmp_dir) / "second.ttl").write_text(SECOND_FILE)
            config = MapperConfig.from_dict(
                {"mapping_files": [{"path": "first.ttl"}, {"path": "second.ttl"}]}, base_dir=tmp_dir
            )
            mapping = load_mapping_graphs(config)
        self.assertIsInstance(mapping, Graph)
        assert mapping is not None
        self.assertEqual(len(mapping), 2)
        self.assertIn((URIRef(EX + "oldProperty"), OWL.equivalentProperty, URIRef(EX + "newProperty")), mapping)
        self.assertIn((URIRef(EX + "OldClass"), OWL.equivalentClass, URIRef(EX + "NewClass")), mapping)

    def test_binds_configured_namespaces(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "first.ttl").write_text(FIRST_FILE)
            config = MapperConfig.from_dict(
                {"namespaces": {"ex": EX}, "mapping_files": [{"path": "first.ttl"}]}, base_dir=tmp_dir
            )
            mapping = load_mapping_graphs(config)
        assert mapping is not None
        self.assertEqual(str(dict(mapping.namespaces())["ex"]), EX)

    def test_missing_file_is_reported_with_its_resolved_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = MapperConfig.from_dict({"mapping_files": [{"path": "absent.ttl"}]}, base_dir=tmp_dir)
            with self.assertRaises(FileNotFoundError) as raised:
                load_mapping_graphs(config)
        self.assertIn(str(Path(tmp_dir) / "absent.ttl"), str(raised.exception))


if __name__ == "__main__":
    unittest.main()
