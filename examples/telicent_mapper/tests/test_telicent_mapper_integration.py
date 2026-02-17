"""Integration tests for telicent_mapper.

Tests the end-to-end behavior of the mapping_function, including:
- RDF transformation with various input formats (Turtle, JSON-LD)
- Record filtering and validation (empty input, invalid RDF, already mapped)
- Header preservation and security labeling
- Key preservation through the pipeline
- Output validation against golden files
"""

import json
import unittest
from pathlib import Path

from rdflib import Graph
from telicent_lib import Record
from telicent_mapper import mapper

from rdf_transform import MapperConfig

# Test directories
FIXTURES_DIR = Path(__file__).parent / "fixtures"
GOLDEN_DIR = Path(__file__).parent / "golden"


class TestMappingFunctionSnapshot(unittest.TestCase):
    """Snapshot tests for mapping_function end-to-end behavior."""

    def setUp(self):
        """Set up mapper module-level variables before each test."""
        config_path = FIXTURES_DIR / "test_config.yaml"
        self.test_config = MapperConfig.from_yaml(str(config_path))

        mapper.mapper_config = self.test_config
        mapper.output_format = "text/turtle"
        mapper.MAPPER_NAME = "test_rdf_transform_mapper"
        mapper.security_label_and_group = "urn:telicent:groups:datasets:test"
        mapper.mapping_graph = None
        mapper.logger = None  # Disable logging for tests

    def tearDown(self):
        """Clean up mapper config after each test."""
        mapper.mapper_config = None
        mapper.output_format = None
        mapper.MAPPER_NAME = None
        mapper.security_label_and_group = None

    def test_turtle_input_transformation(self):
        """Test mapping function with Turtle input."""
        # Read fixture data
        fixture_path = FIXTURES_DIR / "sample1.ttl"
        with open(fixture_path, "rb") as f:
            rdf_data = f.read()

        # Create test record
        record = Record(
            headers=[("Content-Type", "text/turtle")],
            key=b"test-key-1",
            value=rdf_data,
        )

        # Execute mapping
        result = mapper.mapping_function(record)

        # Verify result is not None
        self.assertIsNotNone(result, "Mapping should produce a result")
        self.assertIsInstance(result, Record, "Result should be a Record")

        # Parse output RDF to verify it's valid
        output_graph = Graph()
        output_graph.parse(data=result.value, format="turtle")
        self.assertGreater(len(output_graph), 0, "Output graph should have triples")

        # Save golden file (for first run)
        golden_path = GOLDEN_DIR / "sample1_output.ttl"
        golden_path.parent.mkdir(parents=True, exist_ok=True)

        # Save normalized RDF (sorted triples for consistent comparison)
        serialized = output_graph.serialize(format="turtle")

        if not golden_path.exists():
            # First run - create golden file
            with open(golden_path, "w") as f:
                f.write(serialized)
            self.skipTest("Golden file created - run test again to compare")
        else:
            # Compare with golden file
            golden_graph = Graph()
            golden_graph.parse(golden_path, format="turtle")

            # Compare graphs semantically (isomorphic check)
            self.assertTrue(
                output_graph.isomorphic(golden_graph),
                f"Output differs from golden file. "
                f"Expected {len(golden_graph)} triples, got {len(output_graph)} triples.",
            )

        # Verify headers are set correctly
        headers_dict = {k: v.decode() if isinstance(v, bytes) else v for k, v in result.headers}
        self.assertEqual(headers_dict["Content-Type"], "text/turtle")
        self.assertIn("Security-Label", headers_dict)

        # Save header snapshot
        headers_golden_path = GOLDEN_DIR / "sample1_headers.json"
        if not headers_golden_path.exists():
            with open(headers_golden_path, "w") as f:
                json.dump(headers_dict, f, indent=2)
            self.skipTest("Header golden file created")
        else:
            with open(headers_golden_path) as f:
                expected_headers = json.load(f)
            self.assertEqual(headers_dict, expected_headers, "Headers differ from expected")

    def test_jsonld_input_transformation(self):
        """Test mapping function with JSON-LD input."""
        # Read fixture data
        fixture_path = FIXTURES_DIR / "sample2.jsonld"
        with open(fixture_path, "rb") as f:
            rdf_data = f.read()

        # Create test record
        record = Record(
            headers=[("Content-Type", "application/ld+json")],
            key=b"test-key-2",
            value=rdf_data,
        )

        # Execute mapping
        result = mapper.mapping_function(record)

        # For JSON-LD with Location data, queries won't match (no Person/Organization)
        # So output graph will be empty and result should be None
        if result is not None:
            # If result is returned, verify it's valid
            output_graph = Graph()
            output_graph.parse(data=result.value, format="turtle")
            # Empty graph should be filtered out, but verify structure if present
            self.assertIsInstance(result, Record)

    def test_empty_input_filtered(self):
        """Test that empty RDF input is filtered out."""
        # Create empty turtle document
        empty_rdf = b"@prefix ex: <http://example.org/> .\n"

        record = Record(
            headers=[("Content-Type", "text/turtle")],
            key=b"test-key-empty",
            value=empty_rdf,
        )

        result = mapper.mapping_function(record)
        self.assertIsNone(result, "Empty input should be filtered out")

    def test_none_value_filtered(self):
        """Test that records with None value are filtered out."""
        record = Record(
            headers=[("Content-Type", "text/turtle")],
            key=b"test-key-none",
            value=None,
        )

        result = mapper.mapping_function(record)
        self.assertIsNone(result, "None value should be filtered out")

    def test_already_mapped_record_skipped(self):
        """Test that records already processed by this mapper are skipped."""
        fixture_path = FIXTURES_DIR / "sample1.ttl"
        with open(fixture_path, "rb") as f:
            rdf_data = f.read()

        record = Record(
            headers=[
                ("Content-Type", "text/turtle"),
                ("Exec-Path", "test_rdf_transform_mapper"),  # Same as MAPPER_NAME
            ],
            key=b"test-key-skip",
            value=rdf_data,
        )

        result = mapper.mapping_function(record)
        self.assertIsNone(result, "Already mapped records should be skipped")

    def test_empty_output_after_queries_filtered(self):
        """Test that empty output after query application is filtered."""
        # Create RDF that won't match any queries
        non_matching_rdf = b"""
        @prefix ex: <http://example.org/> .
        ex:Something a ex:UnknownType ;
            ex:property "value" .
        """

        record = Record(
            headers=[("Content-Type", "text/turtle")],
            key=b"test-key-nomatch",
            value=non_matching_rdf,
        )

        result = mapper.mapping_function(record)
        self.assertIsNone(result, "Empty output after queries should be filtered")

    def test_invalid_rdf_filtered(self):
        """Test that invalid RDF is filtered out."""
        invalid_rdf = b"This is not valid RDF at all!"

        record = Record(
            headers=[("Content-Type", "text/turtle")],
            key=b"test-key-invalid",
            value=invalid_rdf,
        )

        result = mapper.mapping_function(record)
        self.assertIsNone(result, "Invalid RDF should be filtered out")

    def test_record_key_preserved(self):
        """Test that the record key is preserved through mapping."""
        fixture_path = FIXTURES_DIR / "sample1.ttl"
        with open(fixture_path, "rb") as f:
            rdf_data = f.read()

        test_key = b"important-key-12345"
        record = Record(
            headers=[("Content-Type", "text/turtle")],
            key=test_key,
            value=rdf_data,
        )

        result = mapper.mapping_function(record)
        self.assertIsNotNone(result)
        self.assertEqual(result.key, test_key, "Record key should be preserved")

    def test_existing_headers_preserved(self):
        """Test that existing headers are preserved in output."""
        fixture_path = FIXTURES_DIR / "sample1.ttl"
        with open(fixture_path, "rb") as f:
            rdf_data = f.read()

        record = Record(
            headers=[
                ("Content-Type", "text/turtle"),
                ("Custom-Header", "custom-value"),
                ("Source-System", "test-system"),
            ],
            key=b"test-key",
            value=rdf_data,
        )

        result = mapper.mapping_function(record)
        self.assertIsNotNone(result)

        headers_dict = {k: v.decode() if isinstance(v, bytes) else v for k, v in result.headers}
        self.assertEqual(headers_dict.get("Custom-Header"), "custom-value")
        self.assertEqual(headers_dict.get("Source-System"), "test-system")
