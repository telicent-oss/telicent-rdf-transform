"""Unit tests for telicent_mapper helper functions.

Tests individual functions in isolation:
- get_format_from_headers(): Format detection from Content-Type headers
- _should_skip_record(): Record validation and filtering logic
"""

import unittest

from rdflib.namespace import RDF
from telicent_mapper import mapper

from rdf_transform import MapperConfig, SPARQLQuery


class TestGetFormatFromHeaders(unittest.TestCase):
    """Tests for get_format_from_headers function."""

    def setUp(self):
        """Set up mapper config before each test."""
        self.sample_config = MapperConfig(
            namespaces={
                "ex": "http://example.org/",
                "rdf": str(RDF),
            },
            queries=[
                SPARQLQuery(
                    name="test_query",
                    description="Test query",
                    enabled=True,
                    order=1,
                    query="""
                        CONSTRUCT {
                            ?s <http://example.org/transformed> ?o .
                        }
                        WHERE {
                            ?s <http://example.org/property> ?o .
                        }
                    """,
                ),
            ],
        )
        mapper.mapper_config = self.sample_config
        mapper.output_format = "text/turtle"

    def tearDown(self):
        """Clean up mapper config after each test."""
        mapper.mapper_config = None
        mapper.output_format = None

    def test_detects_turtle_format(self):
        """Test detection of Turtle format from Content-Type header."""
        from telicent_lib import Record

        record = Record(
            headers=[("Content-Type", "text/turtle")],
            key=None,
            value=b"test",
        )

        result = mapper.get_format_from_headers(record)
        self.assertEqual(result, "turtle")

    def test_detects_json_ld_format(self):
        """Test detection of JSON-LD format from Content-Type header."""
        from telicent_lib import Record

        record = Record(
            headers=[("Content-Type", "application/ld+json")],
            key=None,
            value=b"test",
        )

        result = mapper.get_format_from_headers(record)
        self.assertEqual(result, "json-ld")

    def test_handles_charset_parameter(self):
        """Test that charset parameter in Content-Type is stripped."""
        from telicent_lib import Record

        record = Record(
            headers=[("Content-Type", "text/turtle; charset=utf-8")],
            key=None,
            value=b"test",
        )

        result = mapper.get_format_from_headers(record)
        self.assertEqual(result, "turtle")

    def test_defaults_to_turtle_when_no_header(self):
        """Test that missing Content-Type defaults to turtle."""
        from telicent_lib import Record

        record = Record(
            headers=[],
            key=None,
            value=b"test",
        )

        result = mapper.get_format_from_headers(record)
        self.assertEqual(result, "turtle")

    def test_case_insensitive_header_matching(self):
        """Test that Content-Type header matching is case-insensitive."""
        from telicent_lib import Record

        record = Record(
            headers=[("content-type", "text/turtle")],
            key=None,
            value=b"test",
        )

        result = mapper.get_format_from_headers(record)
        self.assertEqual(result, "turtle")


class TestShouldSkipRecord(unittest.TestCase):
    """Tests for _should_skip_record validation function."""

    def setUp(self):
        """Set up mapper config before each test."""
        self.sample_config = MapperConfig(
            namespaces={
                "ex": "http://example.org/",
                "rdf": str(RDF),
            },
            queries=[],
        )
        mapper.mapper_config = self.sample_config
        mapper.output_format = "text/turtle"
        mapper.MAPPER_NAME = "default_mapper"  # Set a default to avoid None comparison issues

    def tearDown(self):
        """Clean up mapper config after each test."""
        mapper.mapper_config = None
        mapper.output_format = None
        mapper.MAPPER_NAME = None

    def test_skips_record_with_none_value(self):
        """Test that records with None value are skipped."""
        from telicent_lib import Record

        record = Record(headers=[], key=None, value=None)
        self.assertTrue(mapper._should_skip_record(record))

    def test_skips_record_with_matching_exec_path(self):
        """Test that records with matching Exec-Path are skipped."""
        from telicent_lib import Record

        mapper.MAPPER_NAME = "test_mapper"
        record = Record(
            headers=[("Exec-Path", "test_mapper")],
            key=None,
            value=b"test",
        )
        self.assertTrue(mapper._should_skip_record(record))

    def test_processes_record_with_different_exec_path(self):
        """Test that records with different Exec-Path are processed."""
        from telicent_lib import Record

        mapper.MAPPER_NAME = "test_mapper"
        record = Record(
            headers=[("Exec-Path", "other_mapper")],
            key=None,
            value=b"test",
        )
        self.assertFalse(mapper._should_skip_record(record))

    def test_processes_record_without_exec_path(self):
        """Test that records without Exec-Path header are processed."""
        from telicent_lib import Record

        record = Record(
            headers=[],
            key=None,
            value=b"test",
        )
        self.assertFalse(mapper._should_skip_record(record))


# Note: parse_rdf and serialize_rdf tests moved to test_rdf_transform.py
# since those functions are now part of the core library
