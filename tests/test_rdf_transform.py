"""Unit tests for rdf_transform core library."""

import unittest

from rdflib import Graph, Literal, Namespace
from rdflib.namespace import RDF

from rdf_transform import MapperConfig, SPARQLQuery
from rdf_transform.transform import (
    add_prefixes_to_query,
    execute_construct_query,
    load_mapping_graph,
    merge_mapping_with_input,
    parse_rdf,
    serialize_rdf,
    transform_rdf,
)


class TestExecuteConstructQuery(unittest.TestCase):
    """Tests for execute_construct_query function."""

    def setUp(self):
        """Set up test fixtures."""
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

        self.sample_graph = Graph()
        ex = Namespace("http://example.org/")
        # Add some test triples
        self.sample_graph.add((ex.subject1, ex.property, Literal("value1")))
        self.sample_graph.add((ex.subject2, ex.property, Literal("value2")))
        self.sample_graph.add((ex.subject3, ex.otherProperty, Literal("value3")))

    def test_returns_graph_object(self):
        """Test that execute_construct_query returns a Graph object."""
        result = execute_construct_query(self.sample_graph, self.sample_config.queries[0], self.sample_config)
        self.assertIsInstance(result, Graph, "Result should be a Graph object")

    def test_graph_has_bind_method(self):
        """Test that returned graph has bind method (regression test for AttributeError)."""
        result = execute_construct_query(self.sample_graph, self.sample_config.queries[0], self.sample_config)

        # This should not raise AttributeError
        result.bind("test", "http://test.org/")

    def test_query_transforms_data(self):
        """Test that CONSTRUCT query actually transforms the data."""
        result = execute_construct_query(self.sample_graph, self.sample_config.queries[0], self.sample_config)

        # Should have transformed triples
        ex = Namespace("http://example.org/")
        self.assertIn((ex.subject1, ex.transformed, Literal("value1")), result)
        self.assertIn((ex.subject2, ex.transformed, Literal("value2")), result)

        # Should not include non-matching triples
        self.assertNotIn((ex.subject3, ex.transformed, Literal("value3")), result)

    def test_namespaces_are_bound(self):
        """Test that namespaces from config are bound to result graph."""
        result = execute_construct_query(self.sample_graph, self.sample_config.queries[0], self.sample_config)

        # Check that namespaces are bound
        namespaces = dict(result.namespaces())
        self.assertIn("ex", namespaces)
        self.assertEqual(str(namespaces["ex"]), "http://example.org/")


class TestParseRDF(unittest.TestCase):
    """Tests for parse_rdf function."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_config = MapperConfig(
            namespaces={
                "ex": "http://example.org/",
                "rdf": str(RDF),
            },
            queries=[],
        )

    def test_parses_turtle_data(self):
        """Test parsing Turtle RDF data."""
        turtle_data = """
        @prefix ex: <http://example.org/> .
        ex:subject ex:predicate "object" .
        """

        result = parse_rdf(turtle_data, "turtle", self.sample_config)
        self.assertIsInstance(result, Graph)
        self.assertEqual(len(result), 1)

    def test_parses_bytes_data(self):
        """Test parsing RDF data as bytes."""
        turtle_data = b"""
        @prefix ex: <http://example.org/> .
        ex:subject ex:predicate "object" .
        """

        result = parse_rdf(turtle_data, "turtle", self.sample_config)
        self.assertIsInstance(result, Graph)
        self.assertEqual(len(result), 1)

    def test_binds_configured_namespaces(self):
        """Test that configured namespaces are bound to parsed graph."""
        turtle_data = """
        @prefix ex: <http://example.org/> .
        ex:subject ex:predicate "object" .
        """

        result = parse_rdf(turtle_data, "turtle", self.sample_config)
        namespaces = dict(result.namespaces())
        self.assertIn("ex", namespaces)


class TestSerializeRDF(unittest.TestCase):
    """Tests for serialize_rdf function."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_graph = Graph()
        ex = Namespace("http://example.org/")
        self.sample_graph.add((ex.subject1, ex.property, Literal("value1")))
        self.sample_graph.add((ex.subject2, ex.property, Literal("value2")))
        self.sample_graph.add((ex.subject3, ex.otherProperty, Literal("value3")))

    def test_serializes_to_bytes(self):
        """Test that serialization returns bytes."""
        result = serialize_rdf(self.sample_graph, "text/turtle")
        self.assertIsInstance(result, bytes)

    def test_uses_turtle_format(self):
        """Test that serialization uses specified format."""
        result = serialize_rdf(self.sample_graph, "text/turtle")
        self.assertTrue(b"@prefix" in result or b"<http://example.org/" in result)

    def test_uses_ntriples_format(self):
        """Test that serialization uses N-Triples format."""
        result = serialize_rdf(self.sample_graph, "application/n-triples")

        # N-Triples format should have angular brackets and end with dot
        self.assertIn(b"<http://example.org/", result)
        self.assertIn(b" .", result)


class TestTransformRDF(unittest.TestCase):
    """Tests for the main transform_rdf function."""

    def setUp(self):
        """Set up test fixtures."""
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

    def test_transforms_rdf_data(self):
        """Test end-to-end RDF transformation."""
        input_data = b"""
        @prefix ex: <http://example.org/> .
        ex:subject1 ex:property "value1" .
        ex:subject2 ex:property "value2" .
        """

        output_data, metrics = transform_rdf(
            input_data=input_data,
            input_format="turtle",
            output_format="text/turtle",
            config=self.sample_config,
        )

        self.assertIsInstance(output_data, bytes)
        self.assertIn(b"transformed", output_data)

        # Check metrics
        self.assertIn("input_triples", metrics)
        self.assertIn("output_triples", metrics)
        self.assertIn("total_time", metrics)
        self.assertEqual(metrics["input_triples"], 2)
        self.assertEqual(metrics["output_triples"], 2)

    def test_raises_on_empty_input(self):
        """Test that empty input raises ValueError."""
        input_data = b"@prefix ex: <http://example.org/> ."

        with self.assertRaisesRegex(ValueError, "Input graph is empty"):
            transform_rdf(
                input_data=input_data,
                input_format="turtle",
                output_format="text/turtle",
                config=self.sample_config,
            )

    def test_raises_on_empty_output(self):
        """Test that empty output raises ValueError."""
        input_data = b"""
        @prefix ex: <http://example.org/> .
        ex:subject ex:otherProperty "value" .
        """

        with self.assertRaisesRegex(ValueError, "Output graph is empty"):
            transform_rdf(
                input_data=input_data,
                input_format="turtle",
                output_format="text/turtle",
                config=self.sample_config,
            )


class TestLoadMappingGraph(unittest.TestCase):
    """Tests for load_mapping_graph function."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_config = MapperConfig(
            namespaces={
                "ex": "http://example.org/",
                "rdf": str(RDF),
            },
            queries=[],
        )

    def test_loads_mapping_from_file(self):
        """Test loading ontology mappings from a file."""
        import tempfile
        from pathlib import Path

        # Create a temporary mapping file
        with tempfile.TemporaryDirectory() as tmp_dir:
            mapping_file = Path(tmp_dir) / "test_mappings.ttl"
            mapping_file.write_text("""
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix ex: <http://example.org/> .

            ex:oldProperty owl:equivalentProperty ex:newProperty .
            """)

            mapping = load_mapping_graph(str(mapping_file), "turtle", self.sample_config)
            self.assertIsInstance(mapping, Graph)
            self.assertEqual(len(mapping), 1)


class TestMergeMapping(unittest.TestCase):
    """Tests for merge_mapping_with_input function."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_config = MapperConfig(
            namespaces={
                "ex": "http://example.org/",
                "rdf": str(RDF),
            },
            queries=[],
        )

        self.sample_graph = Graph()
        ex = Namespace("http://example.org/")
        self.sample_graph.add((ex.subject1, ex.property, Literal("value1")))
        self.sample_graph.add((ex.subject2, ex.property, Literal("value2")))

    def test_merges_graphs(self):
        """Test merging mapping graph with input graph."""
        mapping = Graph()
        ex = Namespace("http://example.org/")
        mapping.add((ex.test, ex.predicate, Literal("mapping")))

        merged = merge_mapping_with_input(self.sample_graph, mapping, self.sample_config)
        self.assertEqual(len(merged), len(self.sample_graph) + len(mapping))

    def test_returns_input_when_no_mapping(self):
        """Test that input is returned unchanged when no mapping provided."""
        result = merge_mapping_with_input(self.sample_graph, None, self.sample_config)
        self.assertEqual(result, self.sample_graph)


class TestAddPrefixesToQuery(unittest.TestCase):
    """Tests for add_prefixes_to_query function."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_config = MapperConfig(
            namespaces={
                "ex": "http://example.org/",
                "rdf": str(RDF),
            },
            queries=[],
        )

    def test_adds_prefixes_to_query(self):
        """Test that prefixes are added to queries without PREFIX declarations."""
        query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
        result = add_prefixes_to_query(query, self.sample_config)

        self.assertIn("PREFIX ex:", result)
        self.assertIn("PREFIX rdf:", result)
        self.assertIn("CONSTRUCT", result)

    def test_does_not_add_if_prefix_exists(self):
        """Test that prefixes are not added if query already has PREFIX declarations."""
        query = "PREFIX custom: <http://custom.org/>\nCONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
        result = add_prefixes_to_query(query, self.sample_config)

        self.assertEqual(result, query)  # Should be unchanged
