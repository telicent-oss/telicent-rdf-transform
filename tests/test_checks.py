"""Tests for output checks: SELECT queries that must return no rows."""

import unittest

from rdflib import Graph, Literal, Namespace
from rdflib.namespace import OWL, RDF

from rdf_transform import Check, CheckError, MapperConfig, run_checks, transform_rdf

EX = Namespace("http://example.org/")
OLD = Namespace("http://example.org/old#")
NAMESPACES = {"ex": str(EX), "old": str(OLD), "owl": str(OWL), "rdf": str(RDF)}
OLD_TERM_CHECK = {
    "name": "old_terms_remain",
    "message": "{subject} {predicate} {object} .: {object} is still an old term",
    "query": """
        SELECT ?subject ?predicate ?object
        WHERE {
          ?subject ?predicate ?object .
          FILTER(STRSTARTS(STR(?object), "http://example.org/old#"))
        }
    """,
}


def _config(**overrides: object) -> MapperConfig:
    data: dict[str, object] = {"namespaces": NAMESPACES, "queries": [], "checks": [OLD_TERM_CHECK]}
    data.update(overrides)
    return MapperConfig.from_dict(data)


class TestChecksConfig(unittest.TestCase):
    def test_parses_entries_in_order_and_drops_disabled(self) -> None:
        config = _config(
            checks=[
                dict(OLD_TERM_CHECK, name="second", order=2),
                dict(OLD_TERM_CHECK, name="first", order=1),
                dict(OLD_TERM_CHECK, name="off", enabled=False),
            ]
        )
        self.assertEqual([c.name for c in config.checks], ["first", "second"])
        self.assertIsInstance(config.checks[0], Check)

    def test_defaults_to_no_checks(self) -> None:
        self.assertEqual(MapperConfig.from_dict({}).checks, [])

    def test_rejects_entry_without_message(self) -> None:
        with self.assertRaises(ValueError):
            _config(checks=[{"name": "x", "query": "SELECT ?s WHERE { ?s ?p ?o }"}])

    def test_rejects_query_that_is_not_a_select(self) -> None:
        with self.assertRaises(ValueError) as raised:
            _config(checks=[dict(OLD_TERM_CHECK, query="DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }")])
        self.assertIn("SELECT", str(raised.exception))

    def test_accepts_select_after_prefixes_and_comments(self) -> None:
        query = "# leftover terms\nPREFIX ex: <http://example.org/>\nSELECT ?s WHERE { ?s ?p ?o }"
        self.assertEqual(len(_config(checks=[dict(OLD_TERM_CHECK, query=query)]).checks), 1)


class TestRunChecks(unittest.TestCase):
    def setUp(self) -> None:
        self.output = Graph()
        self.output.add((EX.a, RDF.type, EX.Thing))

    def test_no_checks_returns_nothing(self) -> None:
        self.assertEqual(run_checks(self.output, None, _config(checks=[])), [])

    def test_passing_check_returns_nothing(self) -> None:
        self.assertEqual(run_checks(self.output, None, _config()), [])

    def test_failing_rows_are_rendered_with_prefixes_and_sorted(self) -> None:
        self.output.add((EX.b, RDF.type, OLD.Thing))
        self.output.add((EX.a, EX.linkedTo, OLD.Other))
        messages = run_checks(self.output, None, _config())
        self.assertEqual(
            messages,
            [
                "ex:a ex:linkedTo old:Other .: old:Other is still an old term",
                "ex:b rdf:type old:Thing .: old:Thing is still an old term",
            ],
        )

    def test_literals_are_rendered_in_turtle_form(self) -> None:
        self.output.add((EX.a, EX.label, Literal("x")))
        check = {
            "name": "labels",
            "message": "{subject} has label {label}",
            "query": "SELECT ?subject ?label WHERE { ?subject ex:label ?label }",
        }
        self.assertEqual(run_checks(self.output, None, _config(checks=[check])), ['ex:a has label "x"'])

    def test_mapping_graph_is_a_named_graph_and_not_in_the_default_graph(self) -> None:
        mapping = Graph()
        mapping.add((OLD.Thing, OWL.equivalentClass, EX.Thing))
        self.assertEqual(run_checks(self.output, mapping, _config()), [])

        self.output.add((EX.b, RDF.type, OLD.Thing))
        check = {
            "name": "mapped_old_terms",
            "message": "{object} should have become {target}",
            "query": """
                SELECT ?object ?target
                WHERE {
                  ?subject ?predicate ?object .
                  GRAPH <urn:rdf-transform:mapping> {
                    ?object owl:equivalentClass ?target .
                  }
                }
            """,
        }
        self.assertEqual(
            run_checks(self.output, mapping, _config(checks=[check])), ["old:Thing should have become ex:Thing"]
        )

    def test_unbound_placeholder_is_an_error(self) -> None:
        self.output.add((EX.b, RDF.type, OLD.Thing))
        check = dict(OLD_TERM_CHECK, message="{subject} {missing}")
        with self.assertRaises(ValueError) as raised:
            run_checks(self.output, None, _config(checks=[check]))
        self.assertIn("missing", str(raised.exception))
        self.assertIn("old_terms_remain", str(raised.exception))

    def test_messages_follow_check_order(self) -> None:
        self.output.add((EX.b, RDF.type, OLD.Thing))
        second = dict(OLD_TERM_CHECK, name="second", message="second: {object}", order=2)
        first = dict(OLD_TERM_CHECK, name="first", message="first: {object}", order=1)
        self.assertEqual(
            run_checks(self.output, None, _config(checks=[second, first])), ["first: old:Thing", "second: old:Thing"]
        )


class TestTransformRdfWithChecks(unittest.TestCase):
    INPUT = b"""
        @prefix ex: <http://example.org/> .
        @prefix old: <http://example.org/old#> .
        ex:a a old:Thing .
    """

    def test_failed_check_raises_check_error_with_every_message(self) -> None:
        with self.assertRaises(CheckError) as raised:
            transform_rdf(self.INPUT, "text/turtle", "text/turtle", _config())
        self.assertEqual(raised.exception.messages, ["ex:a rdf:type old:Thing .: old:Thing is still an old term"])
        self.assertEqual(str(raised.exception), "ex:a rdf:type old:Thing .: old:Thing is still an old term")
        self.assertIsInstance(raised.exception, ValueError)

    def test_passing_checks_report_their_time(self) -> None:
        _, metrics = transform_rdf(self.INPUT, "text/turtle", "text/turtle", _config(checks=[]))
        self.assertIn("check_time", metrics)


if __name__ == "__main__":
    unittest.main()
