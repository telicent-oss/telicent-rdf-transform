"""Telicent Kafka SPARQL CONSTRUCT mapper implementation.

This module handles Kafka integration and Record processing using the Telicent platform.
The actual RDF transformation logic is in the rdf_transform package.
"""

import sys
import time
from logging import StreamHandler
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from rdflib import Graph
from telicent_lib import Mapper, Record, RecordUtils
from telicent_lib.config import Configurator
from telicent_lib.logging import CoreLoggerFactory
from telicent_lib.sinks import KafkaSink
from telicent_lib.sources import KafkaSource

from rdf_transform import MapperConfig, load_mapping_graph, transform_rdf
from rdf_transform.formats import get_rdflib_format_from_mime
from telicent_mapper.labels import create_security_label

# Module-level variables - will be initialized in main
logger: Any = None
mapper_config: MapperConfig | None = None
output_format: str | None = None
mapping_graph: Graph | None = None  # Optional RDF mapping graph for ontology mappings
security_label_and_group: str | None = None  # Security label and_group for output records
MAPPER_NAME: str | None = None


def get_format_from_headers(record: Record) -> str:
    """Extract RDF format from Content-Type header.

    Args:
        record: Record object with headers

    Returns:
        RDFLib format string (defaults to 'turtle' if not found)
    """
    # Get Content-Type header (case-insensitive)
    content_type = RecordUtils.get_last_header(record, "Content-Type")

    if not content_type:
        if logger:
            logger.warning("No Content-Type header found, defaulting to turtle format")
        return "turtle"

    if logger:
        logger.debug(f"Detected Content-Type: {content_type}")

    return get_rdflib_format_from_mime(content_type)


def _should_skip_record(record: Record) -> bool:
    """Check if record should be skipped.

    Args:
        record: Record to validate

    Returns:
        True if record should be skipped, False otherwise
    """
    if record.value is None:
        if logger:
            logger.warning("Received record with None value, filtering out")
        return True

    # To ensure we don't map a message that already came from this mapper we
    # check that the "Exec-Path" is not equal to the name of the mapper
    exec_path = RecordUtils.get_last_header(record, "Exec-Path")
    if exec_path == MAPPER_NAME:
        if logger:
            logger.debug(f"Skipping record already processed by this mapper (Exec-Path: {exec_path})")
        return True

    return False


def mapping_function(record: Record) -> Record | list[Record] | None:
    """Apply SPARQL CONSTRUCT queries to the record's RDF data.

    Args:
        record: Input record containing RDF data

    Returns:
        Transformed record with mapped RDF data, or None to filter out the record
    """
    start_time = time.time()

    # Check if record should be skipped
    if _should_skip_record(record):
        return None

    try:
        # Determine input format from Content-Type header
        input_format = get_format_from_headers(record)
        if logger:
            logger.debug(f"Detected input format: {input_format}")

        # Call the pure transformation function
        assert mapper_config is not None, "mapper_config must be initialized"
        assert output_format is not None, "output_format must be initialized"

        output_data, timing_metrics = transform_rdf(
            input_data=record.value,
            input_format=input_format,
            output_format=output_format,
            config=mapper_config,
            mapping_graph=mapping_graph,
        )

        # Create the output record
        output_record = Record(
            headers=RecordUtils.to_headers(
                headers={
                    "Content-Type": output_format,
                    "Security-Label": create_security_label(and_group=security_label_and_group),
                },
                existing_headers=record.headers
            ),
            key=record.key,
            value=output_data,
        )

        total_time = time.time() - start_time

        if logger:
            logger.info(
                f"Successfully mapped record: {timing_metrics['input_triples']} -> "
                f"{timing_metrics['output_triples']} triples "
                f"in {total_time:.3f}s (parse: {timing_metrics['parse_time']:.3f}s, "
                f"query: {timing_metrics['query_time']:.3f}s, "
                f"serialize: {timing_metrics['serialize_time']:.3f}s)"
            )

        return output_record

    except ValueError as e:
        # ValueError indicates filtering conditions (empty graphs)
        total_time = time.time() - start_time
        if logger:
            if "empty" in str(e).lower():
                logger.warning(f"{e}, filtering out record (processed in {total_time:.3f}s)")
            else:
                logger.warning(f"Validation error: {e} (processed in {total_time:.3f}s)")
        return None

    except Exception as e:
        total_time = time.time() - start_time
        if logger:
            logger.error(f"Error processing record after {total_time:.3f}s: {e}", exc_info=True)
        # Return None to filter out problematic records
        return None


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Initialize configuration
    config = Configurator()

    # Get mapper name
    MAPPER_NAME = config.get("MAPPER_NAME", required=True, default="telicent_mapper")

    # Initialize logger - fall back to basic logger if Kafka isn't available
    try:
        logger = CoreLoggerFactory.get_logger(name=MAPPER_NAME)
        logger.logger.addHandler(StreamHandler())
    except RuntimeError as e:
        # Kafka not available, use basic Python logger
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        logger = logging.getLogger(MAPPER_NAME)
        logger.warning(f"Could not initialize Kafka logger: {e}. Using basic logger instead.")

    try:
        # Get configuration file path from environment or use default
        config_path = config.get("CONFIG_PATH", required=False, default="config.example.yaml")

        if not Path(config_path).exists():
            logger.error(f"Configuration file not found: {config_path}")
            logger.info("Please set CONFIG_PATH environment variable or create a config file")
            raise ValueError(f"Configuration file not found: {config_path}")

        logger.info(f"Loading configuration from: {config_path}")

        # Load configuration
        mapper_config = MapperConfig.from_yaml(config_path)
        logger.info(f"Loaded {len(mapper_config.queries)} queries from configuration")

        # Get RDF output format setting
        output_format = config.get("RDF_OUTPUT_FORMAT", required=False, default="text/turtle")

        # Get security label and_group setting
        security_label_and_group = config.get(
            "SECURITY_LABEL_AND_GROUP",
            required=False,
            default="urn:telicent:groups:datasets:mapped"
        )

        # Optionally load RDF mapping graph for ontology mappings
        # Environment variable takes precedence over config.yaml
        mapping_file_path = config.get("MAPPING_FILE_PATH", required=False, default=mapper_config.mapping_file)
        mapping_format = config.get("MAPPING_FILE_FORMAT", required=False, default=mapper_config.mapping_file_format)

        if mapping_file_path:
            if not Path(mapping_file_path).exists():
                logger.warning(f"Mapping file not found: {mapping_file_path}. Continuing without mappings.")
                mapping_graph = None
            else:
                try:
                    mapping_graph = load_mapping_graph(
                        source=mapping_file_path, format=mapping_format, config=mapper_config
                    )
                    logger.info(f"Loaded mapping file: {mapping_file_path} ({len(mapping_graph)} triples)")
                except Exception as e:
                    logger.error(f"Failed to load mapping file {mapping_file_path}: {e}")
                    mapping_graph = None
        else:
            mapping_graph = None
            logger.info("No mapping file specified")

        logger.info(f"Initialized mapper with {len(mapper_config.queries)} queries")

        # Get Kafka topics from config
        input_topic = config.get("SOURCE_TOPIC", required=True, default="knowledge")
        output_topic = config.get("TARGET_TOPIC", required=True, default="knowledge")

        # Create Kafka source and sink
        source = KafkaSource(topic=input_topic)
        target = KafkaSink(topic=output_topic)

        # Create and run the mapper
        mapper = Mapper(
            map_function=mapping_function,
            name=MAPPER_NAME,
            source=source,
            target=target,
        )

        mapper.run()
        logger.info("Mapper running...")

    except ValueError as e:
        logger.error(e)
        raise
    except KeyboardInterrupt:
        logger.info("Mapper stopped")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
