FROM telicent/telicent-python3.12:1.2.37 AS builder

USER root
RUN microdnf install -y gcc-c++ python3.12-devel && \
  microdnf clean all

USER user
COPY ./pyproject.toml .
RUN python3 -m pip install --upgrade "pip<25.3" --root-user-action=ignore && \
  python3 -m pip install pip-tools && \
  python3 -m piptools compile --extra knowledge_mapper -o requirements.txt pyproject.toml && \
  python3 -m pip install -r requirements.txt --no-cache-dir

FROM telicent/telicent-python3.12:1.2.37
COPY --from=builder /home/user/app-venv/ /home/user/app-venv/

COPY sbom.json* /opt/telicent/sbom/
WORKDIR /app

# Copy the core rdf_transform library
COPY rdf_transform/__init__.py ./rdf_transform/__init__.py
COPY rdf_transform/config.py ./rdf_transform/config.py
COPY rdf_transform/formats.py ./rdf_transform/formats.py
COPY rdf_transform/transform.py ./rdf_transform/transform.py

# Copy the telicent_mapper module
COPY examples/telicent_mapper/__init__.py ./telicent_mapper/__init__.py
COPY examples/telicent_mapper/mapper.py ./telicent_mapper/mapper.py
COPY examples/telicent_mapper/labels.py ./telicent_mapper/labels.py

# Copy example config and mappings from ies4_to_iesnext_mapper
# Users should mount their own config via volume or set CONFIG_PATH env var
COPY examples/ies4_to_iesnext_mapper/mapping-config.yaml ./mapping-config.yaml
COPY examples/ies4_to_iesnext_mapper/resource-mapping.ttl ./resource-mapping.ttl

CMD ["python3", "-m", "telicent_mapper.mapper"]
