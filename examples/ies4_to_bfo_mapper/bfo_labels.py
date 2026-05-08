
# Human-readable labels for BFO IRIs — added as inline comments in output TTL
BFO_LABELS = {
    # Classes
    "http://purl.obolibrary.org/obo/BFO_0000015": "Process",
    "http://purl.obolibrary.org/obo/BFO_0000027": "Object aggregate",
    "http://purl.obolibrary.org/obo/BFO_0000029": "Site",
    "http://purl.obolibrary.org/obo/BFO_0000030": "Object",
    "http://purl.obolibrary.org/obo/BFO_0000031": "Generically dependent continuant",
    "http://purl.obolibrary.org/obo/BFO_0000040": "Material entity",
    "http://purl.obolibrary.org/obo/BFO_0000182": "History",
    "http://purl.obolibrary.org/obo/BFO_0000011": "Spatiotemporal region",
    "http://purl.obolibrary.org/obo/BFO_0000038": "One-dimensional temporal region",
    "https://www.commoncoreontologies.org/ont_00000686": "Designative Information Content Entity",
    "https://www.commoncoreontologies.org/ont_00000253": "Information Bearing Entity",
    "https://www.commoncoreontologies.org/ont_00000003": "Designative Name ",
    "https://www.commoncoreontologies.org/ont_00000077": "Non-Name Identifier ",

    # Properties
    "http://purl.obolibrary.org/obo/BFO_0000056": "participates in at some time",
    "http://purl.obolibrary.org/obo/BFO_0000057": "has participant at some time",
    "http://purl.obolibrary.org/obo/BFO_0000066": "occurs in",
    "http://purl.obolibrary.org/obo/BFO_0000082": "located in at all times",
    "http://purl.obolibrary.org/obo/BFO_0000084": "generically depends on",
    "http://purl.obolibrary.org/obo/BFO_0000166": "participates in at all times",
    "http://purl.obolibrary.org/obo/BFO_0000184": "history of",
    "http://purl.obolibrary.org/obo/BFO_0000200": "occupies spatiotemporal region",
    "http://purl.obolibrary.org/obo/BFO_0000199": "occupies temporal region",
    "http://purl.obolibrary.org/obo/BFO_0000132": "occurrent part of",
    "https://www.commoncoreontologies.org/ont_00001916": "designates",
    "https://www.commoncoreontologies.org/ont_00001765": "has text value",

}


def annotate_with_comments(ttl: str) -> str:

    lines = ttl.splitlines()
    annotated = []
    for line in lines:
        for iri, label in BFO_LABELS.items():
            # Match the IRI appearing anywhere on the line (full or prefixed)
            local = iri.split("/")[-1]  # e.g. BFO_0000030
            if (iri in line or local in line) and "#" not in line:
                line = f"{line}  # {label}"
                break  # only annotate once per line
        annotated.append(line)
    return "\n".join(annotated)
