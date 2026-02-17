from telicent_labels import SecurityLabelBuilder, TelicentSecurityLabelsV2


def create_security_label(and_group = "urn:telicent:groups:datasets:mapped"):
    return (
        SecurityLabelBuilder()
            .add(TelicentSecurityLabelsV2.CLASSIFICATION.value, "O")
            .add_multiple(TelicentSecurityLabelsV2.AND_GROUPS.value, and_group)
    ).build()
