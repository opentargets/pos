CREATE TABLE if not exists interaction_evidence_log (
    interactionId UInt64,
    evidenceScore Nullable (Float64),
    expansionMethodMiIdentifier Nullable (String),
    expansionMethodShortName Nullable (String),
    hostOrganismScientificName Nullable (String),
    hostOrganismTaxId Nullable (UInt32),
    hostOrganismTissue Tuple (
        fullName Nullable (String),
        shortName Nullable (String),
        xrefs Array (
            Tuple (
                database LowCardinality (String),
                identifier LowCardinality (String)
            )
        )
    ),
    interactionDetectionMethodMiIdentifier String,
    interactionDetectionMethodShortName String,
    interactionIdentifier Nullable (String),
    interactionResources Tuple (
        databaseVersion LowCardinality (String),
        sourceDatabase LowCardinality (String)
    ),
    interactionScore Float64,
    interactionTypeMiIdentifier Nullable (String),
    interactionTypeShortName Nullable (String),
    participantDetectionMethodA Array (
        Tuple (
            miIdentifier Nullable (String),
            shortName Nullable (String)
        )
    ),
    participantDetectionMethodB Array (
        Tuple (
            miIdentifier Nullable (String),
            shortName Nullable (String)
        )
    ),
    pubmedId Nullable (String)
) engine = Log;
