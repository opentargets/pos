CREATE TABLE if not exists interaction_log (
    interactionId UInt64,
    intA String,
    targetA String,
    intB String,
    targetB Nullable (String),
    intABiologicalRole LowCardinality (String),
    intBBiologicalRole LowCardinality (String),
    scoring Nullable (Float64),
    count UInt8,
    sourceDatabase Enum(
        'intact',
        'reactome',
        'signor',
        'string'
    ),
    speciesA Tuple (
        mnemonic LowCardinality (String),
        scientificName LowCardinality (String),
        taxonId UInt8
    ),
    speciesB Tuple (
        mnemonic LowCardinality (String),
        scientificName LowCardinality (Nullable (String)),
        taxonId Nullable (UInt8)
    )
) engine = Log;
