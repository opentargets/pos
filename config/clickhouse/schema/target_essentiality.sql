CREATE TABLE IF NOT EXISTS target_essentiality_log (
    targetId String,
    isEssential Nullable (Bool),
    depMapEssentiality Array (
        Tuple (
            screens Array (
                Tuple (
                    cellLineName Nullable (String),
                    depmapId Nullable (String),
                    diseaseCellLineId Nullable (String),
                    diseaseFromSource Nullable (String),
                    expression Nullable (Float64),
                    geneEffect Nullable (Float64),
                    mutation Nullable (String)
                )
            ),
            tissueId Nullable (String),
            tissueName Nullable (String)
        )
    )
) ENGINE = Log;