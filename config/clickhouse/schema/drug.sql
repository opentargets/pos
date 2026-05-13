CREATE TABLE IF NOT EXISTS drug_log (
    id String,
    name String,
    synonyms Array (String),
    tradeNames Array (String),
    childChemblIds Array (String),
    drugType String,
    crossReferences Array (
        Tuple (
            source String,
            ids Array (String)
        )
    ),
    parentId Nullable (String),
    maximumClinicalStage String,
    description Nullable (String),
    molfile Nullable (String),
) ENGINE = Log;
