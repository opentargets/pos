CREATE TABLE if not exists variants_log (
    `variantId` String,
    `chromosome` Enum(
        '1',
        '2',
        '3',
        '4',
        '5',
        '6',
        '7',
        '8',
        '9',
        '10',
        '11',
        '12',
        '13',
        '14',
        '15',
        '16',
        '17',
        '18',
        '19',
        '20',
        '21',
        '22',
        'X',
        'Y',
        'MT'
    ),
    `position` UInt32,
    `referenceAllele` String,
    `alternateAllele` String,
    `variantEffect` Array (
        Tuple (
            `method` Nullable (String),
            `assessment` Nullable (String),
            `score` Nullable (Float64),
            `assessmentFlag` Nullable (String),
            `targetId` Nullable (String),
            `normalisedScore` Nullable (Float64)
        )
    ),
    `mostSevereConsequenceId` String,
    `transcriptConsequences` Array (
        Tuple (
            `variantFunctionalConsequenceIds` Array (String),
            `aminoAcidChange` Nullable (String),
            `uniprotAccessions` Array (String),
            `isEnsemblCanonical` Bool,
            `codons` Nullable (String),
            `distanceFromFootprint` Int32,
            `distanceFromTss` Int32,
            `targetId` Nullable (String),
            `impact` Nullable (String),
            `transcriptId` Nullable (String),
            `lofteePrediction` Nullable (String),
            `transcriptIndex` UInt32,
            `consequenceScore` Float64
        )
    ),
    `rsIds` Array (String),
    `dbXrefs` Array (
        Tuple (
            `id` Nullable (String),
            `source` Nullable (String)
        )
    ),
    `alleleFrequencies` Array (
        Tuple (
            `populationName` Nullable (String),
            `alleleFrequency` Nullable (Float64)
        )
    ),
    `hgvsId` Nullable (String),
    `variantDescription` String,
) engine = Log;