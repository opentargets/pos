CREATE TABLE if not exists protein_coding_coords_by_target engine = MergeTree() order by targetId as (
    SELECT 
        targetId,
        groupArray(
            (targetId,
            uniprotAccessions,
            aminoAcidPosition,
            alternateAminoAcid,
            referenceAminoAcid,
            variantFunctionalConsequenceIds,
            variantEffect,
            variantId,
            diseases,
            datasources,
            therapeuticAreas
            )::Tuple(
                targetId String,
                uniprotAccessions Array (String),
                aminoAcidPosition Int32,
                alternateAminoAcid LowCardinality (String),
                referenceAminoAcid LowCardinality (String),
                variantFunctionalConsequenceIds Array (String),
                variantEffect Nullable (Float64),
                variantId String,
                diseases Array (String),
                datasources Array (
                    Tuple (
                        datasourceCount UInt32,
                        datasourceId LowCardinality (String),
                        datasourceNiceName LowCardinality (String)
                    )
                ),
                therapeuticAreas Array (String)
            )
        ) as proteinCodingCoords
    FROM protein_coding_coords_log
    GROUP BY targetId
);

CREATE TABLE IF NOT EXISTS protein_coding_coords_by_variant ENGINE = MergeTree() order by variantId as (
    SELECT 
        variantId,
        groupArray(
            (targetId,
            uniprotAccessions,
            aminoAcidPosition,
            alternateAminoAcid,
            referenceAminoAcid,
            variantFunctionalConsequenceIds,
            variantEffect,
            variantId,
            diseases,
            datasources,
            therapeuticAreas
            )::Tuple(
                targetId String,
                uniprotAccessions Array (String),
                aminoAcidPosition Int32,
                alternateAminoAcid LowCardinality (String),
                referenceAminoAcid LowCardinality (String),
                variantFunctionalConsequenceIds Array (String),
                variantEffect Nullable (Float64),
                variantId String,
                diseases Array (String),
                datasources Array (
                    Tuple (
                        datasourceCount UInt32,
                        datasourceId LowCardinality (String),
                        datasourceNiceName LowCardinality (String)
                    )
                ),
                therapeuticAreas Array (String)
            )
        ) as proteinCodingCoords
    FROM protein_coding_coords_log
    GROUP BY variantId
);

DROP TABLE IF EXISTS protein_coding_coords_log SYNC;