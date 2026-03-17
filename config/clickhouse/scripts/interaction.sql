CREATE TABLE IF NOT EXISTS interaction_with_evidence ENGINE = MergeTree
ORDER BY (targetA) SETTINGS allow_nullable_key = 1 AS (
        SELECT i.*, arrayFilter(e -> e.6 IS NOT NULL, groupArray (
                (
                    e.evidenceScore, e.expansionMethodMiIdentifier, e.expansionMethodShortName, e.hostOrganismScientificName, e.hostOrganismTaxId, e.intASource, e.intBSource, e.interactionDetectionMethodMiIdentifier, e.interactionDetectionMethodShortName, e.interactionIdentifier, e.interactionResources, e.interactionTypeMiIdentifier, e.interactionTypeShortName, e.participantDetectionMethodA, e.participantDetectionMethodB, e.pubmedId
                )
            )) AS evidences
        FROM
            interaction_log AS i
            LEFT JOIN interaction_evidence_log AS e 
            ON i.targetA = e.targetA
            AND isNotDistinctFrom(i.targetB, e.targetB)
            AND isNotDistinctFrom(i.intA, e.intA)
            AND isNotDistinctFrom(i.intB, e.intB)
            AND isNotDistinctFrom(i.intABiologicalRole, e.intABiologicalRole)
            AND isNotDistinctFrom(i.intBBiologicalRole, e.intBBiologicalRole)
            AND i.sourceDatabase = e.interactionResources.sourceDatabase
        GROUP BY
            i.*
    );

CREATE TABLE IF NOT EXISTS interaction ENGINE = MergeTree() ORDER BY targetA AS
(
    SELECT 
        targetA,
        groupArray(
            (
                targetA,
                intA, 
                targetB, 
                intB, 
                intABiologicalRole, 
                intBBiologicalRole, 
                scoring,
                count,
                sourceDatabase, 
                speciesA, 
                speciesB,
                evidences
            )::Tuple(
                targetA String,
                intA String, 
                targetB Nullable (String), 
                intB String, 
                intABiologicalRole LowCardinality (String), 
                intBBiologicalRole LowCardinality (String), 
                scoring Nullable (Float64), 
                count UInt8,
                sourceDatabase Enum('intact', 'reactome', 'signor', 'string'), 
                speciesA Tuple (
                    mnemonic LowCardinality (String), 
                    scientificName LowCardinality (String), 
                    taxonId UInt8
                ), 
                speciesB Tuple (
                    mnemonic LowCardinality (String), 
                    scientificName LowCardinality (Nullable (String)), 
                    taxonId Nullable (UInt8)
                ),
                evidences Array (Tuple(
                    evidenceScore Nullable (Float64), 
                    expansionMethodMiIdentifier Nullable (String), 
                    expansionMethodShortName Nullable (String), 
                    hostOrganismScientificName Nullable (String), 
                    hostOrganismTaxId Nullable (UInt32), 
                    intASource String, 
                    intBSource String, 
                    interactionDetectionMethodMiIdentifier String, 
                    interactionDetectionMethodShortName String, 
                    interactionIdentifier Nullable (String), 
                    interactionResources Tuple (
                        databaseVersion LowCardinality (String), 
                        sourceDatabase LowCardinality (String)
                    ),
                    interactionTypeMiIdentifier Nullable (String), 
                    interactionTypeShortName Nullable (String), 
                    participantDetectionMethodA Array (Tuple (
                        miIdentifier Nullable (String), 
                        shortName Nullable (String)
                        )
                    ), 
                    participantDetectionMethodB Array (Tuple (
                        miIdentifier Nullable (String), 
                        shortName Nullable (String)
                    )), 
                    pubmedId Nullable (String)
                ))
            )
        ) AS interactions
    FROM interaction_with_evidence
    GROUP BY targetA      
);

OPTIMIZE TABLE interaction FINAL;

drop table if exists interaction_with_evidence SYNC;