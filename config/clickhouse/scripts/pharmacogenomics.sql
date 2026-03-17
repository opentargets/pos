CREATE TABLE IF NOT EXISTS pharmacogenomics_by_target ENGINE = MergeTree() ORDER BY targetFromSourceId AS (
    SELECT
        targetFromSourceId,
        groupArray(
            (datasourceId,
            datatypeId,
            drugs,
            evidenceLevel,
            genotype,
            genotypeAnnotationText,
            genotypeId,
            haplotypeFromSourceId,
            haplotypeId,
            literature,
            pgxCategory,
            phenotypeFromSourceId,
            phenotypeText,
            variantAnnotation,
            studyId,
            targetFromSourceId,
            variantFunctionalConsequenceId,
            variantRsId,
            variantId,
            isDirectTarget
            )::Tuple (
                datasourceId LowCardinality (String),
                datatypeId LowCardinality (String),
                drugs Array (
                    Tuple (
                        drugId LowCardinality (String),
                        drugFromSource LowCardinality (String)
                    )
                ),
                evidenceLevel LowCardinality (String),
                genotype Nullable (String),
                genotypeAnnotationText Nullable (String),
                genotypeId Nullable (String),
                haplotypeFromSourceId Nullable (String),
                haplotypeId Nullable (String),
                literature Array (String),
                pgxCategory LowCardinality (String),
                phenotypeFromSourceId Nullable (String),
                phenotypeText Nullable (String),
                variantAnnotation Array (
                    Tuple (
                        baseAlleleOrGenotype Nullable (String),
                        comparisonAlleleOrGenotype Nullable (String),
                        directionality Nullable (String),
                        effect Nullable (String),
                        effectDescription Nullable (String),
                        effectType Nullable (String),
                        entity Nullable (String),
                        literature Nullable (String)
                    )
                ),
                studyId Nullable (String),
                targetFromSourceId Nullable (String),
                variantFunctionalConsequenceId Nullable (String),
                variantRsId Nullable (String),
                variantId Nullable (String),
                isDirectTarget Bool
            )
        ) AS pharmacogenomics
    FROM pharmacogenomics_log
    WHERE targetFromSourceId IS NOT NULL
    GROUP BY 
        targetFromSourceId
);

CREATE TABLE IF NOT EXISTS pharmacogenomics_by_variant ENGINE = MergeTree() ORDER BY variantId AS (
    SELECT
        variantId,
        groupArray(
            (datasourceId,
            datatypeId,
            drugs,
            evidenceLevel,
            genotype,
            genotypeAnnotationText,
            genotypeId,
            haplotypeFromSourceId,
            haplotypeId,
            literature,
            pgxCategory,
            phenotypeFromSourceId,
            phenotypeText,
            variantAnnotation,
            studyId,
            targetFromSourceId,
            variantFunctionalConsequenceId,
            variantRsId,
            variantId,
            isDirectTarget
             )::Tuple (
                datasourceId LowCardinality (String),
                datatypeId LowCardinality (String),
                drugs Array (
                    Tuple (
                        drugId LowCardinality (String),
                        drugFromSource LowCardinality (String)
                    )
                ),
                evidenceLevel LowCardinality (String),
                genotype Nullable (String),
                genotypeAnnotationText Nullable (String),
                genotypeId Nullable (String),
                haplotypeFromSourceId Nullable (String),
                haplotypeId Nullable (String),
                literature Array (String),
                pgxCategory LowCardinality (String),
                phenotypeFromSourceId Nullable (String),
                phenotypeText Nullable (String),
                variantAnnotation Array (
                    Tuple (
                        baseAlleleOrGenotype Nullable (String),
                        comparisonAlleleOrGenotype Nullable (String),
                        directionality Nullable (String),
                        effect Nullable (String),
                        effectDescription Nullable (String),
                        effectType Nullable (String),
                        entity Nullable (String),
                        literature Nullable (String)
                    )
                ),
                studyId Nullable (String),
                targetFromSourceId Nullable (String),
                variantFunctionalConsequenceId Nullable (String),
                variantRsId Nullable (String),
                variantId Nullable (String),
                isDirectTarget Bool
             )
        ) AS pharmacogenomics
    FROM pharmacogenomics_log
    WHERE variantId IS NOT NULL
    GROUP BY 
        variantId
);

CREATE TABLE IF NOT EXISTS pharmacogenomics_by_drug ENGINE = MergeTree() ORDER BY drugId AS (
    SELECT
        drug.drugId as drugId,
        groupArray(
            (datasourceId,
            datatypeId,
            drugs,
            evidenceLevel,
            genotype,
            genotypeAnnotationText,
            genotypeId,
            haplotypeFromSourceId,
            haplotypeId,
            literature,
            pgxCategory,
            phenotypeFromSourceId,
            phenotypeText,
            variantAnnotation,
            studyId,
            targetFromSourceId,
            variantFunctionalConsequenceId,
            variantRsId,
            variantId,
            isDirectTarget
             )::Tuple (
                datasourceId LowCardinality (String),
                datatypeId LowCardinality (String),
                drugs Array (
                    Tuple (
                        drugId LowCardinality (String),
                        drugFromSource LowCardinality (String)
                    )
                ),
                evidenceLevel LowCardinality (String),
                genotype Nullable (String),
                genotypeAnnotationText Nullable (String),
                genotypeId Nullable (String),
                haplotypeFromSourceId Nullable (String),
                haplotypeId Nullable (String),
                literature Array (String),
                pgxCategory LowCardinality (String),
                phenotypeFromSourceId Nullable (String),
                phenotypeText Nullable (String),
                variantAnnotation Array (
                    Tuple (
                        baseAlleleOrGenotype Nullable (String),
                        comparisonAlleleOrGenotype Nullable (String),
                        directionality Nullable (String),
                        effect Nullable (String),
                        effectDescription Nullable (String),
                        effectType Nullable (String),
                        entity Nullable (String),
                        literature Nullable (String)
                    )
                ),
                studyId Nullable (String),
                targetFromSourceId Nullable (String),
                variantFunctionalConsequenceId Nullable (String),
                variantRsId Nullable (String),
                variantId Nullable (String),
                isDirectTarget Bool
             )
        ) AS pharmacogenomics
    FROM pharmacogenomics_log
    ARRAY JOIN drugs AS drug
    WHERE drug.drugId IS NOT NULL
    GROUP BY 
        drug.drugId
);

DROP TABLE IF EXISTS pharmacogenomics_log SYNC;