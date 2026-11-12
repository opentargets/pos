-- Built from credible_sets_log (one row per credible set) and credible_set_variant_log
-- (one row per variant in each credible set), so the credible_set_variant load must run first.
-- credible_set_variant_log is dropped at the end of this script. credible_sets_log is kept
-- for targets.sql, which drops it.
-- NB: the load task splits on semicolons, so do not use them inside comments or strings.

-- One row per credible set, with the lead variant statistics from credible_set_variant
-- (every lead variant has a row there) and the number of variants in the credible set.
-- credible_set_variant_log is on the left so the hash tables are built from the
-- 6M credible sets, not the 141M variant rows.
CREATE TABLE IF NOT EXISTS credible_sets ENGINE = EmbeddedRocksDB () PRIMARY KEY credibleSetId AS (
    SELECT
        cs.id AS credibleSetId,
        cs.studyId AS studyId,
        cs.studyType AS studyType,
        cs.chromosome AS chromosome,
        cs.leadVariantId AS leadVariantId,
        -- variant ids are chromosome_position_ref_alt
        toUInt32(splitByChar('_', cs.leadVariantId)[2]) AS leadPosition,
        v.pValueMantissa AS leadPValueMantissa,
        v.pValueExponent AS leadPValueExponent,
        v.reportedBeta AS leadBeta,
        v.reportedStandardError AS leadStandardError,
        v.effectAlleleFrequency AS leadEffectAlleleFrequency,
        v.sampleSize AS leadSampleSize,
        cs.locusStart AS locusStart,
        cs.locusEnd AS locusEnd,
        n.locusSize AS locusSize,
        cs.fineMappingMethod AS fineMappingMethod,
        cs.logBayesFactor AS logBayesFactor,
        cs.confidence AS confidence,
        cs.purityMeanR2 AS purityMeanR2,
        cs.purityMinR2 AS purityMinR2,
        cs.qualityControls AS qualityControls,
        has(cs.qualityControls, 'Trans QTL') AS isTransQtl
    FROM
        credible_set_variant_log AS v
        INNER JOIN credible_sets_log AS cs ON v.credibleSetId = cs.id
        AND v.variantId = cs.leadVariantId
        INNER JOIN (
            SELECT
                credibleSetId,
                toUInt32(count()) AS locusSize
            FROM
                credible_set_variant_log
            GROUP BY
                credibleSetId
        ) AS n ON n.credibleSetId = cs.id
    SETTINGS
        optimize_aggregation_in_order = 1
);

-- The variants in each credible set, most probable first.
CREATE TABLE IF NOT EXISTS credible_sets_locus ENGINE = EmbeddedRocksDB () PRIMARY KEY credibleSetId AS (
    SELECT
        credibleSetId,
        arrayReverseSort (
            l -> l.posteriorProbability,
            groupArray (
                CAST(
                    (
                        variantId,
                        posteriorProbability,
                        logBayesFactor,
                        pValueMantissa,
                        pValueExponent,
                        effectAlleleFrequency,
                        sampleSize,
                        reportedBeta,
                        reportedStandardError,
                        r2Overall,
                        conditionalBeta,
                        conditionalStandardError,
                        rescaledAbsBeta,
                        rescaledStandardError,
                        rescaledDirection,
                        qualityControls
                    ),
                    'Tuple(
                        variantId String,
                        posteriorProbability Float32,
                        logBayesFactor Nullable(Float32),
                        pValueMantissa Nullable(Float32),
                        pValueExponent Nullable(Int32),
                        effectAlleleFrequency Nullable(Float32),
                        sampleSize Nullable(UInt32),
                        reportedBeta Nullable(Float64),
                        reportedStandardError Nullable(Float64),
                        r2Overall Nullable(Float32),
                        conditionalBeta Nullable(Float32),
                        conditionalStandardError Nullable(Float32),
                        rescaledAbsBeta Nullable(Float32),
                        rescaledStandardError Nullable(Float32),
                        rescaledDirection Nullable(Int8),
                        qualityControls Array(LowCardinality(String))
                    )'
                )
            )
        ) AS locus
    FROM
        credible_set_variant_log
    GROUP BY
        credibleSetId
    SETTINGS
        optimize_aggregation_in_order = 1
);

-- credibleSetIds for each study, joined onto studies by studies.sql (which drops this table).
CREATE TABLE IF NOT EXISTS credible_sets_by_study ENGINE = EmbeddedRocksDB () PRIMARY KEY studyId AS (
    SELECT
        studyId,
        groupArray (id) AS credibleSetIds
    FROM
        credible_sets_log
    GROUP BY
        studyId
);

-- credibleSetIds for each variant, joined onto variants by variants.sql (which drops this table).
-- (credibleSetId, variantId) is unique in credible_set_variant, so groupArray has no duplicates.
CREATE TABLE IF NOT EXISTS credible_sets_by_variant ENGINE = EmbeddedRocksDB () PRIMARY KEY variantId AS (
    SELECT
        variantId,
        groupArray (credibleSetId) AS credibleSetIds
    FROM
        credible_set_variant_log
    GROUP BY
        variantId
);

DROP TABLE IF EXISTS credible_set_variant_log SYNC;
