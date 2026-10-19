-- Read model for the variant page GWAS credible sets table.
-- One fully denormalised row per (variant in locus, GWAS credible set) so the API can
-- filter and sort on any column with a single table query.
-- Built from final tables only (the _log tables are dropped by earlier steps),
-- so it must run after credible_sets, studies, disease, targets, variants and l2g_predictions.
-- Diseases are stored as parallel arrays (diseaseIds[i] has diseaseNames[i] and
-- diseaseTherapeuticAreaIds[i] / diseaseTherapeuticAreaNames[i]) to keep filtering simple.
-- NB: the load task splits on semicolons, so do not use them inside comments or strings.
-- ORDER BY: variantId does the work, the rest matches the default sort (most significant first).
CREATE TABLE IF NOT EXISTS gwas_credible_sets_by_variant ENGINE = MergeTree ()
ORDER BY (variantId, pValueExponent, pValueMantissa, studyLocusId) AS
WITH
    disease_with_ta AS (
        SELECT
            d.id AS id,
            any(d.name) AS name,
            groupArrayIf((ta.id, ta.name), d.taId != '') AS therapeuticAreas
        FROM (
                SELECT id, name, taId
                FROM disease
                LEFT ARRAY JOIN therapeuticAreas AS taId
            ) AS d
            LEFT JOIN (SELECT id, name FROM disease) AS ta ON d.taId = ta.id
        GROUP BY d.id
    ),
    study_info AS (
        SELECT
            sd.studyId AS studyId,
            any(sd.traitFromSource) AS traitFromSource,
            groupArrayIf((dt.id, dt.name, dt.therapeuticAreas), sd.diseaseId != '') AS diseases
        FROM (
                SELECT studyId, traitFromSource, diseaseId
                FROM studies
                LEFT ARRAY JOIN diseaseIds AS diseaseId
                WHERE studyType = 'gwas'
            ) AS sd
            LEFT JOIN disease_with_ta AS dt ON sd.diseaseId = dt.id
        GROUP BY sd.studyId
    ),
    top_l2g AS (
        SELECT
            l.studyLocusId AS studyLocusId,
            l.geneId AS geneId,
            l.score AS score,
            t.approvedSymbol AS approvedSymbol
        FROM (
                -- l2g_predictions is already sorted by score descending
                SELECT
                    studyLocusId,
                    tupleElement(l2g_predictions[1], 'geneId') AS geneId,
                    tupleElement(l2g_predictions[1], 'score') AS score
                FROM l2g_predictions
                WHERE notEmpty(l2g_predictions)
            ) AS l
            LEFT JOIN (SELECT id, approvedSymbol FROM targets) AS t ON l.geneId = t.id
    ),
    gwas_sets AS (
        SELECT
            cs.studyLocusId AS studyLocusId,
            cs.pValueMantissa AS pValueMantissa,
            cs.pValueExponent AS pValueExponent,
            cs.beta AS beta,
            cs.finemappingMethod AS finemappingMethod,
            cs.confidence AS confidence,
            cs.variantId AS leadVariantId,
            cs.chromosome AS leadChromosome,
            cs.position AS leadPosition,
            v.referenceAllele AS leadReferenceAllele,
            v.alternateAllele AS leadAlternateAllele,
            cs.studyId AS studyId,
            si.traitFromSource AS traitFromSource,
            si.diseases AS diseases,
            tl.geneId AS topL2GGeneId,
            tl.approvedSymbol AS topL2GApprovedSymbol,
            tl.score AS topL2GScore,
            csl.locus AS locus,
            length(csl.locus) AS locusSize
        FROM credible_sets AS cs
            INNER JOIN credible_sets_locus AS csl ON cs.studyLocusId = csl.studyLocusId
            LEFT JOIN study_info AS si ON cs.studyId = si.studyId
            LEFT JOIN top_l2g AS tl ON cs.studyLocusId = tl.studyLocusId
            LEFT JOIN (
                SELECT variantId, referenceAllele, alternateAllele
                FROM variants
            ) AS v ON cs.variantId = v.variantId
        WHERE cs.studyType = 'gwas'
    )
SELECT
    tupleElement(l, 'variantId') AS variantId,
    studyLocusId,
    tupleElement(l, 'posteriorProbability') AS posteriorProbability,
    locusSize,
    pValueMantissa,
    pValueExponent,
    beta,
    finemappingMethod,
    confidence,
    leadVariantId,
    leadChromosome,
    leadPosition,
    leadReferenceAllele,
    leadAlternateAllele,
    studyId,
    toLowCardinality(traitFromSource) AS traitFromSource,
    CAST(arrayMap(d -> d.1, diseases), 'Array(LowCardinality(String))') AS diseaseIds,
    CAST(arrayMap(d -> d.2, diseases), 'Array(LowCardinality(String))') AS diseaseNames,
    CAST(
        arrayMap(d -> arrayMap(ta -> ta.1, d.3), diseases),
        'Array(Array(LowCardinality(String)))'
    ) AS diseaseTherapeuticAreaIds,
    CAST(
        arrayMap(d -> arrayMap(ta -> ta.2, d.3), diseases),
        'Array(Array(LowCardinality(String)))'
    ) AS diseaseTherapeuticAreaNames,
    topL2GGeneId,
    toLowCardinality(topL2GApprovedSymbol) AS topL2GApprovedSymbol,
    topL2GScore
FROM gwas_sets
ARRAY JOIN locus AS l;

OPTIMIZE TABLE gwas_credible_sets_by_variant FINAL;
