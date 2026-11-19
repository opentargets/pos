-- Read model for the variant page credible sets tables (all study types).
-- One fully denormalised row per (variant in locus, credible set) so the API can
-- filter and sort on any column with a single table query.
-- Built from final tables only (the _log tables are dropped by earlier steps),
-- so it must run after credible_sets, studies, disease, targets, variants, biosample
-- and l2g_predictions.
-- studyTargetId / studyTargetApprovedSymbol and biosampleId / biosampleName are NULL when
-- the study has no target or biosample (e.g. GWAS) or the id is not found.
-- diseases holds (id, name) for each study disease.
-- diseaseTherapeuticAreas is the distinct union of (id, name) across all of the study diseases.
-- The topL2G fields are NULL when a credible set has no L2G predictions
-- (and topL2GApprovedSymbol is also NULL when the gene is not in targets).
-- topL2GScore is Nullable and in the sorting key, so allow_nullable_key is needed.
-- NB: the load task splits on semicolons, so do not use them inside comments or strings.
-- ORDER BY: (studyType, variantId) does the work for the lookup, the rest matches the
-- default sort (most significant first).
CREATE TABLE IF NOT EXISTS variant_credible_sets ENGINE = MergeTree()
ORDER BY
  (studyType, variantId, topL2GScore, credibleSetId)
SETTINGS
  allow_nullable_key = 1 AS
WITH
  disease_with_ta AS (
    SELECT
      d.id AS id,
      any(d.name) AS name,
      groupArrayIf ((ta.id, ta.name), d.taId != '') AS therapeuticAreas
    FROM
      (
        SELECT
          id,
          name,
          taId
        FROM
          disease
          LEFT ARRAY JOIN therapeuticAreas AS taId
      ) AS d
      LEFT JOIN (
        SELECT
          id,
          name
        FROM
          disease
      ) AS ta ON d.taId = ta.id
    GROUP BY
      d.id
  ),
  study_info AS (
    SELECT
      sd.studyId AS studyId,
      any(sd.traitFromSource) AS traitFromSource,
      any(sd.condition) AS condition,
      any(sd.geneId) AS studyTargetId,
      any(sd.biosampleFromSourceId) AS biosampleId,
      groupArrayIf (
        (dt.id, dt.name, dt.therapeuticAreas),
        sd.diseaseId != ''
      ) AS diseases
    FROM
      (
        SELECT
          studyId,
          traitFromSource,
          condition,
          geneId,
          biosampleFromSourceId,
          diseaseId
        FROM
          studies
          LEFT ARRAY JOIN diseaseIds AS diseaseId
      ) AS sd
      LEFT JOIN disease_with_ta AS dt ON sd.diseaseId = dt.id
    GROUP BY
      sd.studyId
  ),
  top_l2g AS (
    SELECT
      l.credibleSetId AS credibleSetId,
      l.geneId AS geneId,
      l.score AS score,
      t.approvedSymbol AS approvedSymbol
    FROM
      (
        -- l2g_predictions is already sorted by score descending
        SELECT
          credibleSetId,
          tupleElement(l2g_predictions[1], 'geneId') AS geneId,
          tupleElement(l2g_predictions[1], 'score') AS score
        FROM
          l2g_predictions
        WHERE
          notEmpty(l2g_predictions)
      ) AS l
      LEFT JOIN (
        SELECT
          id,
          approvedSymbol
        FROM
          targets
      ) AS t ON l.geneId = t.id
  ),
  sets AS (
    SELECT
      cs.studyType AS studyType,
      cs.credibleSetId AS credibleSetId,
      cs.leadPValueMantissa AS pValueMantissa,
      cs.leadPValueExponent AS pValueExponent,
      cs.leadBeta AS beta,
      cs.fineMappingMethod AS fineMappingMethod,
      cs.confidence AS confidence,
      cs.isTransQtl AS isTransQtl,
      cs.leadVariantId AS leadVariantId,
      cs.chromosome AS leadChromosome,
      cs.leadPosition AS leadPosition,
      v.referenceAllele AS leadReferenceAllele,
      v.alternateAllele AS leadAlternateAllele,
      cs.studyId AS studyId,
      si.traitFromSource AS traitFromSource,
      si.condition AS condition,
      si.studyTargetId AS studyTargetId,
      nullIf(st.approvedSymbol, '') AS studyTargetApprovedSymbol,
      si.biosampleId AS biosampleId,
      nullIf(b.biosampleName, '') AS biosampleName,
      si.diseases AS diseases,
      arraySort(
        arrayDistinct(arrayFlatten(arrayMap(d -> d .3, si.diseases)))
      ) AS therapeuticAreas,
      -- top_l2g only has credible sets with predictions, so an unmatched join
      -- (credibleSetId '') means there are none
      if(tl.credibleSetId = '', NULL, tl.geneId) AS topL2GGeneId,
      nullIf(tl.approvedSymbol, '') AS topL2GApprovedSymbol,
      if(tl.credibleSetId = '', NULL, tl.score) AS topL2GScore,
      csl.locus AS locus,
      cs.locusSize AS locusSize
    FROM
      credible_sets AS cs
      INNER JOIN credible_sets_locus AS csl ON cs.credibleSetId = csl.credibleSetId
      LEFT JOIN study_info AS si ON cs.studyId = si.studyId
      LEFT JOIN top_l2g AS tl ON cs.credibleSetId = tl.credibleSetId
      LEFT JOIN (
        SELECT
          variantId,
          referenceAllele,
          alternateAllele
        FROM
          variants
      ) AS v ON cs.leadVariantId = v.variantId
      LEFT JOIN (
        SELECT
          id,
          approvedSymbol
        FROM
          targets
      ) AS st ON ifNull(si.studyTargetId, '') = st.id
      LEFT JOIN (
        SELECT
          biosampleId,
          biosampleName
        FROM
          biosample
      ) AS b ON ifNull(si.biosampleId, '') = b.biosampleId
  )
SELECT
  studyType,
  tupleElement(l, 'variantId') AS variantId,
  credibleSetId,
  tupleElement(l, 'posteriorProbability') AS posteriorProbability,
  locusSize,
  pValueMantissa,
  pValueExponent,
  beta,
  fineMappingMethod,
  confidence,
  isTransQtl,
  leadVariantId,
  leadChromosome,
  leadPosition,
  leadReferenceAllele,
  leadAlternateAllele,
  studyId,
  condition,
  studyTargetId,
  toLowCardinality(studyTargetApprovedSymbol) AS studyTargetApprovedSymbol,
  biosampleId,
  toLowCardinality(biosampleName) AS biosampleName,
  toLowCardinality(traitFromSource) AS traitFromSource,
  CAST(
    arrayMap(d -> (d .1, d .2), diseases),
    'Array(Tuple(id LowCardinality(String), name LowCardinality(String)))'
  ) AS diseases,
  CAST(
    therapeuticAreas,
    'Array(Tuple(id LowCardinality(String), name LowCardinality(String)))'
  ) AS diseaseTherapeuticAreas,
  topL2GGeneId,
  toLowCardinality(topL2GApprovedSymbol) AS topL2GApprovedSymbol,
  topL2GScore
FROM
  sets
  ARRAY JOIN locus AS l;

OPTIMIZE TABLE variant_credible_sets FINAL;
