-- Read model for the target page molQTL credible sets table.
-- One denormalised row per (target, QTL credible set) so the API can filter, sort and page
-- a target's credible sets with a single table query.
-- The target link comes from targets.credibleSetIds (credible sets of the studies whose
-- geneId is the target) because credible_set has no gene column.
-- Built from final tables only (the _log tables are dropped by earlier steps),
-- so it must run after credible_sets, targets, variants, studies and biosample.
-- Every join is on the key of an EmbeddedRocksDB table, so they all run as direct joins.
-- Unmatched joins give default values ('' and 0), the same as variant_credible_sets.
-- NB: the load task splits on semicolons, so do not use them inside comments or strings.
-- ORDER BY: targetId does the work for the lookup, the rest matches the
-- default sort (most significant first).
CREATE TABLE IF NOT EXISTS target_credible_sets ENGINE = MergeTree()
ORDER BY
  (targetId, pValueExponent, pValueMantissa, credibleSetId)
SETTINGS
  allow_nullable_key = 1 AS
SELECT
  tg.id AS targetId,
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
  cs.studyType AS studyType,
  s.condition AS condition,
  s.biosampleFromSourceId AS biosampleId,
  toLowCardinality(b.biosampleName) AS biosampleName,
  cs.locusSize AS locusSize
FROM
  (
    SELECT
      id,
      credibleSetId
    FROM
      targets
      ARRAY JOIN credibleSetIds AS credibleSetId
  ) AS tg
  INNER JOIN credible_sets AS cs ON tg.credibleSetId = cs.credibleSetId
  LEFT JOIN variants AS v ON cs.leadVariantId = v.variantId
  LEFT JOIN studies AS s ON cs.studyId = s.studyId
  LEFT JOIN biosample AS b ON ifNull(s.biosampleFromSourceId, '') = b.biosampleId;

OPTIMIZE TABLE target_credible_sets FINAL;
