-- Read model for the study page credible sets table (GWAS studies only).
-- One denormalised row per credible set so the API can filter, sort and page
-- a study's credible sets with a single table query.
-- Built from final tables only (the _log tables are dropped by earlier steps),
-- so it must run after credible_sets, targets, variants and l2g_predictions.
-- Every join is on the key of an EmbeddedRocksDB table, so they all run as direct joins.
-- Unmatched joins give default values ('' and 0), the same as variant_credible_sets,
-- except the topL2G fields, which are NULL when a credible set has no L2G predictions
-- (and topL2GApprovedSymbol is also NULL when the gene is not in targets).
-- topL2GScore and the p-value are Nullable and in the sorting key, so allow_nullable_key is needed.
-- NB: the load task splits on semicolons, so do not use them inside comments or strings.
-- ORDER BY: studyId does the work for the lookup, the rest matches the
-- default sort (most significant first).
CREATE TABLE IF NOT EXISTS study_credible_sets ENGINE = MergeTree()
ORDER BY
  (
    studyId,
    pValueExponent,
    pValueMantissa,
    credibleSetId
  )
SETTINGS
  allow_nullable_key = 1 AS
SELECT
  cs.studyId AS studyId,
  cs.credibleSetId AS credibleSetId,
  cs.leadVariantId AS leadVariantId,
  cs.chromosome AS leadChromosome,
  cs.leadPosition AS leadPosition,
  v.referenceAllele AS leadReferenceAllele,
  v.alternateAllele AS leadAlternateAllele,
  cs.leadPValueMantissa AS pValueMantissa,
  cs.leadPValueExponent AS pValueExponent,
  cs.leadBeta AS beta,
  cs.fineMappingMethod AS fineMappingMethod,
  cs.confidence AS confidence,
  cs.locusSize AS locusSize,
  -- l2g_predictions is already sorted by score descending
  if(
    empty(l.l2g_predictions),
    NULL,
    tupleElement(l.l2g_predictions[1], 'geneId')
  ) AS topL2GGeneId,
  -- no predictions means no targets match, so nullIf covers both cases
  toLowCardinality(nullIf(t.approvedSymbol, '')) AS topL2GApprovedSymbol,
  if(
    empty(l.l2g_predictions),
    NULL,
    tupleElement(l.l2g_predictions[1], 'score')
  ) AS topL2GScore
FROM
  credible_sets AS cs
  LEFT JOIN l2g_predictions AS l ON cs.credibleSetId = l.credibleSetId
  LEFT JOIN variants AS v ON cs.leadVariantId = v.variantId
  LEFT JOIN targets AS t ON tupleElement(l.l2g_predictions[1], 'geneId') = t.id
WHERE
  cs.studyType = 'gwas';

OPTIMIZE TABLE study_credible_sets FINAL;
