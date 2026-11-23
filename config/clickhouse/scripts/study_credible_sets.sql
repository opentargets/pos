CREATE TABLE IF NOT EXISTS study_credible_sets ENGINE = MergeTree()
ORDER BY
  (studyId, topL2GScore, studyLocusId)
SETTINGS
  allow_nullable_key = 1 AS
SELECT
  cs.studyId AS studyId,
  cs.studyLocusId AS studyLocusId,
  cs.variantId AS leadVariantId,
  cs.chromosome AS leadChromosome,
  cs.position AS leadPosition,
  v.referenceAllele AS leadReferenceAllele,
  v.alternateAllele AS leadAlternateAllele,
  cs.pValueMantissa AS pValueMantissa,
  cs.pValueExponent AS pValueExponent,
  cs.beta AS beta,
  cs.finemappingMethod AS finemappingMethod,
  cs.confidence AS confidence,
  length(csl.locus) AS locusSize,
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
  LEFT JOIN credible_sets_locus AS csl ON cs.studyLocusId = csl.studyLocusId
  LEFT JOIN l2g_predictions AS l ON cs.studyLocusId = l.studyLocusId
  LEFT JOIN variants AS v ON cs.variantId = v.variantId
  LEFT JOIN targets AS t ON tupleElement(l.l2g_predictions[1], 'geneId') = t.id;

OPTIMIZE TABLE study_credible_sets FINAL;
