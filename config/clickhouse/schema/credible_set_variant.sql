CREATE TABLE if not exists credible_set_variant_log (
  `credibleSetId` String,
  `variantId` String,
  `pValueMantissa` Nullable(Float32),
  `pValueExponent` Nullable(Int32),
  `effectAlleleFrequency` Nullable(Float32),
  `sampleSize` Nullable(UInt32),
  `reportedBeta` Nullable(Float64),
  `reportedStandardError` Nullable(Float64),
  `posteriorProbability` Float32,
  `logBayesFactor` Nullable(Float32),
  `r2Overall` Nullable(Float32),
  `conditionalBeta` Nullable(Float32),
  `conditionalStandardError` Nullable(Float32),
  `rescaledAbsBeta` Nullable(Float32),
  `rescaledStandardError` Nullable(Float32),
  `rescaledDirection` Nullable(Int8),
  `qualityControls` Array(LowCardinality(String))
  -- MergeTree (not Log) so the credible_sets postload can group the 141M rows by credibleSetId
  -- in key order (optimize_aggregation_in_order) instead of holding every group in memory.
) engine = MergeTree
ORDER BY
  (credibleSetId, variantId);
