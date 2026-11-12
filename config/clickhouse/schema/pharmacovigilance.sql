CREATE TABLE IF NOT EXISTS pharmacovigilance_log (
  chembl_id String,
  count UInt32,
  critval Float64,
  event LowCardinality(String),
  llr Float64,
  meddraCode LowCardinality(String)
) ENGINE = Log;
