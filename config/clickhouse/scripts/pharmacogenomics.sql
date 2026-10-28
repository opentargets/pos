-- One row per pharmacogenomics record, keyed by rowId.
-- The targets, variants and drug tables each hold a pharmacogenomicsIds array pointing at
-- these rows, so those scripts must run after this one.
-- rowId only needs to be unique within a release: the data is rebuilt from scratch each time.
-- NB: the load task splits on semicolons, so do not use them inside comments or strings.
CREATE TABLE IF NOT EXISTS pharmacogenomics ENGINE = EmbeddedRocksDB () PRIMARY KEY rowId AS (
    SELECT
        rowNumberInAllBlocks() AS rowId,
        *
    FROM pharmacogenomics_log
);

DROP TABLE IF EXISTS pharmacogenomics_log SYNC;
