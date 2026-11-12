CREATE TABLE IF NOT EXISTS pharmacovigilance ENGINE = EmbeddedRocksDB () PRIMARY KEY chembl_id AS (
  SELECT
    chembl_id,
    reverse(
      arraySort(
        a -> a.logLR,
        groupArray(
          (
            count,
            critval,
            event AS name,
            llr AS logLR,
            meddraCode
          ):: Tuple(
            count UInt32,
            critval Float64,
            name LowCardinality(String),
            logLR Float64,
            meddraCode LowCardinality(String)
          )
        )
      )
    ) AS adverse_events,
    arrayElement(adverse_events, 1).critval AS criticalValue
  FROM
    pharmacovigilance_log
  GROUP BY
    chembl_id
);

DROP TABLE IF EXISTS pharmacovigilance_log
SYNC;
