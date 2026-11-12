CREATE TABLE IF NOT EXISTS biosample ENGINE = EmbeddedRocksDB () PRIMARY KEY biosampleId AS (
  SELECT
    *
  FROM
    biosample_log
);

CREATE DICTIONARY biosample_names (id String, name String) PRIMARY KEY id SOURCE (
  CLICKHOUSE (
    QUERY 'SELECT toString(biosampleId) AS id, toString(biosampleName) AS name FROM biosample'
  )
) LAYOUT (COMPLEX_KEY_HASHED ()) LIFETIME (0);

DROP TABLE IF EXISTS biosample_log
SYNC;
