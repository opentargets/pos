CREATE TABLE IF NOT EXISTS biosample ENGINE = EmbeddedRocksDB () PRIMARY KEY biosampleId AS (
  SELECT
    *
  FROM
    biosample_log
);

CREATE VIEW biosample_names_source AS
SELECT
  biosampleId AS id,
  biosampleName AS name
FROM
  biosample;

CREATE DICTIONARY biosample_names (id String, name String) PRIMARY KEY id SOURCE (CLICKHOUSE (TABLE 'biosample_names_source')) LAYOUT (COMPLEX_KEY_HASHED ()) LIFETIME (0);

DROP TABLE IF EXISTS biosample_log
SYNC;
