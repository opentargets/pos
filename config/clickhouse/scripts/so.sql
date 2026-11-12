CREATE TABLE IF NOT EXISTS sequence_ontology_source ENGINE = EmbeddedRocksDB () PRIMARY KEY id AS (
  SELECT
    *
  FROM
    sequence_ontology_log
);

CREATE DICTIONARY sequence_ontology (id String, label String) PRIMARY KEY id SOURCE (
  CLICKHOUSE (
    QUERY 'SELECT toString(id) AS id, toString(label) AS label FROM sequence_ontology_source'
  )
) LAYOUT (COMPLEX_KEY_HASHED ()) LIFETIME (0);

DROP TABLE IF EXISTS sequence_ontology_log
SYNC;
