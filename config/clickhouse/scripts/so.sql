CREATE TABLE IF NOT EXISTS sequence_ontology ENGINE = MergeTree ()
ORDER BY id AS (
        SELECT *
        FROM sequence_ontology_log
    );

DROP TABLE IF EXISTS sequence_ontology_log SYNC;