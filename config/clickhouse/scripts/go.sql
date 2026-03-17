CREATE TABLE IF NOT EXISTS gene_ontology ENGINE = MergeTree ()
ORDER BY id AS (
        SELECT *
        FROM gene_ontology_log
    );

OPTIMIZE TABLE gene_ontology FINAL;

DROP TABLE IF EXISTS gene_ontology_log SYNC;