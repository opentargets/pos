CREATE TABLE IF NOT EXISTS hpo ENGINE = MergeTree ()
ORDER BY id AS (
        SELECT *
        FROM hpo_log
    );

OPTIMIZE TABLE hpo FINAL;

DROP TABLE IF EXISTS hpo_log SYNC;