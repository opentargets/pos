CREATE TABLE IF NOT EXISTS drug ENGINE = MergeTree ()
ORDER BY id AS (
        SELECT *
        FROM drug_log
    );

OPTIMIZE TABLE drug FINAL;

DROP TABLE IF EXISTS drug_log SYNC;