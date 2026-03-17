CREATE TABLE IF NOT EXISTS drug ENGINE = MergeTree ()
ORDER BY id AS (
        SELECT *
        FROM drug_log
    );

DROP TABLE IF EXISTS drug_log SYNC;