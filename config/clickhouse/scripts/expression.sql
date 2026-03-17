CREATE TABLE IF NOT EXISTS expression ENGINE = MergeTree ()
ORDER BY id AS (
        SELECT *
        FROM expression_log
    );

DROP TABLE IF EXISTS expression_log SYNC;