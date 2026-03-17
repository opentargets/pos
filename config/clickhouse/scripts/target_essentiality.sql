CREATE TABLE IF NOT EXISTS target_essentiality ENGINE = MergeTree ()
ORDER BY id AS (
        SELECT *
        FROM target_essentiality_log
    );

DROP TABLE IF EXISTS target_essentiality_log SYNC;