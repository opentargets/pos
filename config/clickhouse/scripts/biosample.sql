CREATE TABLE IF NOT EXISTS biosample ENGINE = MergeTree ()
ORDER BY (biosampleId) AS (
        SELECT *
        FROM biosample_log
    );

OPTIMIZE TABLE biosample FINAL;

DROP TABLE IF EXISTS biosample_log SYNC;