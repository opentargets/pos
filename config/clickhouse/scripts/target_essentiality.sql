CREATE TABLE IF NOT EXISTS target_essentiality ENGINE = EmbeddedRocksDB () PRIMARY KEY targetId AS (
    SELECT *
    FROM target_essentiality_log
);

DROP TABLE IF EXISTS target_essentiality_log SYNC;