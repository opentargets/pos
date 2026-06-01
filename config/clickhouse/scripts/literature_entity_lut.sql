CREATE TABLE IF NOT EXISTS literature_entity_lut ENGINE = MergeTree ()
ORDER BY (
        keywordId, year, month, relevance
    ) AS (
        SELECT
            pmid, pmcid, keywordId, relevance, date, year, month
        FROM literature_entity_lut_log
    );

OPTIMIZE TABLE literature_entity_lut FINAL;

DROP TABLE literature_entity_lut_log SYNC;
