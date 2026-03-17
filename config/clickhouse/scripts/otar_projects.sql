CREATE TABLE IF NOT EXISTS otar_projects ENGINE = MergeTree ()
ORDER BY efo_id AS (
        SELECT *
        FROM otar_projects_log
    );

OPTIMIZE TABLE otar_projects FINAL;