-- pharmacogenomics gives array(pharmacogenomicsIds) for each drug
CREATE TABLE IF NOT EXISTS drug ENGINE = EmbeddedRocksDB () PRIMARY KEY id AS (
    SELECT * EXCEPT pgxDrugId
    FROM drug_log
    LEFT OUTER JOIN (
        SELECT
            toString(drug.drugId) AS pgxDrugId,
            groupArray(rowId) AS pharmacogenomicsIds
        FROM pharmacogenomics
        ARRAY JOIN drugs AS drug
        GROUP BY pgxDrugId
    ) AS pgx ON drug_log.id = pgx.pgxDrugId
);

DROP TABLE IF EXISTS drug_log SYNC;