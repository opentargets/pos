CREATE TABLE IF NOT EXISTS drug_warnings ENGINE = MergeTree() ORDER BY chemblId AS (
    SELECT
        arrayJoin (chemblIds) AS chemblId,
        groupArray ((
            toxicityClass,
            chemblIds,
            country,
            description,
            id,
            references,
            warningType,
            year,
            efoTerm,
            efoId,
            efoIdForWarningClass
        )::Tuple(
            toxicityClass Nullable (String),
            chemblIds Array (String),
            country Nullable (String),
            description Nullable (String),
            id Nullable (UInt32),
            references Array (
                Tuple (
                    id String,
                    source String,
                    url String
                )
            ),
            warningType String,
            year Nullable (UInt16),
            efoTerm Nullable (String),
            efoId Nullable (String),
            efoIdForWarningClass Nullable (String)
            )
        ) AS drugWarnings
    FROM drug_warnings_log
    WHERE chemblIds IS NOT NULL
    GROUP BY 
        chemblId
);

DROP TABLE IF EXISTS drug_warnings_log SYNC;