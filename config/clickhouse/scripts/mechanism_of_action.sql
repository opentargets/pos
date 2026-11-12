CREATE TABLE IF NOT EXISTS mechanism_of_action ENGINE = EmbeddedRocksDB () PRIMARY KEY chemblId AS (
    SELECT 
        arrayJoin(chemblIds) AS chemblId,
        groupArray(
            (
            mechanismOfAction,
            actionType,
            targetName,
            targets,
            curation,
            references
            )::Tuple(
                mechanismOfAction String,
                actionType Nullable (String),
                targetName Nullable (String),
                targets Array (String),
                curation String,
                references Array (
                    Tuple (
                        ids Array (String),
                        source String,
                        urls Array (String)
                    )
                )
            )
        ) AS rows,
        groupArrayDistinct(targetType) AS uniqueTargetTypes,
        groupArrayDistinct(actionType) AS uniqueActionTypes
    FROM mechanism_of_action_log
    GROUP BY
        chemblId
);

DROP TABLE IF EXISTS mechanism_of_action_log SYNC;