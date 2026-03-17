CREATE TABLE IF NOT EXISTS mechanism_of_action ENGINE = MergeTree() ORDER BY chemblId AS (
    SELECT 
        arrayJoin(chemblIds) AS chemblId,
        groupArray(
            (
            mechanismOfAction,
            actionType,
            targetName,
            targets,
            references
            )::Tuple(
                mechanismOfAction String,
                actionType Nullable (String),
                targetName Nullable (String),
                targets Array (String),
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

OPTIMIZE TABLE mechanism_of_action FINAL;

DROP TABLE IF EXISTS mechanism_of_action_log SYNC;