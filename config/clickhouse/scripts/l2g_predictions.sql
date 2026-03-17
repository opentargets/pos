CREATE TABLE IF NOT EXISTS l2g_predictions ENGINE = MergeTree ()
ORDER BY studyLocusId AS (
        SELECT studyLocusId, reverse(
                arraySort (
                    l -> l.score, groupArray (
                        CAST(
                            (
                                studyLocusId, geneId, score, features, shapBaseValue
                            ), 'Tuple(studyLocusId String, geneId String, score Float64, features Array(Tuple(name LowCardinality(String), value Float64, shapValue Float64)), shapBaseValue Float64)'
                        )
                    )
                )
            ) as l2g_predictions
        FROM l2g_predictions_log
        GROUP BY
            studyLocusId
    );

OPTIMIZE TABLE l2g_predictions FINAL;

DROP TABLE IF EXISTS l2g_predictions_log SYNC;