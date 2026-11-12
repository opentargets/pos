CREATE TABLE IF NOT EXISTS l2g_predictions ENGINE = EmbeddedRocksDB () PRIMARY KEY credibleSetId AS (
    SELECT credibleSetId, reverse(
            arraySort (
                l -> l.score, groupArray (
                    CAST(
                        (
                            credibleSetId, geneId, score, features, shapBaseValue
                        ), 'Tuple(credibleSetId String, geneId String, score Float64, features Array(Tuple(name LowCardinality(String), value Float64, shapValue Float64)), shapBaseValue Float64)'
                    )
                )
            )
        ) as l2g_predictions
    FROM l2g_predictions_log
    GROUP BY
        credibleSetId
);

DROP TABLE IF EXISTS l2g_predictions_log SYNC;