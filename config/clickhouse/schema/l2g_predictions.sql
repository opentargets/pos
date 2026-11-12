CREATE TABLE if not exists l2g_predictions_log (
    credibleSetId String,
    geneId String,
    score Float64,
    features Array (
        Tuple (
            name LowCardinality (String),
            value Float64,
            shapValue Float64
        )
    ),
    shapBaseValue Float64
) engine = Log;