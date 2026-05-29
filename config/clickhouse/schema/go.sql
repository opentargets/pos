CREATE TABLE IF NOT EXISTS gene_ontology_log (
    id String,
    label String,
    namespace LowCardinality (String),
    altIds Array (String),
    isA Array (String),
    partOf Array (String),
    regulates Array (String),
    negativelyRegulates Array (String),
    positivelyRegulates Array (String),
    isObsolete Bool,
    ancestors Array (String),
    descendants Array (String)
) ENGINE = Log;
