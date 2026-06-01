create table if not exists literature_entity_lut_log (
    pmid String,
    pmcid Nullable (String),
    date Date,
    year UInt16,
    month UInt8,
    day UInt8,
    keywordId String,
    relevance Float64,
    keywordType FixedString (2)
) engine = Log;
