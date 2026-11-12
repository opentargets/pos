CREATE TABLE IF NOT EXISTS mechanism_of_action_log (
    chemblIds Array (String),
    targets Array (String),
    mechanismOfAction String,
    actionType Nullable (String),
    targetType Nullable (String),
    targetName Nullable (String),
    curation String,
    references Array (
        Tuple (
            ids Array (String),
            source String,
            urls Array (String)
        )
    )
) engine = Log;