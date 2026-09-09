create table if not exists clinical_report_log (
    id String,
    source LowCardinality (String),
    clinicalStage LowCardinality (String),
    origin String,
    phaseFromSource Nullable (String),
    provider String,
    type Nullable (String),
    title Nullable (String),
    trialStudyType Nullable (String),
    trialDescription Nullable (String),
    trialNumberOfArms Nullable (Int32),
    trialStartDate Nullable (Date),
    trialLiterature Array (
        Tuple (
            id String,
            type String,
        )
    ),
    trialOverallStatus Nullable (String),
    trialWhyStopped Nullable (String),
    trialPrimaryPurpose Nullable (String),
    trialPhase Nullable (String),
    trialStopReasonCategories Array (String),
    trialSponsor Tuple (
        agencyClass Nullable (String),
        name Nullable (String)
    ),
    qualityControls Array (String),
    diseases Array (
        Tuple (
            diseaseFromSource String,
            diseaseId String
        )
    ),
    drugs Array (
        Tuple (
            drugFromSource String,
            drugId String
        )
    ),
    countries Array (String),
    year Nullable (Int32),
    sideEffects Array (
        Tuple (
            diseaseId Nullable (String),
            diseaseFromSource Nullable (String)
        )
    ),
    trialOfficialTitle Nullable (String),
    url Nullable (String)
) engine = Log;
