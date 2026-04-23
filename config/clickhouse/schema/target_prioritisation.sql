CREATE TABLE IF NOT EXISTS target_prioritisation_log (
    targetId String,
    geneticConstraint Nullable (Float64),
    hasHighQualityChemicalProbes Nullable (UInt8),
    hasLigand Nullable (UInt8),
    hasPocket Nullable (UInt8),
    hasSafetyEvent Nullable (Int8),
    hasSmallMoleculeBinder Nullable (UInt8),
    hasTEP Nullable (UInt8),
    isCancerDriverGene Nullable (Int8),
    isInMembrane Nullable (UInt8),
    isSecreted Nullable (UInt8),
    maxClinicalStage Nullable (Float64),
    mouseKOScore Nullable (Float64),
    mouseOrthologMaxIdentityPercentage Nullable (Float64),
    paralogMaxIdentityPercentage Nullable (Float64),
    tissueDistribution Nullable (Float64),
    tissueSpecificity Nullable (Float64)
) ENGINE = Log;