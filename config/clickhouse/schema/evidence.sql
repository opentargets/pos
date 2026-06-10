CREATE TABLE IF NOT EXISTS evidence_log (
    `id` String,
    `score` Float64,
    `targetId` String,
    `diseaseId` String,
    `biomarkerName` Nullable (String),
    `biomarkers` Tuple (
        geneExpression Array (
            Tuple (
                name Nullable (String),
                id Nullable (String)
            )
        ),
        geneticVariation Array (
            Tuple (
                id Nullable (String),
                name Nullable (String),
                functionalConsequenceId Nullable (String)
            )
        )
    ),
    `studyLocusId` Nullable (String),
    `diseaseCellLines` Array (
        Tuple (
            id Nullable (String),
            name Nullable (String),
            tissue Nullable (String),
            tissueId Nullable (String)
        )
    ),
    `cohortPhenotypes` Array (String),
    `targetInModel` Nullable (String),
    `reactionId` Nullable (String),
    `reactionName` Nullable (String),
    `projectId` Nullable (String),
    `variantId` Nullable (String),
    `variantRsId` Nullable (String),
    `oddsRatioConfidenceIntervalLower` Nullable (Float64),
    `studySampleSize` Nullable (UInt32),
    `variantAminoacidDescriptions` Array (String),
    `mutatedSamples` Array (
        Tuple (
            functionalConsequenceId Nullable (String),
            numberMutatedSamples Nullable (UInt32),
            numberSamplesTested Nullable (UInt32),
            numberSamplesWithMutationType Nullable (UInt32)
        )
    ),
    `drugId` Nullable (String),
    `drugFromSource` Nullable (String),
    `drugResponse` Nullable (String),
    `cohortShortName` Nullable (String),
    `diseaseModelAssociatedModelPhenotypes` Array (Tuple (id String, label String)),
    `diseaseModelAssociatedHumanPhenotypes` Array (Tuple (id String, label String)),
    `significantDriverMethods` Array (String),
    `pValueExponent` Nullable (Int32),
    `log2FoldChangePercentileRank` Nullable (Int32),
    `biologicalModelAllelicComposition` Nullable (String),
    `confidence` Nullable (String),
    `resourceScore` Nullable (Float64),
    `variantFunctionalConsequenceId` Nullable (String),
    `variantFunctionalConsequenceFromQtlId` Nullable (String),
    `biologicalModelGeneticBackground` Nullable (String),
    `urls` Array (
        Tuple (
            url Nullable (String),
            niceName Nullable (String)
        )
    ),
    `literature` Array (String),
    `pmcIds` Array (String),
    `studyCases` Nullable (UInt32),
    `studyOverview` Nullable (String),
    `allelicRequirements` Array (String),
    `datasourceId` String,
    `datatypeId` String,
    `oddsRatioConfidenceIntervalUpper` Nullable (Float64),
    `log2FoldChangeValue` Nullable (Float64),
    `oddsRatio` Nullable (Float64),
    `cohortDescription` Nullable (String),
    `publicationYear` Nullable (UInt16),
    `diseaseFromSource` Nullable (String),
    `diseaseFromSourceId` Nullable (String),
    `targetFromSourceId` Nullable (String),
    `targetModulation` Nullable (String),
    `textMiningSentences` Array (
        Tuple (
            dEnd UInt32,
            tEnd UInt32,
            dStart UInt32,
            tStart UInt32,
            section String,
            text String
        )
    ),
    `studyId` Nullable (String),
    `clinicalSignificances` Array (String),
    `cohortId` Nullable (String),
    `pValueMantissa` Nullable (Float64),
    `pathways` Array (
        Tuple (
            id Nullable (String),
            name Nullable (String)
        )
    ),
    `publicationFirstAuthor` Nullable (String),
    `alleleOrigins` Array (String),
    `biologicalModelId` Nullable (String),
    `biosamplesFromSource` Array (String),
    `diseaseFromSourceMappedId` Nullable (String),
    `beta` Nullable (Float64),
    `betaConfidenceIntervalLower` Nullable (Float64),
    `betaConfidenceIntervalUpper` Nullable (Float64),
    `studyStartDate` Nullable (String),
    `targetFromSource` Nullable (String),
    `cellLineBackground` Nullable (String),
    `contrast` Nullable (String),
    `crisprScreenLibrary` Nullable (String),
    `cellType` Nullable (String),
    `statisticalTestTail` Nullable (String),
    `interactingTargetFromSourceId` Nullable (String),
    `phenotypicConsequenceLogFoldChange` Nullable (Float64),
    `phenotypicConsequencePValue` Nullable (Float64),
    `geneticInteractionScore` Nullable (Float64),
    `geneticInteractionFDR` Nullable (Float64),
    `biomarkerList` Array (
        Tuple (
            name String,
            description String
        )
    ),
    `projectDescription` Nullable (String),
    `geneticInteractionType` Nullable (String),
    `targetRole` Nullable (String),
    `interactingTargetRole` Nullable (String),
    `ancestry` Nullable (String),
    `ancestryId` Nullable (String),
    `statisticalMethod` Nullable (String),
    `statisticalMethodOverview` Nullable (String),
    `studyCasesWithQualifyingVariants` Nullable (UInt32),
    `releaseVersion` Nullable (String),
    `releaseDate` Nullable (String),
    `warningMessage` Nullable (String),
    `directionOnTarget` Nullable (String),
    `directionOnTrait` Nullable (String),
    `assessments` Array (String),
    `primaryProjectHit` Nullable (Bool),
    `primaryProjectId` Nullable (String),
    `assays` Array (
        Tuple (
            description Nullable (String),
            isHit Nullable (Bool),
            shortName Nullable (String)
        )
    ),
    `clinicalReportId` Nullable (String),
    `clinicalStage` Nullable (String),
    `trialWhyStopped` Nullable (String),
    `trialStopReasonCategories` Array (String),
    `qualityControls` Array (String),
    `publicationDate` Nullable (String),
    `evidenceDate` Nullable (String),
    `validationReadouts` Array (
        Tuple (
            hsaValue Nullable (Float64),
            isValidated Nullable (Bool),
            readoutMethodName Nullable (String),
            screen Nullable (String)
        )
    )
) engine = Log;
