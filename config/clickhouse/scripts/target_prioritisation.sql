CREATE TABLE IF NOT EXISTS target_prioritisation_temp ENGINE = MergeTree() ORDER BY targetId AS (
    SELECT
        targetId,
        arrayFilter(x -> x.2 != '', [
        ('geneticConstraint', toString(geneticConstraint)),
        ('hasHighQualityChemicalProbes', toString(hasHighQualityChemicalProbes)),
        ('hasLigand', toString(hasLigand)),
        ('hasPocket', toString(hasPocket)),
        ('hasSafetyEvent', toString(hasSafetyEvent)),
        ('hasSmallMoleculeBinder', toString(hasSmallMoleculeBinder)),
        ('hasTEP', toString(hasTEP)),
        ('isCancerDriverGene', toString(isCancerDriverGene)),
        ('isInMembrane', toString(isInMembrane)),
        ('isSecreted', toString(isSecreted)),
        ('maxClinicalStage', toString(maxClinicalStage)),
        ('mouseKOScore', toString(mouseKOScore)),
        ('mouseOrthologMaxIdentityPercentage', toString(mouseOrthologMaxIdentityPercentage)),
        ('paralogMaxIdentityPercentage', toString(paralogMaxIdentityPercentage)),
        ('tissueDistribution', toString(tissueDistribution)),
        ('tissueSpecificity', toString(tissueSpecificity))
    ])::Array(Tuple(key String, value String)) AS tp
    FROM target_prioritisation_log
);

CREATE TABLE IF NOT EXISTS target_prioritisation ENGINE = MergeTree() ORDER BY targetId AS (
    SELECT
        targetId,
        arrayFilter(x -> x.2 != '', arrayPushBack (
            tp,
            if(
                tp.size0 > 0,
                (
                    'geneEssentiality',
                    CASE 
                        WHEN arrayElement (
                            geneEssentiality.isEssential,
                            1
                        ) = 'true' THEN '-1' 
                        WHEN arrayElement (
                            geneEssentiality.isEssential,
                            1
                        ) = 'false' THEN '0'
                        ELSE ''
                    END
                ),
                ('geneEssentiality', '')
            )
        ))::Array(Tuple(key String, value String)) AS items
    FROM
        target_prioritisation_temp
        LEFT JOIN target_essentiality ON target_prioritisation_temp.targetId = target_essentiality.id
);

OPTIMIZE TABLE target_prioritisation FINAL;

DROP TABLE IF EXISTS target_prioritisation_temp SYNC;

DROP TABLE IF EXISTS target_prioritisation_log SYNC;