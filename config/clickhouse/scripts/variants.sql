-- pharmacogenomics gives array(pharmacogenomicsIds) for each variant
CREATE TABLE if not exists variants engine = EmbeddedRocksDB () primary key variantId as (
    select *
    from variants_log
    left outer join credible_sets_by_variant using (variantId)
    left outer join (
        select
            assumeNotNull(variantId) as variantId,
            groupArray(rowId) as pharmacogenomicsIds
        from pharmacogenomics
        where variantId is not null
        group by variantId
    ) as pgx using (variantId)
);

DROP TABLE IF EXISTS credible_sets_by_variant SYNC;
DROP TABLE IF EXISTS variants_log SYNC;
