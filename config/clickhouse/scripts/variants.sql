CREATE TABLE if not exists variants engine = EmbeddedRocksDB () primary key variantId as (
    select *
    from variants_log
    left outer join credible_sets_by_variant
        on variants_log.variantId = credible_sets_by_variant.variantId
);

DROP TABLE IF EXISTS credible_sets_by_variant SYNC;
DROP TABLE IF EXISTS variants_log SYNC;
