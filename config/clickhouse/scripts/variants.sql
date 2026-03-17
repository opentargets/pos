CREATE TABLE if not exists variants engine = MergeTree ()
order by variantId as (
        select *
        from variants_log
    );

OPTIMIZE TABLE variants FINAL;

DROP TABLE IF EXISTS variants_log SYNC;