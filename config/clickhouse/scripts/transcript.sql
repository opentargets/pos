CREATE TABLE if not exists transcript engine = EmbeddedRocksDB () primary key targetId as (
    select *
    from transcript_log
);

DROP TABLE IF EXISTS transcript_log SYNC;
