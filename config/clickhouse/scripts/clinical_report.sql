create table if not exists clinical_report engine = MergeTree ()
order by id as (
        select *
        from clinical_report_log
    );

drop table clinical_report_log;