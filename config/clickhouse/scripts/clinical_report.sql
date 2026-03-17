create table if not exists clinical_report engine = MergeTree ()
order by id as (
        select *
        from clinical_report_log
    );

OPTIMIZE TABLE clinical_report FINAL;

drop table clinical_report_log;