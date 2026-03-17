create table if not exists clinical_target engine = MergeTree ()
order by (targetId, id) primary key (targetId) as
select id,
    drugId,
    targetId,
    diseases,
    maxClinicalStage,
    clinicalReportIds
from clinical_target_log
where targetId is not null;

OPTIMIZE TABLE clinical_target FINAL;
drop table clinical_target_log;
