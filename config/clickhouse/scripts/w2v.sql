create table if not exists ml_w2v engine = MergeTree ()
order by (word) primary key (word) as
select category, word, norm, vector
from (
        select category, word, norm, vector
        from ml_w2v_log
    );

OPTIMIZE TABLE ml_w2v FINAL;

drop table ml_w2v_log SYNC;