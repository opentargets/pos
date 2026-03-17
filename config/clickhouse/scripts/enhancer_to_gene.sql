create table if not exists enhancer_to_gene engine = MergeTree ()
order by (chromosome, start, end) as (
        select *
        from enhancer_to_gene_log
    );

OPTIMIZE TABLE enhancer_to_gene FINAL;

drop table enhancer_to_gene_log SYNC;