set echo on;
col sql_redo format a160
col container_name format a16
set lines 200
select container_name, commit_scn, sql_redo from replicated_txns order by id,commit_scn;

