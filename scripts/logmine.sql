set echo on;
BEGIN
sys.dbms_logmnr.start_logmnr (
startscn => &1,
endscn => &2,
options => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + 
DBMS_LOGMNR.COMMITTED_DATA_ONLY + 
DBMS_LOGMNR.CONTINUOUS_MINE +
DBMS_LOGMNR.NO_ROWID_IN_STMT
);
END;
/

