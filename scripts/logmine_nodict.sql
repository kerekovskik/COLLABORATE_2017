set echo on;
BEGIN
sys.dbms_logmnr.start_logmnr (
startscn => &1,
endscn => &2,
options => DBMS_LOGMNR.COMMITTED_DATA_ONLY +
DBMS_LOGMNR.CONTINUOUS_MINE
);
END;
/

