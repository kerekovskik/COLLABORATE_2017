CREATE OR REPLACE PACKAGE REPL_PKG is

PROCEDURE SETUP;

PROCEDURE ADD_TABLE(
V_SCHEMA_NAME in dba_users.username%TYPE,
V_TABLE_NAME in DBA_TABLES.table_name%TYPE,
V_CONTAINER_NAME in varchar2 DEFAULT 'N/A'
);


PROCEDURE INIT_MINING (
V_CONTAINER_NAME in VARCHAR2 DEFAULT 'N/A',
B_FORCE BOOLEAN DEFAULT FALSE
);

PROCEDURE INIT_SYNCING (
V_DB_LINK_NAME in VARCHAR2,
V_CONTAINER_NAME in VARCHAR2 DEFAULT 'N/A',
N_SYNC_SCN in NUMBER default 0,
B_FORCE BOOLEAN DEFAULT FALSE
);

PROCEDURE MINE (
V_CONTAINER_NAME in VARCHAR2 DEFAULT 'N/A'
);

PROCEDURE SYNC (
V_DB_LINK_NAME in VARCHAR2,
V_CONTAINER_NAME in VARCHAR2 DEFAULT 'N/A'
);

END REPL_PKG;
/
show errors;

CREATE OR REPLACE PACKAGE BODY REPL_PKG IS
PROCEDURE SETUP
IS
V_MIN_SUPP_LOG v$database.name%type;
V_STMT varchar(4000);
BEGIN
  select SUPPLEMENTAL_LOG_DATA_MIN into V_MIN_SUPP_LOG from v$database;
  --Add minimum supplemental log data to the database;
  IF V_MIN_SUPP_LOG = 'NO'
  THEN
    V_STMT := 'alter database add supplemental log data';
    DBMS_OUTPUT.PUT_LINE('Database level supplemental logging has been enabled');
    execute immediate V_STMT;
  ELSE
    DBMS_OUTPUT.PUT_LINE('Database has minimum required supplemental logging enabled.');
  END IF;



END SETUP;


PROCEDURE SYNC (
V_DB_LINK_NAME in VARCHAR2,
V_CONTAINER_NAME in VARCHAR2 DEFAULT 'N/A'
)
IS 
TYPE REF_CURSOR_TYPE IS REF CURSOR; 
CURSOR_replicated_txns REF_CURSOR_TYPE;
CURSOR_mining_progress REF_CURSOR_TYPE;
incoming_redo replicated_txns%ROWTYPE;
N_MINED_SCN number; 
N_SYNC_SCN number;
N_COUNTER number;
N_LAST_REPLICATED_SCN number;
V_STMT varchar2(4000);
BEGIN 
	--Check if the container is already initialized for replication
	select count(*) into N_COUNTER from sync_progress where DB_LINK = UPPER(V_DB_LINK_NAME) and CONTAINER_NAME = UPPER(V_CONTAINER_NAME);

	IF N_COUNTER = 0 THEN
		RAISE_APPLICATION_ERROR(-20002, 'Container ' || UPPER(V_CONTAINER_NAME) || '@' ||UPPER(V_DB_LINK_NAME) || ' is not initialized. Use the INIT_SYNCING procedure in this package to register the source.');
	END IF;
	
	select SYNC_SCN into N_SYNC_SCN from sync_progress where DB_LINK = UPPER(V_DB_LINK_NAME) and CONTAINER_NAME = UPPER(V_CONTAINER_NAME);

	
	V_STMT := 'select mined_scn from mining_progress@ ' || V_DB_LINK_NAME || ' where CONTAINER_NAME = :V_CONTAINER_NAME';
	execute immediate V_STMT into N_MINED_SCN using upper(V_CONTAINER_NAME);

	--N_MINED_SCN := 2312890;
	
	open CURSOR_replicated_txns for 'select * from replicated_txns@ ' || V_DB_LINK_NAME || ' where CONTAINER_NAME = :V_CONTAINER_NAME  and commit_scn > :N_SYNC_SCN and commit_scn <= :N_MINED_SCN order by id, commit_scn asc' using V_CONTAINER_NAME,N_SYNC_SCN,N_MINED_SCN;			
	LOOP
		FETCH CURSOR_replicated_txns into incoming_redo;
		exit when CURSOR_replicated_txns%notfound;				

		
		
		IF incoming_redo.sql_redo = 'commit' THEN 
			N_LAST_REPLICATED_SCN := incoming_redo.commit_scn;
			update sync_progress set sync_scn = N_LAST_REPLICATED_SCN where DB_LINK = UPPER(V_DB_LINK_NAME) and CONTAINER_NAME = UPPER(V_CONTAINER_NAME);
			commit;
		else 
			execute immediate incoming_redo.sql_redo;
		END IF;
		
	END LOOP;
	close CURSOR_replicated_txns;
	
	

	
	
END;


PROCEDURE ADD_TABLE(
V_SCHEMA_NAME in dba_users.username%TYPE,
V_TABLE_NAME in DBA_TABLES.table_name%TYPE,
V_CONTAINER_NAME in varchar2 DEFAULT 'N/A'
)
IS
V_STMT varchar2(4000);
N_COUNTER number;
V_UTILITY_STMT varchar2(4000);
BEGIN
	N_COUNTER :=0;
	IF V_CONTAINER_NAME in ('N/A','CDB$ROOT')
	THEN
		select count(*) into N_COUNTER from dba_tables
		where
		owner = V_SCHEMA_NAME
		and TABLE_NAME = V_TABLE_NAME;

		IF N_COUNTER = 0
		THEN
			RAISE_APPLICATION_ERROR(-20000,'Table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME || ' does not exist!' );
		END IF;



		/*
			Check whether the table already has supplemental logging on Primary Key or all columns
			either type of logging is sufficient for the logminer
		*/
		N_COUNTER :=0;
		select count(*) into N_COUNTER from dba_log_groups
		where
		owner = V_SCHEMA_NAME
		and TABLE_NAME = V_TABLE_NAME
		and LOG_GROUP_TYPE in
		(
		'PRIMARY KEY LOGGING'
		,'ALL COLUMN LOGGING'
		)
		;

		IF N_COUNTER > 0
		THEN
			/*
				Let the user know that nothing needs to be done to the table
				Then insert the table into the MONITORED_TABLES table
			*/
			DBMS_OUTPUT.PUT_LINE('Table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME ||' already has the necessary supplemental logging enabled.');
			DBMS_OUTPUT.PUT_LINE('No action is necessary on the table.');
			insert into MONITORED_TABLES VALUES (V_CONTAINER_NAME,V_SCHEMA_NAME,V_TABLE_NAME,SYSDATE);
			commit;
		ELSE
			/*
				Add Supplemental logging to the table.
			*/


			N_COUNTER :=0;
			/*
				First check if the table has a primary key.
				If the table has a primary key, you can add PRIMARY KEY supplemental logging
			*/

			select count(*) into N_COUNTER from dba_constraints
			where
			OWNER = V_SCHEMA_NAME
			and TABLE_NAME = V_TABLE_NAME
			and CONSTRAINT_TYPE = 'P';

			IF N_COUNTER > 0
			THEN
				-- Add Primary key logging
				V_STMT := 'alter table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME ||  ' ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS';
				execute immediate V_STMT;
				DBMS_OUTPUT.PUT_LINE('Primary Key logging for table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME ||' is now enabled.');
			ELSE
				-- Add all column logging
				V_STMT := 'alter table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME ||  ' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
				execute immediate V_STMT;
				DBMS_OUTPUT.PUT_LINE('All column logging for table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME ||' is now enabled.');
			END IF;

			insert into MONITORED_TABLES VALUES (V_CONTAINER_NAME,V_SCHEMA_NAME,V_TABLE_NAME,SYSDATE);
			commit;


		END IF;
	ELSE
		--PDB RELATED TASKS

		/*
			Check if a database link exists for the container
		*/
		N_COUNTER := 0;
		select count(*) into N_COUNTER from user_db_links where DB_LINK = UPPER(V_CONTAINER_NAME) || '.WORLD';

		/*
			Raise an application error letting the user know that they need to create a db link.
		*/
		IF N_COUNTER = 0
		THEN
			RAISE_APPLICATION_ERROR(-20001, 'Database link for container ' || UPPER(V_CONTAINER_NAME) || ' does not exist. Use the create_db_link.sql script provided with this program to make a database link.');
		END IF;

		V_STMT := 'select count(*) from dba_tables@' || V_CONTAINER_NAME || ' where owner = :V_SCHEMA_NAME and TABLE_NAME = :V_TABLE_NAME';
		execute immediate V_STMT into N_COUNTER using upper(V_SCHEMA_NAME),upper(V_TABLE_NAME);



		IF N_COUNTER = 0
		THEN
			RAISE_APPLICATION_ERROR(-20000,'Table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME || ' does not exist in container ' || UPPER(V_CONTAINER_NAME) || '!' );
		END IF;

/*

*/

		/*
			Check whether the table already has supplemental logging on Primary Key or all columns
			either type of logging is sufficient for the logminer
		*/
		N_COUNTER :=0;
		V_STMT := q'!select count(*) from dba_log_groups@!' || upper(V_CONTAINER_NAME) || q'! where owner = :V_SCHEMA_NAME and TABLE_NAME = :V_TABLE_NAME and LOG_GROUP_TYPE in ('PRIMARY KEY LOGGING','ALL COLUMN LOGGING')!';


		execute immediate V_STMT into N_COUNTER using upper(V_SCHEMA_NAME),upper(V_TABLE_NAME);



		IF N_COUNTER > 0
		THEN
			/*
				Let the user know that nothing needs to be done to the table
				Then insert the table into the MONITORED_TABLES table
			*/
			DBMS_OUTPUT.PUT_LINE('Table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME ||' already has the necessary supplemental logging enabled.');
			DBMS_OUTPUT.PUT_LINE('No action is necessary on the table.');

			BEGIN
				insert into MONITORED_TABLES VALUES (V_CONTAINER_NAME,V_SCHEMA_NAME,V_TABLE_NAME,SYSDATE);
				commit;
			EXCEPTION
				WHEN DUP_VAL_ON_INDEX THEN
					DBMS_OUTPUT.PUT_LINE('Table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME || ' for container ' || V_CONTAINER_NAME || ' is already being monitored for changes.');
				WHEN OTHERS THEN
					DBMS_OUTPUT.PUT_LINE('Unknown Error: ' || SQLCODE || ' ' || SQLERRM );
			END;


		ELSE
			/*
				Add Supplemental logging to the table.
			*/


			N_COUNTER :=0;
			/*
				First check if the table has a primary key.
				If the table has a primary key, you can add PRIMARY KEY supplemental logging
			*/

			V_STMT := q'!select count(*) from dba_constraints@!' || upper(V_CONTAINER_NAME) || q'! where OWNER = :V_SCHEMA_NAME and TABLE_NAME = :V_TABLE_NAME and CONSTRAINT_TYPE = 'P'!';
			execute immediate V_STMT into N_COUNTER using upper(V_SCHEMA_NAME),upper(V_TABLE_NAME);


			IF N_COUNTER > 0
			THEN
				-- Add Primary key logging
				V_STMT := 'alter table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME ||  ' ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS';
				V_UTILITY_STMT := 'BEGIN DBMS_UTILITY.EXEC_DDL_STATEMENT@' || V_CONTAINER_NAME || '(:V_STMT); END;';
				execute immediate V_UTILITY_STMT using V_STMT;
				DBMS_OUTPUT.PUT_LINE('Primary Key logging for table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME ||' is now enabled in container '|| upper(V_CONTAINER_NAME) || '.');
			ELSE
				-- Add all column logging
				V_STMT := 'alter table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME ||  ' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
				V_UTILITY_STMT := 'BEGIN DBMS_UTILITY.EXEC_DDL_STATEMENT@' || V_CONTAINER_NAME || '(:V_STMT); END;';
				execute immediate V_UTILITY_STMT using V_STMT;
				DBMS_OUTPUT.PUT_LINE('All column logging for table ' || V_SCHEMA_NAME || '.' || V_TABLE_NAME ||' is now enabled in container '|| upper(V_CONTAINER_NAME) || '.');
			END IF;

				BEGIN
					insert into MONITORED_TABLES VALUES (V_CONTAINER_NAME,V_SCHEMA_NAME,V_TABLE_NAME,SYSDATE);
					commit;
				EXCEPTION
					WHEN DUP_VAL_ON_INDEX THEN
						DBMS_OUTPUT.PUT_LINE('Table' || V_SCHEMA_NAME || '.' || V_TABLE_NAME || ' for container ' || V_CONTAINER_NAME || ' is already being monitored for changes.');
					WHEN OTHERS THEN
						DBMS_OUTPUT.PUT_LINE('Unknown Error: ' || SQLCODE || ' ' || SQLERRM );
				END;




		END IF;

/*

*/




-----
	END IF;

END ADD_TABLE;

PROCEDURE INIT_SYNCING(
V_DB_LINK_NAME in VARCHAR2,
V_CONTAINER_NAME in VARCHAR2 DEFAULT 'N/A',
N_SYNC_SCN in NUMBER default 0,
B_FORCE BOOLEAN DEFAULT FALSE
) IS 
N_COUNTER number;
BEGIN
	N_COUNTER :=0;
	select count(*) into N_COUNTER from user_db_links where DB_LINK = UPPER(V_DB_LINK_NAME);
	/*
		Raise an application error letting the user know that they need to create a db link.
	*/
	IF N_COUNTER = 0
	THEN
		RAISE_APPLICATION_ERROR(-20001, 'Database link  ' || UPPER(V_DB_LINK_NAME) || ' does not exist. Use the create_db_link.sql script provided with this program to make a database link.');
	END IF;
	
	--Check if the container is already initialized for replication
	select count(*) into N_COUNTER from sync_progress where DB_LINK = UPPER(V_DB_LINK_NAME) and CONTAINER_NAME = UPPER(V_CONTAINER_NAME);

	IF N_COUNTER > 0 THEN
		IF B_FORCE = TRUE THEN
			update sync_progress set sync_scn = N_SYNC_SCN where DB_LINK = UPPER(V_DB_LINK_NAME) and CONTAINER_NAME = UPPER(V_CONTAINER_NAME);
			commit;
			DBMS_OUTPUT.PUT_LINE('Container ' || UPPER(V_CONTAINER_NAME) || '@' ||UPPER(V_DB_LINK_NAME)  || ' has been re-initialized at SCN ' || N_SYNC_SCN);
		ELSE
			RAISE_APPLICATION_ERROR(-20002, 'Container ' || UPPER(V_CONTAINER_NAME) || '@' ||UPPER(V_DB_LINK_NAME) || ' is already initialized. Use the B_FORCE Argument to reset SYNC_SCN value to V$DATABASE.CURRENT_SCN.');
		END IF;
	ELSE
		insert into sync_progress values (UPPER(V_DB_LINK_NAME), UPPER(V_CONTAINER_NAME), N_SYNC_SCN);
		commit;
		DBMS_OUTPUT.PUT_LINE('Container ' || UPPER(V_CONTAINER_NAME) || '@' ||UPPER(V_DB_LINK_NAME) || ' has been initialized at SCN ' || N_SYNC_SCN);
	END IF;
	

END;



PROCEDURE MINE (
V_CONTAINER_NAME in VARCHAR2 DEFAULT 'N/A'
)
IS
N_BEGIN_SCN number;
N_END_SCN number;
N_MINED_SCN number;
N_MIN_TXN_SCN number;
N_CURRENT_SCN number;
N_COUNTER number;
V_STMT varchar2(4000);
N_LAST_MINED_SCN number;

TYPE REF_CURSOR_TYPE IS REF CURSOR; 
lgmnr_contents12c REF_CURSOR_TYPE;  

V_CUR_SQL_REDO   v$logmnr_contents.sql_redo%type;
N_CUR_COMMIT_SCN  v$logmnr_contents.commit_scn%type;
V_CUR_SEG_NAME    v$logmnr_contents.seg_name%type;
V_CUR_SEG_OWNER   v$logmnr_contents.seg_owner%type;

BEGIN
	N_COUNTER := 0;

	select count(*) into N_COUNTER from mining_progress where CONTAINER_NAME = UPPER(V_CONTAINER_NAME);

	IF N_COUNTER = 0 THEN
		RAISE_APPLICATION_ERROR(-20003, 'Container Specified in V_CONTAINER_NAME Argument has not been initialized. Please call the INIT_MINING procedure of this package.');
	END IF;

	IF V_CONTAINER_NAME = 'N/A' THEN
		select db.current_scn, nvl(min_scn,0) into N_CURRENT_SCN,N_MIN_TXN_SCN from v$database db,
		(
		select min(start_scn) as min_scn from gv$transaction
		) txn ;
	ELSE
		V_STMT := 'select db.current_scn, nvl(min_scn,0)  from v$database db, (select min(start_scn) as min_scn from gv$transaction@' || V_CONTAINER_NAME || ') txn';

		execute immediate V_STMT into N_CURRENT_SCN,N_MIN_TXN_SCN;
	END IF;

	/*
		Set the N_END_SCN variable to N_CURRENT_SCN if the N_MIN_TXN_SCN is 0 (meaning that there were no active transactions at the start of minng)
		If N_MIN_TXN_SCN is a non-zero number, Subtract 1 and then make it the N_END_SCN if and only if it is lower than N_CURRENT_SCN.
	*/


	IF N_MIN_TXN_SCN = 0 THEN
		N_END_SCN := N_CURRENT_SCN;
	ELSIF N_MIN_TXN_SCN < N_CURRENT_SCN THEN
		N_END_SCN := N_MIN_TXN_SCN -1 ;
	END IF;


	select mined_scn into N_MINED_SCN from mining_progress where container_name = UPPER(V_CONTAINER_NAME);

	/*
		Set the N_BEGIN_SCN as the MINED_SCN value plus 1
	*/

	N_BEGIN_SCN := N_MINED_SCN +1;

  	DBMS_OUTPUT.PUT_LINE('N_CURRENT_SCN: ' || N_CURRENT_SCN);
	DBMS_OUTPUT.PUT_LINE('N_MIN_TXN_SCN: ' || N_MIN_TXN_SCN);
	DBMS_OUTPUT.PUT_LINE('N_MINED_SCN: ' || N_MINED_SCN);
	DBMS_OUTPUT.PUT_LINE('N_BEGIN_SCN: ' || N_BEGIN_SCN);
	DBMS_OUTPUT.PUT_LINE('N_END_SCN: ' || N_END_SCN);

	IF N_BEGIN_SCN > N_END_SCN  THEN
		NULL;
		DBMS_OUTPUT.PUT_LINE('A long running transaction is keeping replication from continuing. Please try again later.');
	ELSE 
		BEGIN
	
			DBMS_LOGMNR.START_LOGMNR(OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
			+ DBMS_LOGMNR.COMMITTED_DATA_ONLY
			+ DBMS_LOGMNR.NO_ROWID_IN_STMT
			+ DBMS_LOGMNR.NO_SQL_DELIMITER
			+ DBMS_LOGMNR.CONTINUOUS_MINE,
			startScn => N_BEGIN_SCN,
			endScn => N_END_SCN
			);
		EXCEPTION 
			WHEN OTHERS THEN 
				DBMS_OUTPUT.PUT_LINE('N_CURRENT_SCN: ' || N_CURRENT_SCN);
				DBMS_OUTPUT.PUT_LINE('N_MIN_TXN_SCN: ' || N_MIN_TXN_SCN);
				DBMS_OUTPUT.PUT_LINE('N_MINED_SCN: ' || N_MINED_SCN);
				DBMS_OUTPUT.PUT_LINE('N_BEGIN_SCN: ' || N_BEGIN_SCN);
				DBMS_OUTPUT.PUT_LINE('N_END_SCN: ' || N_END_SCN);
				RAISE;
		END;
		
		N_LAST_MINED_SCN := 0;
		IF V_CONTAINER_NAME = 'N/A' THEN 
			for rec in (
				select sql_redo,commit_scn,seg_name,seg_owner from v$logmnr_contents                                                                                
				where                                                                                                                
				operation in ('UPDATE','INSERT','DELETE','COMMIT')                                                                   
				and                                                                                                                  
				xid in 
				(
					select distinct xid from v$logmnr_contents where SEG_OWNER ||'.' || SEG_NAME in 
					(
						select schema || '.' || table_name from monitored_tables where container_name = upper(V_CONTAINER_NAME)
					) 
					and operation in ('UPDATE','INSERT','DELETE')
				)
			)
			LOOP
				insert into replicated_txns 
				(
					ID,
					COMMIT_SCN,
					SEG_OWNER,
					SEG_NAME,
					SQL_REDO,
					CONTAINER_NAME 
				)
				values 
				(
					replicated_txns_seq.NEXTVAL,
					rec.commit_scn,
					rec.seg_owner,
					rec.seg_name,
					rec.SQL_REDO,
					upper(V_CONTAINER_NAME)
				);
				DBMS_OUTPUT.PUT_LINE('X');
				N_LAST_MINED_SCN := rec.commit_scn;
			END LOOP;
		ELSE 
			--I had to use a dynamic SQL ref cursor so that the code would be portable between 11g and 12c versions of v$logmnr_contents 
			OPEN lgmnr_contents12c for q'!select sql_redo,commit_scn,seg_name,seg_owner from v$logmnr_contents where operation in ('UPDATE','INSERT','DELETE','COMMIT') and xid in ( select distinct xid from v$logmnr_contents where SEG_OWNER ||'.' || SEG_NAME in (select schema || '.' || table_name from monitored_tables where container_name = upper(:V_CONTAINER_NAME)) and operation in ('UPDATE','INSERT','DELETE')) and SRC_CON_NAME = upper(:V_CONTAINER_NAME)!' using V_CONTAINER_NAME,V_CONTAINER_NAME;
			LOOP
				FETCH lgmnr_contents12c into V_CUR_SQL_REDO ,N_CUR_COMMIT_SCN ,V_CUR_SEG_NAME ,V_CUR_SEG_OWNER;
				exit when lgmnr_contents12c%notfound;				
				insert into replicated_txns 
				(
					ID,
					COMMIT_SCN,
					SEG_OWNER,
					SEG_NAME,
					SQL_REDO,
					CONTAINER_NAME 
				)
				values 
				(
					replicated_txns_seq.NEXTVAL,
					N_CUR_COMMIT_SCN,
					V_CUR_SEG_OWNER,
					V_CUR_SEG_NAME,
					V_CUR_SQL_REDO,
					upper(V_CONTAINER_NAME)
				);
				DBMS_OUTPUT.PUT_LINE('X');
				N_LAST_MINED_SCN := N_CUR_COMMIT_SCN;
			END LOOP;
			close lgmnr_contents12c;
			
	
			
		END IF;
		
		IF N_LAST_MINED_SCN != 0 THEN 
			update mining_progress set mined_scn = N_LAST_MINED_SCN;
		ELSE 
			update mining_progress set mined_scn = N_END_SCN;
		END IF;
		commit;
	END IF;

EXCEPTION 
	WHEN OTHERS THEN 
		ROLLBACK;

END MINE;

PROCEDURE INIT_MINING (
V_CONTAINER_NAME in VARCHAR2 DEFAULT 'N/A',
B_FORCE BOOLEAN DEFAULT FALSE
)
IS
N_CURRENT_SCN number;
N_MIN_TXN_SCN number;
N_START_SCN number;
N_COUNTER number;
BEGIN

	select current_scn into N_CURRENT_SCN from v$database;
	--Check if the container is already initialized for replication
	select count(*) into N_COUNTER from mining_progress where CONTAINER_NAME = UPPER(V_CONTAINER_NAME);

	IF N_COUNTER > 0 THEN
		IF B_FORCE = TRUE THEN
			update mining_progress set MINED_SCN = N_CURRENT_SCN where CONTAINER_NAME = UPPER(V_CONTAINER_NAME);
			commit;
			DBMS_OUTPUT.PUT_LINE('Container ' || V_CONTAINER_NAME || ' has been re-initialized at SCN ' || N_CURRENT_SCN);
		ELSE
			RAISE_APPLICATION_ERROR(-20002, 'Container ' || UPPER(V_CONTAINER_NAME) || ' is already initialized. Use the B_FORCE Argument to advance MINED_SCN value to V$DATABASE.CURRENT_SCN.');
		END IF;
	ELSE
		insert into mining_progress values (UPPER(V_CONTAINER_NAME), N_CURRENT_SCN);
		commit;
		DBMS_OUTPUT.PUT_LINE('Container ' || V_CONTAINER_NAME || ' has been initialized at SCN ' || N_CURRENT_SCN);
	END IF;






END INIT_MINING;

END;
/
show errors;