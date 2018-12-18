--drop table AUDT_DTL_TBL cascade constraints;
CREATE TABLE AUDT_DTL_TBL
(
AUDT_KEY NUMBER(25,0) PRIMARY KEY,
CHNG_TMSTMP TIMESTAMP,
TBL_NM VARCHAR2(32 BYTE),
ROW_ID ROWID,
BTCH_ID NUMBER,
PK_NM VARCHAR2(500 BYTE),
PK_VAL VARCHAR2(500 BYTE),
COL_NM VARCHAR2(32 BYTE),
OLD_VAL VARCHAR2(100 BYTE),
NEW_VAL VARCHAR2(100 BYTE),	
CHNG_TYP VARCHAR2(30 BYTE),
DB_USR_ID VARCHAR2(32 BYTE),
OPER_SYS_USR_ID VARCHAR2(32 BYTE),
MDUL_NM VARCHAR2(500 BYTE)
);

--drop table AUDT_EXCL_LST_TBL cascade constraints;
CREATE TABLE AUDT_EXCL_LST_TBL
(
  EXCL_ID   INTEGER,
  TBL_NM    VARCHAR2(32 BYTE),
  USR_ID    VARCHAR2(32 BYTE),
  EXCL_IND  VARCHAR2(1 BYTE)
);

--DROP SEQUENCE AUDT_BTCHID_SEQ;
CREATE SEQUENCE AUDT_BTCHID_SEQ INCREMENT BY 1 START WITH 1 CACHE 100;
--DROP SEQUENCE AUDT_AUDTID_SEQ;
CREATE SEQUENCE AUDT_AUDTID_SEQ INCREMENT BY 1 START WITH 1 CACHE 10000;

set serveroutput on;
CREATE OR REPLACE PROCEDURE sp_CRT_AUDT_TRG(TableName IN varchar2, TableOwner IN varchar2)
AUTHID CURRENT_USER
IS
  v_sql_stmt clob;
  v_PK_String varchar2(500);

--Declare cursor to retrieve all primary key columns for the table
  CURSOR c_Pk_Values IS
    SELECT cols.column_name
    FROM all_constraints cons, all_cons_columns cols
    WHERE cols.table_name = TableName
    AND cons.constraint_type = 'P'
    AND cons.owner= TableOwner
    AND cons.constraint_name = cols.constraint_name
    AND cons.owner = cols.owner
    ORDER BY cols.table_name, cols.position;
  v_Excln_lst varchar2(4000); --This variable will hold a list of all database user ids whose changes will not be captured by the audit table
  begin

--get ~ separated list of all primary keys
    SELECT listagg(cols.column_name,'~') within group (order by cols.table_name, cols.position) into v_PK_String
    FROM all_constraints cons, all_cons_columns cols
    WHERE cols.table_name = TableName
    AND cons.constraint_type = 'P'
    AND cons.owner= TableOwner
    AND cons.constraint_name = cols.constraint_name
    AND cons.owner = cols.owner
    ORDER BY cols.table_name, cols.position;

--Get user ids whose DML actions should not be audited. Every time a user id is added to the exclusion table, the procedure should be run again
    SELECT NVL(LISTAGG(chr(39)||USR_ID||chr(39),',') WITHIN GROUP (ORDER BY USR_ID),CHR(39)||'DUMMYID'||CHR(39)) INTO v_Excln_lst FROM AUDT_EXCL_LST_TBL WHERE TBL_NM=TableName AND EXCL_IND='Y';


--Start creation of trigger script
    v_sql_stmt := 'CREATE OR REPLACE TRIGGER '||TableName||'_ADT ' || CHR(10)||
                  '  AFTER INSERT OR UPDATE OR DELETE ON '||TableOwner||'.'||TableName||' REFERENCING NEW AS NEW OLD AS OLD ' ||CHR(10)||
                  '  FOR EACH ROW ' ||CHR(10)||
                  ' WHEN (SYS_CONTEXT('||CHR(39)||'USERENV'||CHR(39)||','||CHR(39)||'SESSION_USER'||CHR(39)||') NOT IN ('||v_Excln_lst||'))'||CHR(10)||
                  ' DECLARE '||CHR(10)||
                  ' v_Btch_Sequence_Id integer;'||CHR(10)||
				  ' v_Chng_Typ varchar2(30);'||CHR(10)||
                  ' v_PkValList varchar2(500);'||CHR(10)||
                  'BEGIN ' ||CHR(10)||
                  
--Get next value of sequence to assign to BTCH_ID
                  'SELECT AUDT_BTCHID_SEQ.nextval INTO v_Btch_Sequence_Id FROM DUAL;'||CHR(10)||
                  
--Identify type of DML firing the trigger
                  'if updating then '||CHR(10)||
                  '  v_Chng_Typ:='||CHR(39)||'UPDATE'|| CHR(39)||';'||CHR(10)||
                  'elsif deleting then '||CHR(10)||
                  '  v_Chng_Typ:='||CHR(39)||'DELETE'||CHR(39)||';'||CHR(10)||
                  'elsif inserting then '||CHR(10)||
                  ' v_Chng_Typ:='||CHR(39)||'INSERT'||CHR(39)||';'||CHR(10)||
                  'end if;'||CHR(10);

--Get list of primary key values to be inserted into the audit table
    FOR i IN c_Pk_Values
    LOOP
        v_sql_stmt:=v_sql_stmt||' if inserting then '|| CHR(10)||
                                ' v_PkValList:= v_PkValList||'||CHR(39)||'~'||CHR(39)||'||:NEW.'||i.column_name||';'||CHR(10)||
                                'elsif updating or deleting then '||CHR(10)||
                                ' v_PkValList:= v_PkValList||'||CHR(39)||'~'||CHR(39)||'||:OLD.'||i.column_name||';'||CHR(10)||
                                'END IF;'||CHR(10);
    END LOOP;
        v_sql_stmt:=v_sql_stmt||'v_PkValList:=LTRIM(v_PkValList,'||CHR(39)||'~'||CHR(39)||');'||CHR(10);
    
--iterate through all columns of the table to create trigger on every column
    for x in (select column_name, table_name from all_tab_cols where table_name = TableName and owner= TableOwner)
    loop
	v_sql_stmt := v_sql_stmt || 'if updating('||CHR(39)||x.column_name||CHR(39)||') AND :NEW.'||x.column_name||'<> :OLD.'||x.column_name||' then '||CHR(10)||
					'  insert into AUDT_DTL_TBL select AUDT_AUDTID_SEQ.nextval,SYSTIMESTAMP,'||CHR(39)||TableName||CHR(39)||',:NEW.ROWID,v_Btch_Sequence_Id,'||CHR(39)||v_PK_String||CHR(39)||',v_PkValList,'||CHR(39)||x.column_name||CHR(39)||',:old.'||x.column_name||',:new.'||x.column_name||','||CHR(39)||'UPDATE'||CHR(39)||',sys_context('||CHR(39)||'USERENV'||CHR(39)||','||CHR(39)||'SESSION_USER'||CHR(39)||'),sys_context('||CHR(39)||'USERENV'||CHR(39)||','||CHR(39)||'OS_USER'||CHR(39)||'),sys_context('||CHR(39)||'USERENV'||CHR(39)||','||CHR(39)||'MODULE'||CHR(39)||') from dual;'||CHR(10)||
                    'elsif deleting then '||CHR(10)||
					'  insert into AUDT_DTL_TBL select AUDT_AUDTID_SEQ.nextval,SYSTIMESTAMP,'||CHR(39)||TableName||CHR(39)||',:OLD.ROWID,v_Btch_Sequence_Id,'||CHR(39)||v_PK_String||CHR(39)||',v_PkValList,'||CHR(39)||x.column_name||CHR(39)||',:old.'||x.column_name||',:new.'||x.column_name||','||CHR(39)||'DELETE'||CHR(39)||',sys_context('||CHR(39)||'USERENV'||CHR(39)||','||CHR(39)||'SESSION_USER'||CHR(39)||'),sys_context('||CHR(39)||'USERENV'||CHR(39)||','||CHR(39)||'OS_USER'||CHR(39)||'),sys_context('||CHR(39)||'USERENV'||CHR(39)||','||CHR(39)||'MODULE'||CHR(39)||') from dual;'||CHR(10)||
                    'elsif inserting then '||CHR(10)||
					'  insert into AUDT_DTL_TBL select AUDT_AUDTID_SEQ.nextval,SYSTIMESTAMP,'||CHR(39)||TableName||CHR(39)||',:NEW.ROWID,v_Btch_Sequence_Id,'||CHR(39)||v_PK_String||CHR(39)||',v_PkValList,'||CHR(39)||x.column_name||CHR(39)||',:old.'||x.column_name||',:new.'||x.column_name||','||CHR(39)||'INSERT'||CHR(39)||',sys_context('||CHR(39)||'USERENV'||CHR(39)||','||CHR(39)||'SESSION_USER'||CHR(39)||'),sys_context('||CHR(39)||'USERENV'||CHR(39)||','||CHR(39)||'OS_USER'||CHR(39)||'),sys_context('||CHR(39)||'USERENV'||CHR(39)||','||CHR(39)||'MODULE'||CHR(39)||') from dual;'||CHR(10)||
                    'end if; '||CHR(10);
    end loop;
v_sql_stmt := v_sql_stmt ||'END; '||CHR(10);

--Enable for debugging only. May have to use DBMS_OUTPUT.ENABLE(100000) for increasing output buffer
--DBMS_OUTPUT.ENABLE(100000000);
--dbms_output.put_line(v_sql_stmt); 

--Dynamic creation of trigger by executing the script created by the procedure
execute immediate v_sql_stmt;

--Exception handler.
EXCEPTION
WHEN OTHERS THEN
DBMS_OUTPUT.PUT_LINE('Error -'||SQLERRM);
end;
/

--set serveroutput on;
--EXECUTE sp_crt_audt_trg('DEMO_CUSTOMERS','INSIGHT');