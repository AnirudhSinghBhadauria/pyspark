import duckdb
import pandas as pd
# pd.set_option("display.max_rows", none)
# pd.set_option("display.max_columns", none)
path = "C:/Users/MSOUVAN/Desktop/latest_ent_bill/export.tsv"
ENT_ORG_PersonLKUP = "C:/Users/MSOUVAN/Desktop/latest_ent_bill/ENT_ORG_PersonLKUP.csv"
ENT_ORG_PersonTICIDHistoryLKUP ="C:/Users/MSOUVAN/Desktop/latest_ent_bill/ENT_ORG_PersonTICIDHistoryLKUP.csv"
ENT_ORG_ProcessingOfficeLKUP ="C:/Users/MSOUVAN/Desktop/latest_ent_bill/ENT_ORG_ProcessingOfficeLKUP.csv"
TOOL_ORG_TrackIt_Team ="C:/Users/MSOUVAN/Desktop/latest_ent_bill/TOOL_ORG_TrackIt_Team.csv"
TOOL_ORG_TrackIt_Users ="C:/Users/MSOUVAN/Desktop/latest_ent_bill/TOOL_ORG_TrackIt_Users.csv"
OPER_BI_ServiceCenterClosedAHTLKUP ="C:/Users/MSOUVAN/Desktop/latest_ent_bill/OPER_BI_ServiceCenterClosedAHTLKUP.csv"
TOOL_ORG_TrackIt_VolumeCount ="C:/Users/MSOUVAN/Desktop/latest_ent_bill/TOOL_ORG_TrackIt_VolumeCount;.csv"
# Connect to DuckDB (creates an in-memory database)
conn = duckdb.connect()

 
def read_file(path, ENT_ORG_PersonLKUP):
    try:
        print(f"Reading {path} into VC_DTL_FLT_TBL")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS VC_DTL_FLT_TBL AS SELECT * FROM read_csv_auto('{path}', sep='\t')"
        )
        print(f"Reading {ENT_ORG_PersonLKUP} into ENT_ORG_PersonLKUP")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS ENT_ORG_PersonLKUP AS SELECT * FROM read_csv_auto('{ENT_ORG_PersonLKUP}', sep=',', ignore_errors=True)"
        )
        print(f"Reading {ENT_ORG_PersonTICIDHistoryLKUP} into VC_DTL_FLT_TBL")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS ENT_ORG_PersonTICIDHistoryLKUP AS SELECT * FROM read_csv_auto('{ENT_ORG_PersonTICIDHistoryLKUP}', sep='|', ignore_errors=True)"
        )
        print(f"Reading {ENT_ORG_ProcessingOfficeLKUP} into VC_DTL_FLT_TBL")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS ENT_ORG_ProcessingOfficeLKUP AS SELECT * FROM read_csv_auto('{ENT_ORG_ProcessingOfficeLKUP}', sep='|', ignore_errors=True)"
        )
        print(f"Reading {TOOL_ORG_TrackIt_Team} into VC_DTL_FLT_TBL")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS TOOL_ORG_TrackIt_Team AS SELECT * FROM read_csv_auto('{TOOL_ORG_TrackIt_Team}', sep='|', ignore_errors=True)"
        )
        print(f"Reading {TOOL_ORG_TrackIt_Users} into VC_DTL_FLT_TBL")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS TOOL_ORG_TrackIt_Users AS SELECT * FROM read_csv_auto('{TOOL_ORG_TrackIt_Users}', sep='|', ignore_errors=True)"
        )
        print(f"Reading {OPER_BI_ServiceCenterClosedAHTLKUP} into VC_DTL_FLT_TBL")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS OPER_BI_ServiceCenterClosedAHTLKUP AS SELECT * FROM read_csv_auto('{OPER_BI_ServiceCenterClosedAHTLKUP}', sep='|', ignore_errors=True)"
        )
        print(f"Reading {TOOL_ORG_TrackIt_VolumeCount} into VC_DTL_FLT_TBL")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS TOOL_ORG_TrackIt_VolumeCount AS SELECT * FROM read_csv_auto('{TOOL_ORG_TrackIt_VolumeCount}', sep='|', ignore_errors=True)"
        )
    except duckdb.ParserException as e:
        print(f"ParserException: {e}")
        raise e
    except Exception as e:
        print(f"Exception: {e}")
        raise e
 
read_file(path,ENT_ORG_PersonLKUP)

# Write and execute your query
# query = """describe ENT_ORG_PersonTICIDHistoryLKUP"""

query = """select 
'TrackIT' as DATASOURCECD,
oft.DATASOURCECD || oft.CLOSEDAHTID as TrackITKey,
oft.DATASOURCECD || oft.CLOSEDAHTID || trim(oft.SUBTASKDESC) as OracleKey, 
PRODUCTIONDT 
,PROCSSR_TIC_ID
,WEEKENDINGDT
,COMPLETEDYEAR
,COMPLETEDMONTH
,PRODUCTIONQUARTER
,MONTHINDEX
,QUARTER
,PRODUCTIONMONTHNM
,SUBTASKDESC
,oft.FUNCTIONNM
,SUBJECTNM
,oft.CLOSEDAHTID
,COMPLETEDCNT
,MANAGERTICID
,MANAGERNM
,PROCESSORNM
,PROCESSORRESPCD
,RESPCDTYPE
,PROCESSORLOCATIONCD
,PROCESSORSTATUSCD
,PROCESSOROFFICENM
,PRIMARYTEAM from(
with oracle_final as(    
with oracle_join as(
SELECT
    dbw.PRD_DT AS PRODUCTIONDT,
    strftime(
        strptime(
            SUBSTR(ACTV_END_TS, 1, 9) || ' ' || SUBSTR(ACTV_END_TS, 10, 8),
            '%d-%b-%y %I.%M.%S'
        ),
        '%Y-%m-%d %H:%M:%S'
    ) AS  Weekendingdt,
    CASE
        WHEN dbw.PROCSSR_TIC_ID = 'UNKNOWN   ' THEN 'N' || 'UNKNOWN'
        ELSE 'N' || dbw.PROCSSR_TIC_ID
    END AS PROCSSR_TIC_ID,
 
    SUBSTR(PRD_DT, 1, 4) AS COMPLETEDYEAR,
    SUBSTR(PRD_DT, 6, 2) AS COMPLETEDMONTH,
    CASE
        WHEN SUBSTR(PRD_DT, 6, 2) IN ('01', '02', '03') THEN CONCAT('Q1 ', SUBSTR(PRD_DT, 3, 2))
        WHEN SUBSTR(PRD_DT, 6, 2) IN ('04', '05', '06') THEN CONCAT('Q2 ', SUBSTR(PRD_DT, 3, 2))
        WHEN SUBSTR(PRD_DT, 6, 2) IN ('07', '08', '09') THEN CONCAT('Q3 ', SUBSTR(PRD_DT, 3, 2))
        WHEN SUBSTR(PRD_DT, 6, 2) IN ('10', '11', '12') THEN CONCAT('Q4 ', SUBSTR(PRD_DT, 3, 2))
        ELSE 'NULL'
    END AS PRODUCTIONQUARTER,
    ((CAST(SUBSTR(PRD_DT, 1, 4) AS INTEGER) * 12) + CAST(SUBSTR(PRD_DT, 6, 2) AS INTEGER)) AS MONTHINDEX,
    CASE
        WHEN SUBSTR(PRD_DT, 6, 2) IN ('01', '02', '03') THEN 'Q1'
        WHEN SUBSTR(PRD_DT, 6, 2) IN ('04', '05', '06') THEN 'Q2'
        WHEN SUBSTR(PRD_DT, 6, 2) IN ('07', '08', '09') THEN 'Q3'
        WHEN SUBSTR(PRD_DT, 6, 2) IN ('10', '11', '12') THEN 'Q4'
    END AS QUARTER,
    CASE
        WHEN SUBSTR(PRD_DT, 6, 2) = '01' THEN 'JAN'
        WHEN SUBSTR(PRD_DT, 6, 2) = '02' THEN 'FEB'
        WHEN SUBSTR(PRD_DT, 6, 2) = '03' THEN 'MAR'
        WHEN SUBSTR(PRD_DT, 6, 2) = '04' THEN 'APR'
        WHEN SUBSTR(PRD_DT, 6, 2) = '05' THEN 'MAY'
        WHEN SUBSTR(PRD_DT, 6, 2) = '06' THEN 'JUN'
        WHEN SUBSTR(PRD_DT, 6, 2) = '07' THEN 'JUL'
        WHEN SUBSTR(PRD_DT, 6, 2) = '08' THEN 'AUG'
        WHEN SUBSTR(PRD_DT, 6, 2) = '09' THEN 'SEP'
        WHEN SUBSTR(PRD_DT, 6, 2) = '10' THEN 'OCT'
        WHEN SUBSTR(PRD_DT, 6, 2) = '11' THEN 'NOV'
        WHEN SUBSTR(PRD_DT, 6, 2) = '12' THEN 'DEC'
        ELSE 'NULL'
    END AS PRODUCTIONMONTHNM,
        Case
        When dbw.PROC_DESC='P3U DB UW stand-alone umbrella policy' and dbw.SRC_SYS_NM='CSW' then 'P3U DB stand-alone umbrella policy'
        When dbw.PROC_DESC='Workers Comp Check Exceptions' and dbw.SRC_SYS_NM='OLD' then 'OLD Exception Processing'
        When dbw.PROC_DESC='Pay/Unpay' and dbw.SRC_SYS_NM='WR' then 'Work Router - Pay/Unpay'
        When dbw.PROC_DESC='Refund' and dbw.SRC_SYS_NM='WR' then 'Work Router - Refund'
        When dbw.PROC_DESC='AUDIT / AUDIT EXCEPTIONS' and dbw.SRC_SYS_NM='DBW' then 'Audit/Audit Exceptions'
        When dbw.PROC_DESC='BANKRUPTCY' and dbw.SRC_SYS_NM='DBW' then 'Bankruptcy'
        When dbw.PROC_DESC='CANCEL SUB-AUDIT' and dbw.SRC_SYS_NM='DBW' then 'Cancel Subject to Audit'
        When dbw.PROC_DESC='CANCELLATION REQUEST' and dbw.SRC_SYS_NM='DBW' then 'Cancellation Request'
        When dbw.PROC_DESC='CASH REQUEST / RESEARCH' and dbw.SRC_SYS_NM='DBW' then 'Cash request/research'
        When dbw.PROC_DESC='CHECK IN THE MAIL' and dbw.SRC_SYS_NM='DBW' then 'check in the mail'
        When dbw.PROC_DESC='CONSUMER AFFAIRS' and dbw.SRC_SYS_NM='DBW' then 'Consumer Affairs'
        When dbw.PROC_DESC='DELINQUENCY' and dbw.SRC_SYS_NM='DBW' then 'Deliquency'
        When dbw.PROC_DESC='EARNED PREMIUM' and dbw.SRC_SYS_NM='DBW' then 'Earned Premium'
        When dbw.PROC_DESC='EARNED PREMIUM - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Earned Premium - Manual'
        When dbw.PROC_DESC='ESCALATION - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Escalation - Manual'
        When dbw.PROC_DESC='ESTIMATED AUDIT' and dbw.SRC_SYS_NM='DBW' then 'Estimated Audit'
        When dbw.PROC_DESC='ESTIMATED AUDIT - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Estimated Audit Manual'
        When dbw.PROC_DESC='FINAL AUDIT' and dbw.SRC_SYS_NM='DBW' then 'Final Audit'
        When dbw.PROC_DESC='FINAL AUDIT - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Final Audit - Manual'
        When dbw.PROC_DESC='FIRST INSTALLMENT' and dbw.SRC_SYS_NM='DBW' then 'First Installment'
        When dbw.PROC_DESC='FLAT CANCELLATION' and dbw.SRC_SYS_NM='DBW' then 'Flat Cancellation'
        When dbw.PROC_DESC='FLAT CANCELLATION - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Flat Cancel - manual'
        When dbw.PROC_DESC='FREE-FORM' and dbw.SRC_SYS_NM='DBW' then 'Free form'
        When dbw.PROC_DESC='MANUAL DNOC' and dbw.SRC_SYS_NM='DBW' then 'Manual DNOC'
        When dbw.PROC_DESC='MERGE ACCOUNTS / OFF ACCOUNT' and dbw.SRC_SYS_NM='DBW' then 'Merge Accounts / Off Account'
        When dbw.PROC_DESC='NON-PAY CANCELLATION' and dbw.SRC_SYS_NM='DBW' then 'Non-Pay Cancellation'
        When dbw.PROC_DESC='NON-PAY CANCELLATION - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Non-Pay Cancellation Manual'
        When dbw.PROC_DESC='NSF PAYMENT' and dbw.SRC_SYS_NM='DBW' then 'NSF Payment'
        When dbw.PROC_DESC='NSF PAYMENT - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'NSF Payment - Manual'
        When dbw.PROC_DESC='OCEAN MARINE' and dbw.SRC_SYS_NM='DBW' then 'Ocean Marine'
        When dbw.PROC_DESC='OUT OF BALANCE BILL' and dbw.SRC_SYS_NM='DBW' then 'Out of balance bill'
        When dbw.PROC_DESC='PAY BY PHONE' and dbw.SRC_SYS_NM='DBW' then 'Pay by Phone'
        When dbw.PROC_DESC='PAY DEAL' and dbw.SRC_SYS_NM='DBW' then 'Pay Deal'
        When dbw.PROC_DESC='PRE-BILL COURTESY CALL' and dbw.SRC_SYS_NM='DBW' then 'Pre Bill Courtsey'
        When dbw.PROC_DESC='PRE-BILL COURTESY CALL - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Pre Bill Courtsey manual'
        When dbw.PROC_DESC='PREMIUM TOLERANCE' and dbw.SRC_SYS_NM='DBW' then 'Premium Tolerance'
        When dbw.PROC_DESC='REFUND / REFUND PENDING' and dbw.SRC_SYS_NM='DBW' then 'Refund / Refund Pending'
        When dbw.PROC_DESC='REINSTATEMENT' and dbw.SRC_SYS_NM='DBW' then 'Reinstatement'
        When dbw.PROC_DESC='REMAINING INSTALLMENT' and dbw.SRC_SYS_NM='DBW' then 'Remaining Installment'
        When dbw.PROC_DESC='REMAINING INSTALLMENT - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Remaining Installment Manual'
        When dbw.PROC_DESC='SUSPEND MONITORING' and dbw.SRC_SYS_NM='DBW' then 'Suspend Monitoring'
        When dbw.PROC_DESC='TRAVPAY AUDIT' and dbw.SRC_SYS_NM='DBW' then 'TravPay Audit'
        When dbw.PROC_DESC='TRAVPAY AUDIT - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Travpay Audit - Manual'
        When dbw.PROC_DESC='TRAVPAY CANCELLATION' and dbw.SRC_SYS_NM='DBW' then 'TravPay Cancellation'
        When dbw.PROC_DESC='TRAVPAY CANCELLATION - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Travpay Cancellation - Manual'
        When dbw.PROC_DESC='TRAVPAY COURTESY CALL' and dbw.SRC_SYS_NM='DBW' then 'TravPay Courtesy Call'
        When dbw.PROC_DESC='TRAVPAY COURTESY CALL - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'TravPay Courtesy Call Manual'
        When dbw.PROC_DESC='TRAVPAY PAYMENT OUTSIDE INSURELINX - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'TravPay Payment Outside InsureLinx - Manual Total'
        When dbw.PROC_DESC='UN-HONORED PAYMENT' and dbw.SRC_SYS_NM='DBW' then 'Un-Honored Payment'
        When dbw.PROC_DESC='TRAVPAY UNHONORED' and dbw.SRC_SYS_NM='DBW' then 'TravPay Unhonored'  
        Else dbw.PROC_DESC end AS SUBTASKDESC,
    dbw.SRC_SYS_NM AS DATASOURCECD,
    CASE
        WHEN dbw.FUNC_NM = 'Compliance Support' THEN 'Billing Support'
        ELSE dbw.FUNC_NM
    END AS FUNCTIONNM,
    'VOLUME' AS SUBJECTNM,
    'ORACLE ID' AS CLOSEDAHTID,
    SUM(CMPLT_CNT) AS COMPLETEDCNT
FROM
    vc_dtl_flt_tbl dbw
WHERE
    SRC_SYS_NM IN ('DBW', 'CSW', 'WR', 'SHRPT', 'OLD', 'ABS')
    AND PROCSSR_TIC_ID IS NOT NULL
    -- AND DATE_ADD('month', -24, CURRENT_DATE)
GROUP BY
    dbw.PRD_DT,
    dbw.PROCSSR_TIC_ID,
    strftime(
        strptime(
            SUBSTR(ACTV_END_TS, 1, 9) || ' ' || SUBSTR(ACTV_END_TS, 10, 8),
            '%d-%b-%y %I.%M.%S'
        ),
        '%Y-%m-%d %H:%M:%S'
    ),
    SUBSTR(PRD_DT, 1, 4),
    SUBSTR(PRD_DT, 6, 2),
    dbw.PROC_DESC,
    dbw.SRC_SYS_NM,
    dbw.FUNC_NM,
    'VOLUME',
    'ORACLE ID'
 
union all
 
SELECT
    dbw.PRD_DT AS PRODUCTIONDT,
    strftime(
        strptime(
            SUBSTR(ACTV_END_TS, 1, 9) || ' ' || SUBSTR(ACTV_END_TS, 10, 8),
            '%d-%b-%y %I.%M.%S'
        ),
        '%Y-%m-%d %H:%M:%S'
    ) AS  Weekendingdt,
    SUBSTR(dbw.PRD_DT, 1, 4) AS COMPLETEDYEAR,
    SUBSTR(dbw.PRD_DT, 6, 2) AS COMPLETEDMONTH,
    CASE
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) IN ('01', '02', '03') THEN CONCAT('Q1 ', SUBSTR(dbw.PRD_DT, 3, 2))
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) IN ('04', '05', '06') THEN CONCAT('Q2 ', SUBSTR(dbw.PRD_DT, 3, 2))
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) IN ('07', '08', '09') THEN CONCAT('Q3 ', SUBSTR(dbw.PRD_DT, 3, 2))
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) IN ('10', '11', '12') THEN CONCAT('Q4 ', SUBSTR(dbw.PRD_DT, 3, 2))
        ELSE 'NULL'
    END AS PRODUCTIONQUARTER,
    (CAST(SUBSTR(dbw.PRD_DT, 1, 4) AS INTEGER) * 12) + CAST(SUBSTR(dbw.PRD_DT, 6, 2) AS INTEGER) AS MONTHINDEX,
    CASE
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) IN ('01', '02', '03') THEN 'Q1'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) IN ('04', '05', '06') THEN 'Q2'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) IN ('07', '08', '09') THEN 'Q3'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) IN ('10', '11', '12') THEN 'Q4'
    END AS QUARTER,
    CASE
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '01' THEN 'JAN'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '02' THEN 'FEB'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '03' THEN 'MAR'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '04' THEN 'APR'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '05' THEN 'MAY'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '06' THEN 'JUN'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '07' THEN 'JUL'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '08' THEN 'AUG'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '09' THEN 'SEP'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '10' THEN 'OCT'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '11' THEN 'NOV'
        WHEN SUBSTR(dbw.PRD_DT, 6, 2) = '12' THEN 'DEC'
        ELSE 'NULL'
    END AS PRODUCTIONMONTHNM,
    Case
        When dbw.PROC_DESC='P3U DB UW stand-alone umbrella policy' and dbw.SRC_SYS_NM='CSW' then 'P3U DB stand-alone umbrella policy'
        When dbw.PROC_DESC='Workers Comp Check Exceptions' and dbw.SRC_SYS_NM='OLD' then 'OLD Exception Processing'
        When dbw.PROC_DESC='Pay/Unpay' and dbw.SRC_SYS_NM='WR' then 'Work Router - Pay/Unpay'
        When dbw.PROC_DESC='Refund' and dbw.SRC_SYS_NM='WR' then 'Work Router - Refund'
        When dbw.PROC_DESC='AUDIT / AUDIT EXCEPTIONS' and dbw.SRC_SYS_NM='DBW' then 'Audit/Audit Exceptions'
        When dbw.PROC_DESC='BANKRUPTCY' and dbw.SRC_SYS_NM='DBW' then 'Bankruptcy'
        When dbw.PROC_DESC='CANCEL SUB-AUDIT' and dbw.SRC_SYS_NM='DBW' then 'Cancel Subject to Audit'
        When dbw.PROC_DESC='CANCELLATION REQUEST' and dbw.SRC_SYS_NM='DBW' then 'Cancellation Request'
        When dbw.PROC_DESC='CASH REQUEST / RESEARCH' and dbw.SRC_SYS_NM='DBW' then 'Cash request/research'
        When dbw.PROC_DESC='CHECK IN THE MAIL' and dbw.SRC_SYS_NM='DBW' then 'check in the mail'
        When dbw.PROC_DESC='CONSUMER AFFAIRS' and dbw.SRC_SYS_NM='DBW' then 'Consumer Affairs'
        When dbw.PROC_DESC='DELINQUENCY' and dbw.SRC_SYS_NM='DBW' then 'Deliquency'
        When dbw.PROC_DESC='EARNED PREMIUM' and dbw.SRC_SYS_NM='DBW' then 'Earned Premium'
        When dbw.PROC_DESC='EARNED PREMIUM - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Earned Premium - Manual'
        When dbw.PROC_DESC='ESCALATION - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Escalation - Manual'
        When dbw.PROC_DESC='ESTIMATED AUDIT' and dbw.SRC_SYS_NM='DBW' then 'Estimated Audit'
        When dbw.PROC_DESC='ESTIMATED AUDIT - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Estimated Audit Manual'
        When dbw.PROC_DESC='FINAL AUDIT' and dbw.SRC_SYS_NM='DBW' then 'Final Audit'
        When dbw.PROC_DESC='FINAL AUDIT - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Final Audit - Manual'
        When dbw.PROC_DESC='FIRST INSTALLMENT' and dbw.SRC_SYS_NM='DBW' then 'First Installment'
        When dbw.PROC_DESC='FLAT CANCELLATION' and dbw.SRC_SYS_NM='DBW' then 'Flat Cancellation'
        When dbw.PROC_DESC='FLAT CANCELLATION - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Flat Cancel - manual'
        When dbw.PROC_DESC='FREE-FORM' and dbw.SRC_SYS_NM='DBW' then 'Free form'
        When dbw.PROC_DESC='MANUAL DNOC' and dbw.SRC_SYS_NM='DBW' then 'Manual DNOC'
        When dbw.PROC_DESC='MERGE ACCOUNTS / OFF ACCOUNT' and dbw.SRC_SYS_NM='DBW' then 'Merge Accounts / Off Account'
        When dbw.PROC_DESC='NON-PAY CANCELLATION' and dbw.SRC_SYS_NM='DBW' then 'Non-Pay Cancellation'
        When dbw.PROC_DESC='NON-PAY CANCELLATION - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Non-Pay Cancellation Manual'
        When dbw.PROC_DESC='NSF PAYMENT' and dbw.SRC_SYS_NM='DBW' then 'NSF Payment'
        When dbw.PROC_DESC='NSF PAYMENT - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'NSF Payment - Manual'
        When dbw.PROC_DESC='OCEAN MARINE' and dbw.SRC_SYS_NM='DBW' then 'Ocean Marine'
        When dbw.PROC_DESC='OUT OF BALANCE BILL' and dbw.SRC_SYS_NM='DBW' then 'Out of balance bill'
        When dbw.PROC_DESC='PAY BY PHONE' and dbw.SRC_SYS_NM='DBW' then 'Pay by Phone'
        When dbw.PROC_DESC='PAY DEAL' and dbw.SRC_SYS_NM='DBW' then 'Pay Deal'
        When dbw.PROC_DESC='PRE-BILL COURTESY CALL' and dbw.SRC_SYS_NM='DBW' then 'Pre Bill Courtsey'
        When dbw.PROC_DESC='PRE-BILL COURTESY CALL - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Pre Bill Courtsey manual'
        When dbw.PROC_DESC='PREMIUM TOLERANCE' and dbw.SRC_SYS_NM='DBW' then 'Premium Tolerance'
        When dbw.PROC_DESC='REFUND / REFUND PENDING' and dbw.SRC_SYS_NM='DBW' then 'Refund / Refund Pending'
        When dbw.PROC_DESC='REINSTATEMENT' and dbw.SRC_SYS_NM='DBW' then 'Reinstatement'
        When dbw.PROC_DESC='REMAINING INSTALLMENT' and dbw.SRC_SYS_NM='DBW' then 'Remaining Installment'
        When dbw.PROC_DESC='REMAINING INSTALLMENT - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Remaining Installment Manual'
        When dbw.PROC_DESC='SUSPEND MONITORING' and dbw.SRC_SYS_NM='DBW' then 'Suspend Monitoring'
        When dbw.PROC_DESC='TRAVPAY AUDIT' and dbw.SRC_SYS_NM='DBW' then 'TravPay Audit'
        When dbw.PROC_DESC='TRAVPAY AUDIT - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Travpay Audit - Manual'
        When dbw.PROC_DESC='TRAVPAY CANCELLATION' and dbw.SRC_SYS_NM='DBW' then 'TravPay Cancellation'
        When dbw.PROC_DESC='TRAVPAY CANCELLATION - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'Travpay Cancellation - Manual'
        When dbw.PROC_DESC='TRAVPAY COURTESY CALL' and dbw.SRC_SYS_NM='DBW' then 'TravPay Courtesy Call'
        When dbw.PROC_DESC='TRAVPAY COURTESY CALL - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'TravPay Courtesy Call Manual'
        When dbw.PROC_DESC='TRAVPAY PAYMENT OUTSIDE INSURELINX - MANUAL' and dbw.SRC_SYS_NM='DBW' then 'TravPay Payment Outside InsureLinx - Manual Total'
        When dbw.PROC_DESC='UN-HONORED PAYMENT' and dbw.SRC_SYS_NM='DBW' then 'Un-Honored Payment'
        When dbw.PROC_DESC='TRAVPAY UNHONORED' and dbw.SRC_SYS_NM='DBW' then 'TravPay Unhonored'
    ELSE dbw.PROC_DESC
    END AS SUBTASKDESC,
    dbw.SRC_SYS_NM AS DATASOURCECD,
    CASE
        WHEN dbw.FUNC_NM = 'Compliance Support' THEN 'Billing Support'
        ELSE dbw.FUNC_NM
    END AS FUNCTIONNM,
    'VOLUME' AS SUBJECTNM,
    'ORACLE ID' AS CLOSEDAHTID,
    SUM(dbw.CMPLT_CNT) AS COMPLETEDCNT,
    'N' || ns.TICID AS PROCSSR_TIC_ID
FROM VC_DTL_FLT_TBL dbw
LEFT JOIN ENT_ORG_PersonLKUP ns
ON dbw.PROCSSR_NM = ns.RAWNAME
WHERE dbw.SRC_SYS_NM IN ('DBW','CSW','WR','SHRPT','OLD','ABS')
AND dbw.PROCSSR_TIC_ID IS NULL
GROUP BY
    dbw.PRD_DT,
    dbw.PROCSSR_NM,
    strftime(
        strptime(
            SUBSTR(ACTV_END_TS, 1, 9) || ' ' || SUBSTR(ACTV_END_TS, 10, 8),
            '%d-%b-%y %I.%M.%S'
        ),
        '%Y-%m-%d %H:%M:%S'
    ) ,
    SUBSTR(dbw.PRD_DT, 1, 4),
    SUBSTR(dbw.PRD_DT, 6, 2),
    dbw.PROC_DESC,
    dbw.SRC_SYS_NM,
    dbw.FUNC_NM,
    ns.RAWNAME,
    ns.TICID ,
    'VOLUME',
    'ORACLE ID'
)
SELECT 
    o.*,
    pthl.MANAGERTICID,
    pthl.MANAGERNM,
    DATASOURCECD || CLOSEDAHTID AS TrackITKey,
    pthl.PROCESSORNM,
    pthl.PROCESSORRESPCD,
    pthl.RESPCDTYPE,
    pthl.PROCESSORLOCATIONCD,
    pthl.PROCESSORSTATUSCD,
    pol.PROCESSOROFFICENM,
    tuser.PRIMARYTEAM
FROM 
    oracle_join o
LEFT JOIN 
    (
        SELECT 
            CONCAT('N', TICID) AS PROCSSR_TIC_ID,
            TICID, 
            ProductionMonthCd AS COMPLETEDMONTH, 
            ProductionYearCd AS COMPLETEDYEAR, 
            ManagerTICID AS MANAGERTICID, 
            ManagerNm AS MANAGERNM, 
            PersonNm AS PROCESSORNM, 
            RespCd AS PROCESSORRESPCD, 
            CASE 
                WHEN RespCd IN ('39014000', '39015000', '95850110') THEN 'Billing' 
                ELSE 'Non-Billing' 
            END AS RESPCDTYPE, 
            CONCAT('L ', LocationCd) AS PROCESSORLOCATIONCD, 
            StatusCd AS PROCESSORSTATUSCD
        FROM 
            ENT_ORG_PersonTICIDHistoryLKUP
    ) AS pthl 
ON 
    o.PROCSSR_TIC_ID = pthl.PROCSSR_TIC_ID
LEFT JOIN 
    (
        SELECT
            CONCAT('L ', LocationCd) AS PROCESSORLOCATIONCD,
            ProcessingOfficeNm AS PROCESSOROFFICENM
        FROM 
            ENT_ORG_ProcessingOfficeLKUP
    ) AS pol
ON 
    pthl.PROCESSORLOCATIONCD = pol.PROCESSORLOCATIONCD
LEFT JOIN 
    (
        SELECT
            'N' || u.EmployeeTICID AS PROCSSR_TIC_ID,
            CASE
                WHEN t.TeamNm IS NULL THEN 'Unknown'
                ELSE t.TeamNm
            END AS PRIMARYTEAM
        FROM
            TOOL_ORG_TrackIt_Users u
        LEFT JOIN
            TOOL_ORG_TrackIt_Team t
        ON
            u.TeamID = t.TeamID
            AND u.BusinessUnitID = t.BusinessUnitID
    ) AS tuser
ON 
    o.PROCSSR_TIC_ID = tuser.PROCSSR_TIC_ID
)
select * from oracle_final

union all

SELECT
  PROCSSR_TIC_ID,
  DATASOURCECD || CLOSEDAHTID AS TrackITKey,
  PROCESSORNM,
  PROCESSORRESPCD,
  RESPCDTYPE,
  PROCESSORLOCATIONCD,
  PROCESSORSTATUSCD,
  MANAGERTICID,
  MANAGERNM,
  PROCESSOROFFICENM,
  WEEKENDINGDT,
  PRIMARYTEAM,
  CASE WHEN FUNCTIONNM = 'Compliance Support' THEN 'Billing Support' ELSE FUNCTIONNM END AS FUNCTIONNM,
  SUBTASKDESC,
  DATASOURCECD,
  CLOSEDAHTID,
  COMPLETEDYEAR,
  COMPLETEDMONTH,
  PRODUCTIONQUARTER,
  MONTHINDEX,
  QUARTER,
  PRODUCTIONMONTHNM,
  PRODUCTIONDT,
  SUBJECTNM,
  SUM(COMPLETEDCNT) AS COMPLETEDCNT
FROM (
  SELECT
    'N' || tvc.EmployeeTICID AS PROCSSR_TIC_ID,
    plt.PersonNm AS PROCESSORNM,
    plt.RespCd AS PROCESSORRESPCD,
    CASE WHEN plt.RespCd IN ('39014000', '39015000', '95850110') THEN 'Billing' ELSE 'Non-Billing' END AS RESPCDTYPE,
    TLKUP.PRIMARYTEAM,
    'L ' || plt.LocationCd AS PROCESSORLOCATIONCD,
    plt.StatusCd AS PROCESSORSTATUSCD,
    plt.ManagerTICID AS MANAGERTICID,
    plt.ManagerNm AS MANAGERNM,
    pol.ProcessingOfficeNm AS PROCESSOROFFICENM,
    (CaptureDt + INTERVAL (7 - EXTRACT(DAYOFWEEK FROM CaptureDt)) DAY) AS WEEKENDINGDT,
    CASE WHEN CaptureDt BETWEEN dpl1.EffectiveDt AND dpl1.CutoffEffectiveDt THEN dpl1.TeamNm ELSE dpl2.TeamNm END AS TEAMNM,
    CASE WHEN CaptureDt BETWEEN dpl1.EffectiveDt AND dpl1.CutoffEffectiveDt THEN dpl1.FunctionNm ELSE dpl2.FunctionNm END AS FUNCTIONNM,
    CASE WHEN CaptureDt BETWEEN dpl1.EffectiveDt AND dpl1.CutoffEffectiveDt THEN dpl1.SubtaskNm ELSE dpl2.SubtaskNm END AS SUBTASKDESC,
    CASE WHEN CaptureDt BETWEEN dpl1.EffectiveDt AND dpl1.CutoffEffectiveDt THEN dpl1.DataSourceCd ELSE dpl2.DataSourceCd END AS DATASOURCECD,
    CASE WHEN CaptureDt BETWEEN dpl1.EffectiveDt AND dpl1.CutoffEffectiveDt THEN dpl1.ClosedAHTID ELSE dpl2.ClosedAHTID END AS CLOSEDAHTID,
    tvc.VolumeCnt AS COMPLETEDCNT,
    EXTRACT(YEAR FROM CaptureDt) AS COMPLETEDYEAR,
    EXTRACT(MONTH FROM CaptureDt) AS COMPLETEDMONTH,
    CASE
      WHEN EXTRACT(MONTH FROM CaptureDt) IN (1, 2, 3) THEN 'Q1 ' || RIGHT(CAST(EXTRACT(YEAR FROM CaptureDt) AS STRING), 2)
      WHEN EXTRACT(MONTH FROM CaptureDt) IN (4, 5, 6) THEN 'Q2 ' || RIGHT(CAST(EXTRACT(YEAR FROM CaptureDt) AS STRING), 2)
      WHEN EXTRACT(MONTH FROM CaptureDt) IN (7, 8, 9) THEN 'Q3 ' || RIGHT(CAST(EXTRACT(YEAR FROM CaptureDt) AS STRING), 2)
      WHEN EXTRACT(MONTH FROM CaptureDt) IN (10, 11, 12) THEN 'Q4 ' || RIGHT(CAST(EXTRACT(YEAR FROM CaptureDt) AS STRING), 2)
      ELSE 'NULL'
    END AS PRODUCTIONQUARTER,
    (EXTRACT(YEAR FROM CaptureDt) * 12) + EXTRACT(MONTH FROM CaptureDt) AS MONTHINDEX,
    CASE
      WHEN EXTRACT(MONTH FROM CaptureDt) IN (1, 2, 3) THEN 'Q1'
      WHEN EXTRACT(MONTH FROM CaptureDt) IN (4, 5, 6) THEN 'Q2'
      WHEN EXTRACT(MONTH FROM CaptureDt) IN (7, 8, 9) THEN 'Q3'
      WHEN EXTRACT(MONTH FROM CaptureDt) IN (10, 11, 12) THEN 'Q4'
    END AS QUARTER,
    CASE
      WHEN EXTRACT(MONTH FROM CaptureDt) = 1 THEN 'JAN'
      WHEN EXTRACT(MONTH FROM CaptureDt) = 2 THEN 'FEB'
      WHEN EXTRACT(MONTH FROM CaptureDt) = 3 THEN 'MAR'
      WHEN EXTRACT(MONTH FROM CaptureDt) = 4 THEN 'APR'
      WHEN EXTRACT(MONTH FROM CaptureDt) = 5 THEN 'MAY'
      WHEN EXTRACT(MONTH FROM CaptureDt) = 6 THEN 'JUN'
      WHEN EXTRACT(MONTH FROM CaptureDt) = 7 THEN 'JUL'
      WHEN EXTRACT(MONTH FROM CaptureDt) = 8 THEN 'AUG'
      WHEN EXTRACT(MONTH FROM CaptureDt) = 9 THEN 'SEP'
      WHEN EXTRACT(MONTH FROM CaptureDt) = 10 THEN 'OCT'
      WHEN EXTRACT(MONTH FROM CaptureDt) = 11 THEN 'NOV'
      WHEN EXTRACT(MONTH FROM CaptureDt) = 12 THEN 'DEC'
      ELSE 'NULL'
    END AS PRODUCTIONMONTHNM,
    CAST(EXTRACT(YEAR FROM tvc.CaptureDt) AS STRING) || '/' ||
    LPAD(CAST(EXTRACT(MONTH FROM tvc.CaptureDt) AS STRING), 2, '0') AS PRODUCTIONDT,
    'VOLUME' AS SUBJECTNM
  FROM TOOL_ORG_TrackIt_VolumeCount AS tvc
  LEFT JOIN ENT_ORG_PersonTICIDHistoryLKUP AS plt
    ON tvc.EmployeeTICID = plt.TICID AND
       EXTRACT(MONTH FROM tvc.CaptureDt) = plt.ProductionMonthCd AND
       EXTRACT(YEAR FROM tvc.CaptureDt) = plt.ProductionYearCd
  LEFT JOIN ENT_ORG_ProcessingOfficeLKUP AS pol
    ON plt.LocationCd = pol.LocationCd
  LEFT JOIN (
    SELECT
      'N' || u.EmployeeTICID AS PROCSSR_TIC_ID,
      COALESCE(t.TeamNm, 'Unknown') AS PRIMARYTEAM
    FROM TOOL_ORG_TrackIt_Users u
    LEFT JOIN TOOL_ORG_TrackIt_Team t
      ON u.TeamID = t.TeamID AND
         u.BusinessUnitID = t.BusinessUnitID
  ) TLKUP
    ON TLKUP.PROCSSR_TIC_ID = 'N' || tvc.EmployeeTICID
  LEFT JOIN OPER_BI_ServiceCenterClosedAHTLKUP AS dpl1
    ON dpl1.ClosedAHTID = tvc.PlatformID AND
       CaptureDt BETWEEN dpl1.EffectiveDt AND dpl1.CutoffEffectiveDt
  LEFT JOIN OPER_BI_ServiceCenterClosedAHTLKUP AS dpl2
    ON dpl2.ClosedAHTID = tvc.PlatformID AND
       CaptureDt NOT BETWEEN dpl2.EffectiveDt AND dpl2.CutoffEffectiveDt
  WHERE
    (CASE
      WHEN CaptureDt BETWEEN dpl1.EffectiveDt AND dpl1.CutoffEffectiveDt THEN dpl1.DataSourceCd
      ELSE dpl2.DataSourceCd
    END) = 'BCT' AND
    (CASE
      WHEN CaptureDt BETWEEN dpl1.EffectiveDt AND dpl1.CutoffEffectiveDt THEN dpl1.ServiceCenterNm
      ELSE dpl2.ServiceCenterNm
    END) LIKE 'Billing%' AND
    CaptureDt >= CURRENT_DATE - INTERVAL '24 months'
) FIN
GROUP BY
  PROCSSR_TIC_ID,
  PROCESSORNM,
  PROCESSORRESPCD,
  RESPCDTYPE,
  PROCESSORLOCATIONCD,
  PROCESSORSTATUSCD,
  MANAGERTICID,
  MANAGERNM,
  PROCESSOROFFICENM,
  WEEKENDINGDT,
  TEAMNM,
  PRIMARYTEAM,
  FUNCTIONNM,
  SUBTASKDESC,
  DATASOURCECD,
  CLOSEDAHTID,
  COMPLETEDYEAR,
  COMPLETEDMONTH,
  PRODUCTIONQUARTER,
  MONTHINDEX,
  QUARTER,
  PRODUCTIONMONTHNM,
  PRODUCTIONDT,
  SUBJECTNM
ORDER BY PRODUCTIONDT) as oft

left join (SELECT DISTINCT 
    'TrackIT' || CLOSEDAHTID as TrackITKey,
    ClosedAHTID AS CLOSEDAHTID, 
    DataSourceCd AS DATASOURCECD, 
    AHTminutes, 
    SubtaskNm, 
    FunctionNm, 
    TeamNm
FROM 
    OPER_BI_ServiceCenterClosedAHTLKUP dpl1
WHERE 
    dpl1.ServiceCenterNm LIKE 'Billing%' 
    AND DataSourceCd = 'BCT' 
    AND CutoffEffectiveDt = TIMESTAMP '9999-12-31 00:00:00') as q
    on oft.TrackITKey = q.TrackITKey
    
left join (SELECT DISTINCT 
    SubtaskNm AS SUBTASKDESC, 
    DATASOURCECD || CLOSEDAHTID || trim(SUBTASKDESC) as OracleKey,
    'ORACLE ID' AS CLOSEDAHTID, 
    DataSourceCd AS DATASOURCECD,
    AHTminutes AS AHTminutes1, 
    FunctionNm
FROM 
    OPER_BI_ServiceCenterClosedAHTLKUP dpl1
WHERE 
    dpl1.ServiceCenterNm LIKE 'Billing%' 
    AND DataSourceCd IN ('CSW', 'DBW', 'ABS', 'OLD', 'SHRPT', 'WR') 
    AND closedAHTID <> '5536' 
    AND CutoffEffectiveDt = TIMESTAMP '9999-12-31 00:00:00') as w
    on  (oft.DATASOURCECD || oft.CLOSEDAHTID || trim(oft.SUBTASKDESC)) = w.OracleKey
"""

pd.set_option('display.max_columns', None)

result = conn.execute(query).fetchdf()
print(result.head(2))

# Print results
# for row in result:
#    print(row)
