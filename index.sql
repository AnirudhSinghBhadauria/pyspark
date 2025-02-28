----------View query--------


CREATE OR REPLACE FORCE EDITIONABLE VIEW "HUB_EQMS"."EQMS_WD_ARG_ACT_RIGHTGROUP" ("IAC_CODE", "ARG_ACT_ID", "ARG_ORG_CODE", "ARG_RGP_CODE", "MODIFIED_DATE", "MODIFIED_BY", "CREATED_DATE", "CREATED_BY", "EXCEP_SYNCH_ID", "TRANS_DATE", "TRANS_CREATED_DATE", "ERROR_CODE_LIST", "ERROR_CODE", "ERROR_INFO", "SYNCH_STATUS", "SYNCH_ID", "ORG_CODE") AS 
SELECT
 CAST(UPPER(Substr (EMAIL, 1,instr(EMAIL,'@') - 1)) AS NVARCHAR2(200)) AS IAC_CODE,
'TBD' AS ARG_ACT_ID,
'TBD' AS ARG_ORG_CODE,
'TRN_TRAINEE' AS ARG_RGP_CODE,
NULL AS MODIFIED_DATE,
'DATA_HUB_WD' AS  MODIFIED_BY,
NULL AS CREATED_DATE,
'DATA_HUB_WD' AS  CREATED_BY,
NULL AS EXCEP_SYNCH_ID,
NULL AS TRANS_DATE,
NULL AS TRANS_CREATED_DATE,
NULL AS ERROR_CODE_LIST,
'0' AS ERROR_CODE,
NULL AS ERROR_INFO,
'NEW' AS SYNCH_STATUS,
NULL AS SYNCH_ID,
'SMITH AND NEPHEW ' AS ORG_CODE
FROM  hub_eqms.stg_json_workday_emp wd_emp
join HUB_EQMS.gbip_ad_users ad_users on
upper(wd_emp.USER_ID)=upper(ad_users.sam_account_nm)
where email is not null and employee_status in ('Active','On Leave')
and ad_users.active_on_system_ind='Y' and upper(USER_ID) not in (select sam_account_nm from hub_eqms.EQMS_WD_EXCLUDE)
AND 1=1
UNION 
 SELECT
 CAST(UPPER(Substr (EMAIL, 1,instr(EMAIL,'@') - 1)) AS NVARCHAR2(200)) AS IAC_CODE,
'TBD' AS ARG_ACT_ID,
'TBD' AS ARG_ORG_CODE,
'TRN_PERSON_MANAGER' AS ARG_RGP_CODE,
NULL AS MODIFIED_DATE,
'DATA_HUB_WD' AS  MODIFIED_BY,
NULL AS CREATED_DATE,
'DATA_HUB_WD' AS  CREATED_BY,
NULL AS EXCEP_SYNCH_ID,
NULL AS TRANS_DATE,
NULL AS TRANS_CREATED_DATE,
NULL AS ERROR_CODE_LIST,
'0' AS ERROR_CODE,
NULL AS ERROR_INFO,
'NEW' AS SYNCH_STATUS,
NULL AS SYNCH_ID,
'SMITH AND NEPHEW ' AS ORG_CODE
FROM  hub_eqms.stg_json_workday_emp wd_emp
join HUB_EQMS.gbip_ad_users ad_users on
upper(wd_emp.USER_ID)=upper(ad_users.sam_account_nm)
where email is not null and employee_status in ('Active','On Leave')
and ad_users.active_on_system_ind='Y' and upper(USER_ID) not in (select sam_account_nm from hub_eqms.EQMS_WD_EXCLUDE)
AND MANAGEMENT_LEVEL IN ('2.1 M1','2.2 M2','3.1 M3','3.2 M4','4.1 M5');

UNION
  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HUB_EQMS"."EQMS_NED_ARG_ACT_RIGHTGROUP" ("IAC_CODE", "ARG_ACT_ID", "ARG_ORG_CODE", "ARG_RGP_CODE", "MODIFIED_DATE", "MODIFIED_BY", "CREATED_DATE", "CREATED_BY", "EXCEP_SYNCH_ID", "TRANS_DATE", "TRANS_CREATED_DATE", "ERROR_CODE_LIST", "ERROR_CODE", "ERROR_INFO", "SYNCH_STATUS", "SYNCH_ID", "ORG_CODE") AS 
  SELECT IAC_CODE, ARG_ACT_ID, ARG_ORG_CODE, ARG_RGP_CODE, MODIFIED_DATE, MODIFIED_BY, CREATED_DATE, CREATED_BY, EXCEP_SYNCH_ID, TRANS_DATE, TRANS_CREATED_DATE, ERROR_CODE_LIST, ERROR_CODE, ERROR_INFO, SYNCH_STATUS, SYNCH_ID, ORG_CODE
FROM (
SELECT 
cast(UPPER(Substr (NED.Email_Address, 1,instr(NED.Email_Address,'@') - 1)) as NVARCHAR2(200))   AS IAC_CODE,
'TBD' AS ARG_ACT_ID,
'TBD' AS ARG_ORG_CODE,
'TRN_TRAINEE' AS ARG_RGP_CODE,
NULL AS MODIFIED_DATE,
'DATA_HUB_ND' AS  MODIFIED_BY,
NULL AS CREATED_DATE,
'DATA_HUB_ND' AS  CREATED_BY,
NULL AS EXCEP_SYNCH_ID,
NULL AS TRANS_DATE,
NULL AS TRANS_CREATED_DATE,
NULL AS ERROR_CODE_LIST,
'0' AS ERROR_CODE,
NULL AS ERROR_INFO,
'NEW' AS SYNCH_STATUS,
NULL AS SYNCH_ID,
'SMITH AND NEPHEW ' AS ORG_CODE,
sam_account_nm
FROM (with Networkd_id as (
Select * from hub_eqms.stg_ned_emp ned 
join  HUB_EQMS.gbip_ad_users ad_users on 
UPPER(DECODE(ned.network_id,'ctr_gokakn','CG_GOKAKN','ctr_messmem1','CTR_MESSMM',ned.network_id))=upper(ad_users.sam_account_nm)
and ad_users.active_on_system_ind='Y' and ned.employee_status<>'T' and ned.network_id is not null
and upper(substr(ad_users.email_address, instr(ad_users.email_address, '@')+1))='SMITH-NEPHEW.COM' and NETWORK_ID<>'ctr_gandhr'
),
Networkd_id_email as (
select * from Networkd_id
union 
Select * from hub_eqms.stg_ned_emp ned 
join  HUB_EQMS.gbip_ad_users ad_users on 
upper(ned.EMAI_ADDRESS)=upper(ad_users.EMAIL_ADDRESS)
and ad_users.active_on_system_ind='Y' and ned.employee_status<>'T'
and  upper(substr(ad_users.email_address, instr(ad_users.email_address, '@')+1))='SMITH-NEPHEW.COM'
and  (upper(ad_users.EMAIL_ADDRESS)) not in (select upper(EMAIL_ADDRESS) from Networkd_id) and 
upper(ad_users.sam_account_nm) not in  (select upper(sam_account_nm) from Networkd_id)
)
select * from (
select * from Networkd_id_email
union 
Select * from hub_eqms.stg_ned_emp ned 
join  HUB_EQMS.gbip_ad_users ad_users on 
UPPER(DECODE(ned.USERNETWORKID,'ctr_gokakn','CG_GOKAKN','ctr_messmem1','CTR_MESSMM',decode(ned.USERNETWORKID,null,'',ned.USERNETWORKID)))=upper(ad_users.sam_account_nm)
and ad_users.active_on_system_ind='Y' and ned.employee_status<>'T' AND ned.network_id is not null 
and upper(substr(ad_users.email_address, instr(ad_users.email_address, '@')+1))='SMITH-NEPHEW.COM' and USERNETWORKID<>'ctr_gandhr'
and (upper(ad_users.EMAIL_ADDRESS)) not in (select upper(EMAIL_ADDRESS) from Networkd_id_email) and 
upper(ad_users.sam_account_nm) not in  (select upper(sam_account_nm) from Networkd_id_email)) ) NED)
WHERE  upper(IAC_CODE) not in (select sam_account_nm from hub_eqms.EQMS_NED_EXCLUDE) and 
upper(sam_account_nm) not in (select sam_account_nm from hub_eqms.EQMS_NED_EXCLUDE)
and IAC_CODE NOT IN (SELECT IAC_CODE FROM HUB_EQMS.EQMS_WD_ARG_ACT_RIGHTGROUP);



