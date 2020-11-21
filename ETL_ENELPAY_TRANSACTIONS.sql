
set hive.metastore.uris=thrift://tfbdaprnode04.carte.local:9083,thrift://tfbdaprnode05.carte.local:9083,thrift://tfbdaprnode06.carte.local:9083;
set hive.zookeeper.quorum=tfbdaprnode01.carte.local,tfbdaprnode02.carte.local,tfbdaprnode03.carte.local;
set hbase.zookeeper.quorum=tfbdaprnode01.carte.local,tfbdaprnode02.carte.local,tfbdaprnode03.carte.local;
set zookeeper.znode.parent=/hbase;



set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;




WITH VIEW AS (
SELECT 
COALESCE(MAX(tra_timestamp),cast('1970-01-01' as timestamp)) as max_tra_timestamp
FROM enelpayprod.a_tra_enelpay
)
INSERT OVERWRITE TABLE enelpayprod.i_tra_enelpay
SELECT
CASE WHEN tra_tra_cod='' OR ISNULL(tra_tra_cod) THEN 'ND' ELSE TRIM( tra_tra_cod) END tra_tra_cod,
tra_timestamp,
CASE WHEN tra_type_txt='' OR ISNULL(tra_type_txt) THEN 'ND' ELSE TRIM( tra_type_txt) END tra_type_txt,
tra_amount_imp,
tra_automaticrecharge_bol,
tra_suppliertax_imp,
tra_supplierfee_imp,
CASE WHEN tra_aposauthnumber_txt='' OR ISNULL(tra_aposauthnumber_txt) THEN 'ND' ELSE TRIM( tra_aposauthnumber_txt) END tra_aposauthnumber_txt,
CASE WHEN tra_apostnxid_txt='' OR ISNULL(tra_apostnxid_txt) THEN 'ND' ELSE TRIM( tra_apostnxid_txt) END tra_apostnxid_txt,
CASE WHEN tra_aposshopid_txt='' OR ISNULL(tra_aposshopid_txt) THEN 'ND' ELSE TRIM( tra_aposshopid_txt) END tra_aposshopid_txt,
CASE WHEN tra_originapp_txt='' OR ISNULL(tra_originapp_txt) THEN 'ND' ELSE TRIM( tra_originapp_txt) END tra_originapp_txt,
tra_status_txt,
CASE WHEN tra_instrtype_txt='' OR ISNULL(tra_instrtype_txt) THEN 'ND' ELSE TRIM( tra_instrtype_txt) END tra_instrtype_txt,
tra_instrid_txt,
CASE WHEN tra_instrinfo_txt='' OR ISNULL(tra_instrinfo_txt) THEN 'ND' ELSE TRIM( tra_instrinfo_txt) END tra_instrinfo_txt,
CASE WHEN tra_message_txt='' OR ISNULL(tra_message_txt) THEN 'ND' ELSE TRIM( tra_message_txt) END tra_message_txt,
CASE WHEN tra_authorotp_txt='' OR ISNULL(tra_authorotp_txt) THEN 'ND' ELSE TRIM( tra_authorotp_txt) END tra_authorotp_txt,
tra_authorotpattempts_int,
CASE WHEN tra_internibanfrom_txt='' OR ISNULL(tra_internibanfrom_txt) THEN 'ND' ELSE TRIM( tra_internibanfrom_txt) END tra_internibanfrom_txt,
CASE WHEN tra_interndescrfrom_txt='' OR ISNULL(tra_interndescrfrom_txt) THEN 'ND' ELSE TRIM( tra_interndescrfrom_txt) END tra_interndescrfrom_txt,
CASE WHEN tra_ndgcodefrom_txt='' OR ISNULL(tra_ndgcodefrom_txt) THEN 'ND' ELSE TRIM( tra_ndgcodefrom_txt) END tra_ndgcodefrom_txt,
CASE WHEN tra_internibanto_txt='' OR ISNULL(tra_internibanto_txt) THEN 'ND' ELSE TRIM( tra_internibanto_txt) END tra_internibanto_txt,
CASE WHEN tra_interndescriptionto_txt='' OR ISNULL(tra_interndescriptionto_txt) THEN 'ND' ELSE TRIM( tra_interndescriptionto_txt) END tra_interndescriptionto_txt,
CASE WHEN tra_ndgcodeto_txt='' OR ISNULL(tra_ndgcodeto_txt) THEN 'ND' ELSE TRIM( tra_ndgcodeto_txt) END tra_ndgcodeto_txt,
CASE WHEN tra_externibanfrom_txt='' OR ISNULL(tra_externibanfrom_txt) THEN 'ND' ELSE TRIM( tra_externibanfrom_txt) END tra_externibanfrom_txt,
CASE WHEN tra_externdescrfrom_txt='' OR ISNULL(tra_externdescrfrom_txt) THEN 'ND' ELSE TRIM( tra_externdescrfrom_txt) END tra_externdescrfrom_txt,
CASE WHEN tra_causal_txt='' OR ISNULL(tra_causal_txt) THEN 'ND' ELSE TRIM( tra_causal_txt) END tra_causal_txt,
CASE WHEN tra_externibanto_txt='' OR ISNULL(tra_externibanto_txt) THEN 'ND' ELSE TRIM( tra_externibanto_txt) END tra_externibanto_txt,
CASE WHEN tra_externdescrto_txt='' OR ISNULL(tra_externdescrto_txt) THEN 'ND' ELSE TRIM( tra_externdescrto_txt) END tra_externdescrto_txt,
CASE WHEN tra_consumer_cod='' OR ISNULL(tra_consumer_cod) THEN 'ND' ELSE TRIM( tra_consumer_cod) END tra_consumer_cod,
CASE WHEN tra_historylog_txt='' OR ISNULL(tra_historylog_txt) THEN 'ND' ELSE TRIM( tra_historylog_txt) END tra_historylog_txt,
from_unixtime(cast(substring(cast(tra_creationdate_ts as string),1,10) as bigint)),
from_unixtime(cast(substring(cast(tra_laststatusupdate_ts as string),1,10) as bigint)),
from_unixtime(cast(substring(cast(tra_otpexpdate_ts as string),1,10) as bigint)),
tra_authamount_imp,
CASE WHEN tra_curr_txt='' OR ISNULL(tra_curr_txt) THEN 'ND' ELSE TRIM( tra_curr_txt) END tra_curr_txt,
CASE WHEN tra_substatus_txt='' OR ISNULL(tra_substatus_txt) THEN 'ND' ELSE TRIM( tra_substatus_txt) END tra_substatus_txt,
CASE WHEN tra_sddverifstatus_txt='' OR ISNULL(tra_sddverifstatus_txt) THEN 'ND' ELSE TRIM( tra_sddverifstatus_txt) END tra_sddverifstatus_txt,
tra_signreq_bol,
tra_batchcount_int,
CASE WHEN tra_suppl_cod='' OR ISNULL(tra_suppl_cod) THEN 'ND' ELSE TRIM( tra_suppl_cod) END tra_suppl_cod,
CASE WHEN tra_suppldescr_txt='' OR ISNULL(tra_suppldescr_txt) THEN 'ND' ELSE TRIM( tra_suppldescr_txt) END tra_suppldescr_txt,
CASE WHEN tra_period_txt='' OR ISNULL(tra_period_txt) THEN 'ND' ELSE TRIM( tra_period_txt) END tra_period_txt,
CASE WHEN tra_invoicenum_txt='' OR ISNULL(tra_invoicenum_txt) THEN 'ND' ELSE TRIM( tra_invoicenum_txt) END tra_invoicenum_txt,
CASE WHEN tra_clientnum_txt='' OR ISNULL(tra_clientnum_txt) THEN 'ND' ELSE TRIM( tra_clientnum_txt) END tra_clientnum_txt,
CASE WHEN tra_expdate_txt='' OR ISNULL(tra_expdate_txt) THEN 'ND' ELSE TRIM( tra_expdate_txt) END tra_expdate_txt,
tra_supplconv_bol,
CASE WHEN tra_supplsae_txt='' OR ISNULL(tra_supplsae_txt) THEN 'ND' ELSE TRIM( tra_supplsae_txt) END tra_supplsae_txt,
CASE WHEN tra_supplisocountry_cod='' OR ISNULL(tra_supplisocountry_cod) THEN 'ND' ELSE TRIM( tra_supplisocountry_cod) END tra_supplisocountry_cod,
CASE WHEN tra_merch_cod='' OR ISNULL(tra_merch_cod) THEN 'ND' ELSE TRIM( tra_merch_cod) END tra_merch_cod,
CASE WHEN tra_merchdescr_txt='' OR ISNULL(tra_merchdescr_txt) THEN 'ND' ELSE TRIM( tra_merchdescr_txt) END tra_merchdescr_txt,
CASE WHEN tra_order_cod='' OR ISNULL(tra_order_cod) THEN 'ND' ELSE TRIM( tra_order_cod) END tra_order_cod,
CASE WHEN tra_undotrans_cod='' OR ISNULL(tra_undotrans_cod) THEN 'ND' ELSE TRIM( tra_undotrans_cod) END tra_undotrans_cod,
CASE WHEN tra_exptime_txt='' OR ISNULL(tra_exptime_txt) THEN 'ND' ELSE TRIM( tra_exptime_txt) END tra_exptime_txt,
CASE WHEN tra_callbackurl_txt='' OR ISNULL(tra_callbackurl_txt) THEN 'ND' ELSE TRIM( tra_callbackurl_txt) END tra_callbackurl_txt,
CASE WHEN tra_accountmode_txt='' OR ISNULL(tra_accountmode_txt) THEN 'ND' ELSE TRIM( tra_accountmode_txt) END tra_accountmode_txt,
CASE WHEN tra_mode_txt='' OR ISNULL(tra_mode_txt) THEN 'ND' ELSE TRIM( tra_mode_txt) END tra_mode_txt,
CASE WHEN tra_token_txt='' OR ISNULL(tra_token_txt) THEN 'ND' ELSE TRIM( tra_token_txt) END tra_token_txt,
CASE WHEN tra_internalreason_txt='' OR ISNULL(tra_internalreason_txt) THEN 'ND' ELSE TRIM( tra_internalreason_txt) END tra_internalreason_txt,
CASE WHEN tra_easyboxerr_cod='' OR ISNULL(tra_easyboxerr_cod) THEN 'ND' ELSE TRIM( tra_easyboxerr_cod) END tra_easyboxerr_cod,
CASE WHEN tra_numlog_txt='' OR ISNULL(tra_numlog_txt) THEN 'ND' ELSE TRIM( tra_numlog_txt) END tra_numlog_txt,
from_unixtime(cast(substring(cast(tra_canc_bol as string),1,10) as bigint)),
CASE WHEN tra_numrifannullo_txt='' OR ISNULL(tra_numrifannullo_txt) THEN 'ND' ELSE TRIM( tra_numrifannullo_txt) END tra_numrifannullo_txt,
from_unixtime(cast(substring(cast(tra_canceldate_ts as string),1,10) as bigint)),
CASE WHEN tra_user_cod='' OR ISNULL(tra_user_cod) THEN 'ND' ELSE TRIM( tra_user_cod) END tra_user_cod,
CASE WHEN tra_terminal_cod='' OR ISNULL(tra_terminal_cod) THEN 'ND' ELSE TRIM( tra_terminal_cod) END tra_terminal_cod,
CASE WHEN tra_origtransftype_txt='' OR ISNULL(tra_origtransftype_txt) THEN 'ND' ELSE TRIM( tra_origtransftype_txt) END tra_origtransftype_txt,
CASE WHEN tra_reservation_cod='' OR ISNULL(tra_reservation_cod) THEN 'ND' ELSE TRIM( tra_reservation_cod) END tra_reservation_cod,
from_unixtime(cast(substring(cast(tra_reservation_ts as string),1,10) as bigint)),
CASE WHEN tra_transftype_txt='' OR ISNULL(tra_transftype_txt) THEN 'ND' ELSE TRIM( tra_transftype_txt) END tra_transftype_txt,
CASE WHEN tra_casual_cod='' OR ISNULL(tra_casual_cod) THEN 'ND' ELSE TRIM( tra_casual_cod) END tra_casual_cod,
tra_transfnum_txt,
CASE WHEN tra_extreason_txt='' OR ISNULL(tra_extreason_txt) THEN 'ND' ELSE TRIM( tra_extreason_txt) END tra_extreason_txt,
CASE WHEN tra_extbankcountryto_txt='' OR ISNULL(tra_extbankcountryto_txt) THEN 'ND' ELSE TRIM( tra_extbankcountryto_txt) END tra_extbankcountryto_txt,
CASE WHEN tra_extopref_txt='' OR ISNULL(tra_extopref_txt) THEN 'ND' ELSE TRIM( tra_extopref_txt) END tra_extopref_txt,
CASE WHEN tra_panalias_txt='' OR ISNULL(tra_panalias_txt) THEN 'ND' ELSE TRIM( tra_panalias_txt) END tra_panalias_txt,
tra_aliasreq_bol,
CASE WHEN tra_aposrefop_cod='' OR ISNULL(tra_aposrefop_cod) THEN 'ND' ELSE TRIM( tra_aposrefop_cod) END tra_aposrefop_cod,
CASE WHEN tra_aposrefamount_txt='' OR ISNULL(tra_aposrefamount_txt) THEN 'ND' ELSE TRIM( tra_aposrefamount_txt) END tra_aposrefamount_txt,
CASE WHEN tra_aposerr_cod='' OR ISNULL(tra_aposerr_cod) THEN 'ND' ELSE TRIM( tra_aposerr_cod) END tra_aposerr_cod,
CASE WHEN tra_aposmerchtype_txt='' OR ISNULL(tra_aposmerchtype_txt) THEN 'ND' ELSE TRIM( tra_aposmerchtype_txt) END tra_aposmerchtype_txt,
CASE WHEN tra_aposinperson_txt='' OR ISNULL(tra_aposinperson_txt) THEN 'ND' ELSE TRIM( tra_aposinperson_txt) END tra_aposinperson_txt,
CASE WHEN tra_aposreqrefnum_txt='' OR ISNULL(tra_aposreqrefnum_txt) THEN 'ND' ELSE TRIM( tra_aposreqrefnum_txt) END tra_aposreqrefnum_txt,
CASE WHEN tra_p2pmobilephone_txt='' OR ISNULL(tra_p2pmobilephone_txt) THEN 'ND' ELSE TRIM( tra_p2pmobilephone_txt) END tra_p2pmobilephone_txt,
CASE WHEN tra_p2pmobilephonefrom_txt='' OR ISNULL(tra_p2pmobilephonefrom_txt) THEN 'ND' ELSE TRIM( tra_p2pmobilephonefrom_txt) END tra_p2pmobilephonefrom_txt,
CASE WHEN tra_p2ptext_txt='' OR ISNULL(tra_p2ptext_txt) THEN 'ND' ELSE TRIM( tra_p2ptext_txt) END tra_p2ptext_txt,
CASE WHEN tra_p2pcontactnameto_txt='' OR ISNULL(tra_p2pcontactnameto_txt) THEN 'ND' ELSE TRIM( tra_p2pcontactnameto_txt) END tra_p2pcontactnameto_txt,
CASE WHEN tra_p2pcontactnamefrom_txt='' OR ISNULL(tra_p2pcontactnamefrom_txt) THEN 'ND' ELSE TRIM( tra_p2pcontactnamefrom_txt) END tra_p2pcontactnamefrom_txt,
from_unixtime(cast(substring(cast(tra_externaccount_ts as string),1,10) as bigint)),
from_unixtime(cast(substring(cast(tra_externauth_ts as string),1,10) as bigint)),
from_unixtime(cast(substring(cast(tra_internaccount_ts as string),1,10) as bigint))
FROM enelpayprod.e_tra_enelpay e,view
where e.tra_timestamp > max_tra_timestamp
and e.tra_originapp_txt = 'Enel-Pay';




INSERT OVERWRITE TABLE enelpayprod.a_tra_enelpay PARTITION (tra_creationdate_int)
SELECT 
s.*,
cast(concat(substr(cast(tra_creationdate_ts as STRING),1,4),
substr(cast(tra_creationdate_ts as STRING),6,2),
substr(cast(tra_creationdate_ts as STRING),9,2)) as int)
FROM enelpayprod.i_tra_enelpay s
UNION ALL
SELECT 
t.* 
FROM enelpayprod.a_tra_enelpay t LEFT JOIN
enelpayprod.i_tra_enelpay s
on t.tra_tra_cod = s.tra_tra_cod
WHERE s.tra_tra_cod IS NULL AND t.tra_creationdate_int in (
SELECT DISTINCT 
cast(concat(substr(cast(tra_creationdate_ts as STRING),1,4),
substr(cast(tra_creationdate_ts as STRING),6,2),
substr(cast(tra_creationdate_ts as STRING),9,2)) as int)
FROM enelpayprod.i_tra_enelpay);


