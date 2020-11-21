
set hive.metastore.uris=thrift://tfbdaprnode04.carte.local:9083,thrift://tfbdaprnode05.carte.local:9083,thrift://tfbdaprnode06.carte.local:9083;
set hive.zookeeper.quorum=tfbdaprnode01.carte.local,tfbdaprnode02.carte.local,tfbdaprnode03.carte.local;
set hbase.zookeeper.quorum=tfbdaprnode01.carte.local,tfbdaprnode02.carte.local,tfbdaprnode03.carte.local;
set zookeeper.znode.parent=/hbase;


insert overwrite table enelpayprod.a_sub_enelpay
select
case when trim(sub_sub_cod) = '' or isnull(sub_sub_cod) then 'ND' else trim(sub_sub_cod) end sub_sub_cod, 
case when trim(sub_usercode_cod) = '' or isnull(sub_usercode_cod) then 'ND' else trim(sub_usercode_cod) end sub_usercode_cod, 
case when trim(sub_parentcode_cod) = '' or isnull(sub_parentcode_cod) then 'ND' else trim(sub_parentcode_cod) end sub_parentcode_cod, 
case when trim(sub_firstname_txt) = '' or isnull(sub_firstname_txt) then 'ND' else trim(sub_firstname_txt) end sub_firstname_txt,
case when trim(sub_secondname_txt) = '' or isnull(sub_secondname_txt) then 'ND' else trim(sub_secondname_txt) end sub_secondname_txt,
case when trim(sub_email_txt) = '' or isnull(sub_email_txt) then 'ND' else trim(sub_email_txt) end sub_email_txt,
case when trim(sub_username_txt) = '' or isnull(sub_username_txt) then 'ND' else trim(sub_username_txt) end sub_username_txt,
case when trim(sub_fiscalcode_txt) = '' or isnull(sub_fiscalcode_txt) then 'ND' else trim(sub_fiscalcode_txt) end sub_fiscalcode_txt,
case when trim(sub_originapp_txt) = '' or isnull(sub_originapp_txt) then 'ND' else trim(sub_originapp_txt) end sub_originapp_txt,
case when trim(sub_gender_txt) = '' or isnull(sub_gender_txt) then 'ND' else trim(sub_gender_txt) end sub_gender_txt,
from_unixtime(cast(substring(cast(sub_subscribe_ts as string),1,10) as bigint)),
from_unixtime(cast(substring(cast(sub_closing_ts as string),1,10) as bigint)),
sub_userstatus_i,
case when trim(sub_userinternalstatus_txt) = '' or isnull(sub_userinternalstatus_txt) then 'ND' else trim(sub_userinternalstatus_txt) end sub_userinternalstatus_txt,
case when trim(sub_que_txt) = '' or isnull(sub_que_txt) then 'ND' else trim(sub_que_txt) end sub_que_txt,
case when trim(sub_queid_txt) = '' or isnull(sub_queid_txt) then 'ND' else trim(sub_queid_txt) end sub_queid_txt,
case when trim(sub_ans_txt) = '' or isnull(sub_ans_txt) then 'ND' else trim(sub_ans_txt) end sub_ans_txt,
case when trim(sub_relauth_txt) = '' or isnull(sub_relauth_txt) then 'ND' else trim(sub_relauth_txt) end sub_relauth_txt,
case when trim(sub_country_txt) = '' or isnull(sub_country_txt) then 'ND' else trim(sub_country_txt) end sub_country_txt,
case when trim(sub_townofbirth_txt) = '' or isnull(sub_townofbirth_txt) then 'ND' else trim(sub_townofbirth_txt) end sub_townofbirth_txt,
sub_zipcode_i,
case when trim(sub_provinceofbirth_txt) = '' or isnull(sub_provinceofbirth_txt) then 'ND' else trim(sub_provinceofbirth_txt) end sub_provinceofbirth_txt,
case when trim(sub_pubkeys_txt) = '' or isnull(sub_pubkeys_txt) then 'ND' else trim(sub_pubkeys_txt) end sub_pubkeys_txt,
case when trim(sub_prodlist_txt) = '' or isnull(sub_prodlist_txt) then 'ND' else trim(sub_prodlist_txt) end sub_prodlist_txt,
case when trim(sub_instrlist_txt) = '' or isnull(sub_instrlist_txt) then 'ND' else trim(sub_instrlist_txt) end sub_instrlist_txt,
from_unixtime(cast(substring(cast(sub_docrel_ts as string),1,10) as bigint)),
from_unixtime(cast(substring(cast(sub_docexp_ts as string),1,10) as bigint)),
case when trim(sub_taxres_txt) = '' or isnull(sub_taxres_txt) then 'ND' else trim(sub_taxres_txt) end sub_taxres_txt,
case when trim(sub_tnxlist_txt) = '' or isnull(sub_tnxlist_txt) then 'ND' else trim(sub_tnxlist_txt) end sub_tnxlist_txt,
sub_codesharer_json
from enelpayprod.e_sub_enelpay
where sub_originapp_txt = 'Enel-Pay';



insert overwrite table enelpayprod.a_sub_act_enelpay
select * 
from enelpayprod.a_sub_enelpay where sub_sub_cod in 
(select distinct b.anom_sub_cod from enelpayprod.a_svapro_enelpay a 
join enelpayprod.a_anom_enelpay b on a.svapro_userid_txt = b.anom_anomsub_cod);
