
set hive.metastore.uris=thrift://tfbdaprnode04.carte.local:9083,thrift://tfbdaprnode05.carte.local:9083,thrift://tfbdaprnode06.carte.local:9083;
set hive.zookeeper.quorum=tfbdaprnode01.carte.local,tfbdaprnode02.carte.local,tfbdaprnode03.carte.local;
set hbase.zookeeper.quorum=tfbdaprnode01.carte.local,tfbdaprnode02.carte.local,tfbdaprnode03.carte.local;
set zookeeper.znode.parent=/hbase;



insert overwrite table enelpayprod.a_anom_enelpay
select
anom_sub_cod, 
case when trim(subAnomino) = '' or isnull(subAnomino) then 'ND' else trim(subAnomino) end anom_anomsub_cod
from enelpayprod.e_anom_enelpay a
lateral view explode(map_keys(a.anom_anomsub_cod)) b as subAnomino
where trim(anom_sub_cod) <> '' 
and anom_sub_cod is not null;