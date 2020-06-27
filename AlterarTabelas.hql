SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency=true;
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=1;

use adventure;

-- Convert a Tabela em ORC 
create table address_SCD like address stored as orcfile;

-- Converte em Transacional 
ALTER TABLE address_SCD SET TBLPROPERTIES ('transactional'='true');

-- Inclui as Colunas  DataIni e DataFim

ALTER TABLE adventure.address_SCD ADD COLUMNS (DataIni date, DataFim date);

-- Alimenta a Tabela e a Data Inicio 
insert into address_SCD select *, current_date, cast(null as date) from address;

