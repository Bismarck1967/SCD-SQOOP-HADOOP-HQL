SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency=true;
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=2;
SET hive.auto.convert.join=false;
set hive.optimize.skewjoin=true;
set hive.optimize.skewjoin.compiletime=true;
set hive.groupby.skewindata=true;
set hive.skewjoin.key=100000;

-- Abre o Banco 

use adventure;

merge into address_scd
using (
  -- The base staging data.
 
 select
    address.addressid as join_key,
    address.* from address

  union all

  -- Generate an extra row for changed records.
  -- The null join_key means it will be inserted.

  select
    null, address.*
  from
    address join address_scd on address.addressid = address_scd.addressid
  where
    ( address.AddressLine1    <> address_scd.AddressLine1    or 
      address.AddressLine2    <> address_scd.AddressLine2    or
      address.City            <> address_scd.City            or
      address.StateProvinceID <> address_scd.StateProvinceID or
      address.PostalCode      <> address_scd.PostalCode      or
      address.ModifiedDate    <> address_scd.ModifiedDate)
    and address_scd.datafim is null
) sub

on sub.join_key = address_scd.addressid

when matched
  and sub.AddressLine1    <> address_scd.AddressLine1    or 
      sub.AddressLine2    <> address_scd.AddressLine2    or
      sub.City            <> address_scd.City            or
      sub.StateProvinceID <> address_scd.StateProvinceID or
      sub.PostalCode      <> address_scd.PostalCode      or
      sub.ModifiedDate    <> address_scd.ModifiedDate
  then update set datafim = current_date()
when not matched
  then insert values (sub.addressid, sub.AddressLine1, sub.AddressLine2, sub.City, sub.StateProvinceID, sub.PostalCode, sub.ModifiedDate, current_date(), null);