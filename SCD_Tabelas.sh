#!/bin/bash
echo "******** Mudar o Diretorio para criar o SCHEMA HIVE"
cd "/Bismarck/Projetos/Projeto5/MetaDados"
pwd
echo 
DIR="/Bismarck/Projetos/Projeto5/MetaDados/metastore_db"

if [ -d $DIR ] ; then
	echo
	echo "*******************"
	echo "SCHEMA Hive Criado"
	echo "*******************"
	echo
else
	echo 
	echo "Sem SCHEMA com Dados Originais"
	echo 
	exit
fi
echo "******** Apaga dados Anteriores e Cria Pastas Necessárias"
echo hdfs dfs -mkdir /Bismarck
echo hdfs dfs -mkdir /Bismarck/Projetos
echo hdfs dfs -mkdir /Bismarck/Projetos/Projeto5
echo hdfs dfs -mkdir /Bismarck/Projetos/Projeto5/Import
echo hdfs dfs -mkdir /Bismarck/Projetos/Projeto5/Import/HDFS
echo hdfs dfs -mkdir /user
echo hdfs dfs -mkdir /user/hive
echo hdfs dfs -mkdir /user/hive/warehouse

echo hdfs dfs -rm -r /Bismarck/Projetos/Projeto5/Import/HDFS/*
echo set -e
echo
echo "******** Importa os dados Com SQOOP para o HIVE e HBASE Tabela: ADDRESS ********"
echo
sqoop import -m 1 --connect jdbc:mysql://localhost:3306/adventureworks --table address --username root --password Bism@rck1 --fields-terminated-by "," --hive-import --hive-overwrite --hive-database adventure --columns "AddressID, AddressLine1, AddressLine2, City, StateProvinceID, PostalCode, ModifiedDate" --target-dir /Bismarck/Projetos/Projeto5/Import/HDFS/address00
echo
echo "******* Fim de Importação !!!!! ********"
echo
echo "******* Preparando as Tabelas para SCDs ********"
echo
hive -f /Bismarck/Projetos/Projeto5/HQL/SCD.hql
echo
echo "******* Fim da Preparação dos SCDs ********"
