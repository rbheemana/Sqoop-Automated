set mapred.job.queue.name=${default_queuename};
set hive.merge.mapfiles=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=true;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.optimize.sort.dynamic.partition=true;
SET mapreduce.map.memory.mb=6144;
SET mapreduce.map.java.opts=-Xmx4096M;
SET mapreduce.reduce.memory.mb=6144;
SET mapreduce.reduce.java.opts=-Xmx4096M;
set mapred.job.queue.name=${default_queuename};
set hive.merge.mapfiles=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=true;
set hive.exec.max.dynamic.partitions=2000;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

INSERT OVERWRITE TABLE ${hv_db}.${hv_table}
  ${partition_clause}
SELECT  
  ${columns_without_partition}
-- ,${hdfs_load_ts}
  ${partition_column_select}
FROM ${hv_db_stage}.${stage_table};
 
--msck repair table ${hv_db}.${table};
