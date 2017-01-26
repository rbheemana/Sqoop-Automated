set mapred.job.queue.name=${default_queuename};
set hive.merge.mapfiles=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=true;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

INSERT INTO TABLE ${hv_db}.${hv_table}
  ${partition_clause}
SELECT  
  ${columns_without_partition}
-- ,${hdfs_load_ts}
 ,${partition_column_select}
FROM ${hv_db_stage}.${stage_table};
 
--msck repair table ${hv_db}.${table};
