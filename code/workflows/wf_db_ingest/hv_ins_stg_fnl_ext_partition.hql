set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=true;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

INSERT OVERWRITE TABLE ${hv_db}.${hv_table}
  ${partition_clause}
SELECT  
  ${columns_without_partition}
 ,${partition_column_select} as ${partition_column}
FROM ${hv_db_stage}.${stage_table}
union all
SELECT  
  ${columns_without_partition}
 ,${partition_column}
FROM ${hv_db}.${hv_table} a
left join (select distinct( ${where_column} ) as  ${where_column}_b
           from ${hv_db_stage}.${stage_table}) b
on   a.${where_column} = b.${where_column}_b
left join (select distinct(${partition_column_select})  as  ${partition_column}_c
           from ${hv_db_stage}.${stage_table}) c
on   a.${partition_column} = c.${partition_column}_c
where b.${where_column}_b is null
and   c.${partition_column}_c is not null;
 
--msck repair table ${hv_db}.${table};
