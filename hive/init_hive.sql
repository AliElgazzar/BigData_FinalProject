CREATE DATABASE IF NOT EXISTS bigdata_project
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/bigdata_project.db';

USE bigdata_project;

CREATE TABLE IF NOT EXISTS wikimedia_enriched_events (
  id BIGINT,
  type STRING,
  title STRING,
  namespace BIGINT,
  editor_user STRING,
  bot BOOLEAN,
  server_name STRING,
  wiki STRING,
  event_time TIMESTAMP,
  comment STRING,
  language STRING,
  project_type STRING,
  region STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/bigdata_project.db/wikimedia_enriched_events'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

CREATE TABLE IF NOT EXISTS wikimedia_window_summary (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  server_name STRING,
  language STRING,
  project_type STRING,
  change_count BIGINT,
  processed_time TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/bigdata_project.db/wikimedia_window_summary'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

CREATE TABLE IF NOT EXISTS wikimedia_bot_summary (
  server_name STRING,
  language STRING,
  project_type STRING,
  bot_label STRING,
  event_count BIGINT,
  processed_time TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/bigdata_project.db/wikimedia_bot_summary'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');
