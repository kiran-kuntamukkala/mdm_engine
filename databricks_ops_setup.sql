-- Run once as a Databricks catalog administrator.
-- Replace the principal below with the user or group used by the Streamlit app.

CREATE SCHEMA IF NOT EXISTS mdm.ops;

CREATE TABLE IF NOT EXISTS mdm.ops.mdm_run_summary (
  run_id STRING,
  run_timestamp STRING,
  catalog_name STRING,
  source_schema STRING,
  selected_tables STRING,
  ambiguity_columns STRING,
  final_columns STRING,
  temp_output_table STRING,
  final_output_table STRING
);

CREATE TABLE IF NOT EXISTS mdm.ops.mdm_run_actions (
  run_id STRING,
  run_timestamp STRING,
  action_type STRING,
  table_name STRING,
  table_priority INT,
  source_column STRING,
  source_data_type STRING,
  logical_field STRING,
  selected_as_ambiguity BOOLEAN,
  final_column STRING
);

-- Example grants. Replace `mdm_app_users` with the real principal.
-- GRANT USE CATALOG ON CATALOG mdm TO `mdm_app_users`;
-- GRANT USE SCHEMA ON SCHEMA mdm.ops TO `mdm_app_users`;
-- GRANT MODIFY ON TABLE mdm.ops.mdm_run_summary TO `mdm_app_users`;
-- GRANT MODIFY ON TABLE mdm.ops.mdm_run_actions TO `mdm_app_users`;