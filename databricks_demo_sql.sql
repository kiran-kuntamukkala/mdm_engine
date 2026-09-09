-- Databricks SQL script to create the demo Bronze tables and load test data
-- Run in a Databricks SQL warehouse or notebook using %sql

CREATE CATALOG IF NOT EXISTS mdm;
CREATE SCHEMA IF NOT EXISTS mdm.bronze;

-- Table 1: crm_customers
CREATE OR REPLACE TABLE mdm.bronze.crm_customers (
  record_id STRING,
  customer_name STRING,
  email_id STRING,
  mobile_no STRING,
  address STRING
);

INSERT INTO mdm.bronze.crm_customers VALUES
  ('CRM001', 'Robert Smith', 'robert@gmail.com', '+91-9876543210', '12 Main Street, New York, NY'),
  ('CRM002', 'Rob Smith', 'robert@gmail.com', '9876543210', '12 Main Street, New York, NY'),
  ('CRM003', 'Alice Johnson', 'alice.johnson@gmail.com', '+1-415-555-0111', '88 Park Avenue, San Francisco, CA'),
  ('CRM004', 'Robert Smyth', 'robert@gmail.com', '9876543210', '12 Main St, New York, NY'),
  ('CRM005', 'Neha Patel', 'neha.patel@gmail.com', '+91-9988776655', '44 Residency Road, Bengaluru, KA'),
  ('CRM006', 'Michael Chen', NULL, '+1-646-555-0142', '720 Broadway, New York, NY'),
  ('CRM007', 'Sara Williams', 'sara.williams@gmail.com', NULL, '19 Oak Lane, Austin, TX');

-- Table 2: banking_customers
CREATE OR REPLACE TABLE mdm.bronze.banking_customers (
  record_id STRING,
  cust_name STRING,
  mail STRING,
  phone STRING,
  addr STRING
);

INSERT INTO mdm.bronze.banking_customers VALUES
  ('BANK100', 'ROBERT SMITH', 'robert@gmail.com', '9876543210', '12 Main St, New York, NY'),
  ('BANK101', 'Robert Smtih', 'robert@ymail.com', '9876543210', '12 Main Street, New York, NY'),
  ('BANK102', 'Peter Jones', 'peter.jones@bank.com', '5551234567', '10 Market Street, Boston, MA'),
  ('BANK103', 'NEHA PATEL', 'neha.patel@gmail.com', '9988776655', '44 Residency Rd, Bengaluru, Karnataka'),
  ('BANK104', 'Michael Chen', 'michael.chen@bank.com', '+1-646-555-0142', '720 Broadway, New York, NY'),
  ('BANK105', 'Daniel Lee', 'daniel.lee@bank.com', '6175550188', NULL);

-- Table 3: creditcard_customers
CREATE OR REPLACE TABLE mdm.bronze.creditcard_customers (
  record_id STRING,
  customer_name STRING,
  email STRING,
  mobile STRING,
  street_address STRING
);

INSERT INTO mdm.bronze.creditcard_customers VALUES
  ('CC001', 'Robert Smith', 'robert.smith@gmail.com', '+91-9876543210', '12 Main Street, New York, NY'),
  ('CC002', 'Robert Smyth', 'robert@gmail.com', '9876543210', '12 Main St, New York, NY'),
  ('CC003', 'Maria Garcia', 'maria.garcia@gmail.com', '+1-202-555-0199', '500 Lake Shore Dr, Chicago, IL'),
  ('CC004', 'Neha Patel', 'neha.patel@card.com', '+91-9988776655', '44 Residency Road, Bengaluru, KA'),
  ('CC005', 'Sara Williams', 'sara.williams@gmail.com', '+1-512-555-0177', '19 Oak Lane, Austin, TX'),
  ('CC006', 'Olivia Brown', 'olivia.brown@gmail.com', '+1-312-555-0133', '230 W Monroe Street, Chicago, IL');

-- Optional: view the loaded source data
SELECT * FROM mdm.bronze.crm_customers;
SELECT * FROM mdm.bronze.banking_customers;
SELECT * FROM mdm.bronze.creditcard_customers;

-- Optional: quick validation of overlap patterns
SELECT 'crm_customers' AS source, COUNT(*) AS rows FROM mdm.bronze.crm_customers
UNION ALL
SELECT 'banking_customers', COUNT(*) FROM mdm.bronze.banking_customers
UNION ALL
SELECT 'creditcard_customers', COUNT(*) FROM mdm.bronze.creditcard_customers;
