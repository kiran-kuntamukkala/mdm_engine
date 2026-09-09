-- Databricks MDM test data
-- Run this script in the catalog where the app is connected.
-- It creates/uses the bronze schema in the active catalog, so no CREATE CATALOG privilege is required.

CREATE SCHEMA IF NOT EXISTS bronze;
USE SCHEMA bronze;

-- Source 1: CRM records. Canonical-looking names with duplicate records.
CREATE OR REPLACE TABLE crm_customers_test (
  record_id STRING,
  customer_name STRING,
  email_id STRING,
  mobile_no STRING,
  address STRING
);

INSERT INTO crm_customers_test VALUES
  ('CRM001', 'Robert Smith', 'robert.smith@gmail.com', '+1-212-555-0101', '12 Main Street, New York, NY'),
  ('CRM002', 'Rob Smith', 'robert.smith@gmail.com', '2125550101', '12 Main St, New York, NY'),
  ('CRM003', 'Alice Johnson', 'alice.johnson@gmail.com', '+1-415-555-0111', '88 Park Avenue, San Francisco, CA'),
  ('CRM004', 'Alicia Johnson', NULL, '4155550111', '88 Park Ave, San Francisco, CA'),
  ('CRM005', 'Neha Patel', 'neha.patel@gmail.com', '+91-9988776655', '44 Residency Road, Bengaluru, KA'),
  ('CRM006', 'Michael Chen', NULL, '+1-646-555-0142', '720 Broadway, New York, NY'),
  ('CRM007', 'Sara Williams', 'sara.williams@gmail.com', NULL, '19 Oak Lane, Austin, TX'),
  ('CRM008', 'Daniel Lee', 'daniel.lee@gmail.com', '+1-617-555-0188', '5 Beacon Street, Boston, MA'),
  ('CRM009', 'Maria Garcia', 'maria.garcia@gmail.com', '+1-202-555-0199', '500 Lake Shore Drive, Chicago, IL'),
  ('CRM010', 'Olivia Brown', 'olivia.brown@gmail.com', '+1-312-555-0133', '230 W Monroe Street, Chicago, IL');

-- Source 2: Banking records. Deliberately different aliases for the same logical fields.
CREATE OR REPLACE TABLE banking_customers_test (
  record_id STRING,
  cust_name STRING,
  mail STRING,
  phone STRING,
  addr STRING
);

INSERT INTO banking_customers_test VALUES
  ('BANK001', 'ROBERT SMITH', 'robert.smith@gmail.com', '2125550101', '12 Main St, New York, NY'),
  ('BANK002', 'Robert Smtih', 'robert@ymail.com', '2125550101', '12 Main Street, New York, NY'),
  ('BANK003', 'Alice Johnson', 'alice.johnson@gmail.com', '4155550111', '88 Park Avenue, San Francisco, CA'),
  ('BANK004', 'Peter Jones', 'peter.jones@bank.com', '6175550122', '10 Market Street, Boston, MA'),
  ('BANK005', 'Neha Patel', 'neha.patel@gmail.com', '9988776655', '44 Residency Rd, Bengaluru, Karnataka'),
  ('BANK006', 'Michael Chen', 'michael.chen@bank.com', '6465550142', '720 Broadway, New York, NY'),
  ('BANK007', 'Sara Williams', 'sara.williams@gmail.com', '5125550177', '19 Oak Lane, Austin, TX'),
  ('BANK008', 'Daniel Lee', 'daniel.lee@bank.com', '6175550188', NULL),
  ('BANK009', 'Maria Garcia', 'maria.garcia@gmail.com', '2025550199', '500 Lake Shore Dr, Chicago, IL'),
  ('BANK010', 'Olivia Brown', NULL, '3125550133', '230 W Monroe St, Chicago, IL');

-- Source 3: Credit-card records. Similar names, alternate emails, and new customers.
CREATE OR REPLACE TABLE creditcard_customers_test (
  record_id STRING,
  full_name STRING,
  email STRING,
  mobile STRING,
  street_address STRING
);

INSERT INTO creditcard_customers_test VALUES
  ('CARD001', 'Robert Smyth', 'robert.smith@gmail.com', '+1 (212) 555-0101', '12 Main Street, New York, NY'),
  ('CARD002', 'Alice Jhnson', 'alice.johnson@gmail.com', '+1 (415) 555-0111', '88 Park Ave, San Francisco, CA'),
  ('CARD003', 'Neha Patel', 'neha.patel@card.com', '+91 9988776655', '44 Residency Road, Bengaluru, KA'),
  ('CARD004', 'Michael Chen', 'michael.chen@bank.com', '+1-646-555-0142', '720 Broadway, New York, NY'),
  ('CARD005', 'Sara Williams', 'sara.williams@gmail.com', '+1-512-555-0177', '19 Oak Lane, Austin, TX'),
  ('CARD006', 'Daniel Lee', 'daniel.lee@gmail.com', '+1-617-555-0188', '5 Beacon St, Boston, MA'),
  ('CARD007', 'Maria Garcia', 'maria.garcia@gmail.com', '+1-202-555-0199', '500 Lake Shore Dr, Chicago, IL'),
  ('CARD008', 'Olivia Brown', 'olivia.brown@gmail.com', '+1-312-555-0133', '230 W Monroe Street, Chicago, IL'),
  ('CARD009', 'James Wilson', 'james.wilson@gmail.com', '+1-206-555-0166', '701 Pine Street, Seattle, WA'),
  ('CARD010', 'Priya Nair', 'priya.nair@gmail.com', '+91-9876501234', '17 MG Road, Bengaluru, KA');

-- Source 4: Web leads. Sparse records test null handling and lower-quality identifiers.
CREATE OR REPLACE TABLE web_leads_test (
  id STRING,
  name STRING,
  work_email STRING,
  cell STRING,
  mailing_address STRING
);

INSERT INTO web_leads_test VALUES
  ('WEB001', 'Bob Smith', 'robert.smith@gmail.com', '212-555-0101', '12 Main Street New York NY'),
  ('WEB002', 'A Johnson', 'alice.johnson@gmail.com', NULL, '88 Park Avenue San Francisco CA'),
  ('WEB003', 'N Patel', NULL, '+91 9988776655', '44 Residency Road Bengaluru'),
  ('WEB004', 'Mike Chen', 'michael.chen@bank.com', '6465550142', NULL),
  ('WEB005', 'Sara W', 'sara.williams@gmail.com', '5125550177', '19 Oak Lane Austin TX'),
  ('WEB006', 'J. Wilson', 'james.wilson@gmail.com', '2065550166', '701 Pine St Seattle WA'),
  ('WEB007', 'Priya Nair', 'priya.nair@gmail.com', '9876501234', '17 MG Road Bengaluru KA'),
  ('WEB008', 'Kevin Miller', 'kevin.miller@gmail.com', '3035550144', '400 Blake Street, Denver, CO'),
  ('WEB009', 'Kevin Miller', 'kevin.miller@gmail.com', NULL, NULL),
  ('WEB010', 'Lina Zhang', 'lina.zhang@gmail.com', '4155550190', '900 Market Street, San Francisco, CA');

-- Source 5: Support records. Contains hard-to-identify and conflicting values.
CREATE OR REPLACE TABLE support_customers_test (
  customer_id STRING,
  person_name STRING,
  e_mail STRING,
  telephone STRING,
  addr STRING
);

INSERT INTO support_customers_test VALUES
  ('SUP001', 'Robert Smith', 'robert.smith@gmail.com', '2125550101', '12 Main Street, New York, NY'),
  ('SUP002', 'Robert Smith', 'robert.old@gmail.com', '2125550101', '12 Main Street, New York, NY'),
  ('SUP003', 'Alice Johnson', 'alice.johnson@gmail.com', '4155550111', '88 Park Avenue, San Francisco, CA'),
  ('SUP004', 'Peter Jones', 'peter.jones@bank.com', '6175550122', '10 Market Street, Boston, MA'),
  ('SUP005', 'James Wilson', 'james.wilson@gmail.com', '2065550166', '701 Pine Street, Seattle, WA'),
  ('SUP006', 'Kevin Miller', 'kevin.miller@gmail.com', '3035550144', '400 Blake Street, Denver, CO'),
  ('SUP007', 'Lina Zhang', NULL, '4155550190', '900 Market Street, San Francisco, CA'),
  ('SUP008', 'Unknown Customer', 'shared@example.com', NULL, NULL),
  ('SUP009', 'Unknown Customer', 'shared@example.com', NULL, NULL),
  ('SUP010', 'A. Johnson', 'alice.johnson@gmail.com', '4155550111', 'Different address supplied by support');

-- Validation: table sizes.
SELECT 'crm_customers_test' AS source, COUNT(*) AS row_count FROM crm_customers_test
UNION ALL SELECT 'banking_customers_test', COUNT(*) FROM banking_customers_test
UNION ALL SELECT 'creditcard_customers_test', COUNT(*) FROM creditcard_customers_test
UNION ALL SELECT 'web_leads_test', COUNT(*) FROM web_leads_test
UNION ALL SELECT 'support_customers_test', COUNT(*) FROM support_customers_test;

-- Validation: inspect all source schemas and sample records.
DESCRIBE TABLE crm_customers_test;
DESCRIBE TABLE banking_customers_test;
DESCRIBE TABLE creditcard_customers_test;
DESCRIBE TABLE web_leads_test;
DESCRIBE TABLE support_customers_test;

SELECT * FROM crm_customers_test ORDER BY record_id;
SELECT * FROM banking_customers_test ORDER BY record_id;
SELECT * FROM creditcard_customers_test ORDER BY record_id;
SELECT * FROM web_leads_test ORDER BY id;
SELECT * FROM support_customers_test ORDER BY customer_id;

-- Different-domain fixtures
-- These tables intentionally contain fields such as product_name, vendor_email,
-- manager_phone, and shipping_address. They help test whether classification is
-- based on business context or only on column-name patterns.

CREATE OR REPLACE TABLE product_catalog_test (
  product_id STRING,
  product_name STRING,
  product_email STRING,
  supplier_phone STRING,
  shipping_address STRING,
  sku STRING,
  category STRING,
  unit_price DECIMAL(10, 2)
);

INSERT INTO product_catalog_test VALUES
  ('PROD001', 'Road Runner Shoes', 'catalog@roadrunner.example', '+1-212-555-0201', '100 Factory Road, Newark, NJ', 'SHOE-001', 'Footwear', 79.99),
  ('PROD002', 'Blue Harbor Jacket', 'catalog@blueharbor.example', '+1-617-555-0202', '22 Harbor Way, Boston, MA', 'JKT-002', 'Apparel', 129.50),
  ('PROD003', 'Metro Coffee Maker', 'catalog@metrohome.example', '+1-312-555-0203', '8 Industrial Park, Chicago, IL', 'HOME-003', 'Appliances', 89.00),
  ('PROD004', 'Alpine Desk Lamp', NULL, '+1-303-555-0204', '41 Workshop Lane, Denver, CO', 'HOME-004', 'Home', 34.95),
  ('PROD005', 'Garden Pro Gloves', 'catalog@gardenpro.example', NULL, '9 Greenhouse Road, Austin, TX', 'GARD-005', 'Garden', 14.75);

CREATE OR REPLACE TABLE ecommerce_orders_test (
  order_id STRING,
  order_name STRING,
  buyer_email STRING,
  contact_phone STRING,
  delivery_address STRING,
  order_status STRING,
  order_total DECIMAL(10, 2),
  order_date DATE
);

INSERT INTO ecommerce_orders_test VALUES
  ('ORD001', 'Robert Smith Order', 'robert.smith@gmail.com', '+1-212-555-0101', '12 Main Street, New York, NY', 'SHIPPED', 159.98, DATE '2026-08-01'),
  ('ORD002', 'Alice Johnson Order', 'alice.johnson@gmail.com', '+1-415-555-0111', '88 Park Avenue, San Francisco, CA', 'DELIVERED', 89.00, DATE '2026-08-02'),
  ('ORD003', 'Neha Patel Order', 'neha.patel@gmail.com', '+91-9988776655', '44 Residency Road, Bengaluru, KA', 'PROCESSING', 129.50, DATE '2026-08-03'),
  ('ORD004', 'Guest Order', 'shared@example.com', NULL, NULL, 'CANCELLED', 34.95, DATE '2026-08-04'),
  ('ORD005', 'James Wilson Order', 'james.wilson@gmail.com', '+1-206-555-0166', '701 Pine Street, Seattle, WA', 'DELIVERED', 79.99, DATE '2026-08-05');

CREATE OR REPLACE TABLE employee_directory_test (
  employee_id STRING,
  employee_name STRING,
  work_email STRING,
  manager_phone STRING,
  office_address STRING,
  department STRING,
  hire_date DATE
);

INSERT INTO employee_directory_test VALUES
  ('EMP001', 'Karen Adams', 'karen.adams@company.example', '+1-212-555-0301', '1 Madison Avenue, New York, NY', 'Finance', DATE '2021-04-12'),
  ('EMP002', 'Luis Rivera', 'luis.rivera@company.example', '+1-415-555-0302', '200 Market Street, San Francisco, CA', 'Engineering', DATE '2022-07-18'),
  ('EMP003', 'Mina Shah', 'mina.shah@company.example', '+91-9988000303', '55 MG Road, Bengaluru, KA', 'Operations', DATE '2020-01-06'),
  ('EMP004', 'David Kim', NULL, '+1-312-555-0304', '400 Lakeside Drive, Chicago, IL', 'Sales', DATE '2023-09-25'),
  ('EMP005', 'Tara Wilson', 'tara.wilson@company.example', NULL, '700 Pine Street, Seattle, WA', 'Support', DATE '2019-11-04');

CREATE OR REPLACE TABLE supplier_directory_test (
  supplier_id STRING,
  supplier_name STRING,
  supplier_email STRING,
  supplier_phone STRING,
  warehouse_address STRING,
  tax_id STRING,
  payment_terms STRING
);

INSERT INTO supplier_directory_test VALUES
  ('SUPP001', 'Northwind Textiles', 'sales@northwind.example', '+1-973-555-0401', '100 Factory Road, Newark, NJ', 'TAX-001', 'NET30'),
  ('SUPP002', 'Blue Harbor Imports', 'contact@blueharbor.example', '+1-617-555-0402', '22 Harbor Way, Boston, MA', 'TAX-002', 'NET45'),
  ('SUPP003', 'Metro Home Goods', 'orders@metrohome.example', '+1-312-555-0403', '8 Industrial Park, Chicago, IL', 'TAX-003', 'NET30'),
  ('SUPP004', 'Alpine Manufacturing', NULL, '+1-303-555-0404', '41 Workshop Lane, Denver, CO', 'TAX-004', 'NET60'),
  ('SUPP005', 'Garden Pro Wholesale', 'sales@gardenpro.example', NULL, '9 Greenhouse Road, Austin, TX', 'TAX-005', 'PREPAID');

-- Domain validation: these tables should not normally be selected as customer
-- source tables. The MDM classifier is name-pattern based, so fields containing
-- name/email/phone/address may be discovered; use the app metadata preview to
-- inspect and confirm the detected logical fields.
SELECT 'product_catalog_test' AS source, COUNT(*) AS row_count FROM product_catalog_test
UNION ALL SELECT 'ecommerce_orders_test', COUNT(*) FROM ecommerce_orders_test
UNION ALL SELECT 'employee_directory_test', COUNT(*) FROM employee_directory_test
UNION ALL SELECT 'supplier_directory_test', COUNT(*) FROM supplier_directory_test;

DESCRIBE TABLE product_catalog_test;
DESCRIBE TABLE ecommerce_orders_test;
DESCRIBE TABLE employee_directory_test;
DESCRIBE TABLE supplier_directory_test;

SELECT * FROM product_catalog_test ORDER BY product_id;
SELECT * FROM ecommerce_orders_test ORDER BY order_id;
SELECT * FROM employee_directory_test ORDER BY employee_id;
SELECT * FROM supplier_directory_test ORDER BY supplier_id;


https://adb-1109595526709077.17.azuredatabricks.net/

dapiea7deda690abbfef7ddeb7adcfe1564d-3