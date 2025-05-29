CREATE OR REPLACE DATABASE ECOMERSE_DB;

CREATE OR REPLACE SCHEMA ECOMERSE_DB.PUBLIC;

CREATE OR REPLACE STAGE ecommerce_stage;

CREATE OR REPLACE TABLE ecommerce_raw (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity NUMBER,
    price FLOAT,
    discount FLOAT,
    total_price FLOAT,
    delivery_address STRING,
    payment_method STRING,
    status STRING,
    order_date STRING
);

CREATE OR REPLACE FILE FORMAT ecommerce_csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;

CREATE OR REPLACE STAGE ecommerce_stage;

COPY INTO ecommerce_raw
FROM @ecommerce_stage/ecommerce_orders.csv
FILE_FORMAT = ecommerce_csv_format
ON_ERROR = 'CONTINUE';

--Анализ и обработка на данните--
--Липсващ адрес при Delivered → към td_for_review--
CREATE OR REPLACE TABLE td_for_review AS
SELECT *
FROM ecommerce_raw
WHERE delivery_address IS NULL AND LOWER(status) = 'delivered';

--Липсващ customer_id → td_suspicious_records--
CREATE OR REPLACE TABLE td_suspicious_records AS
SELECT *
FROM ecommerce_raw
WHERE customer_id IS NULL;

--Липсващ payment_method → попълване с "Unknown"--
CREATE OR REPLACE TABLE ecommerce_step_1 AS
SELECT *,
       COALESCE(payment_method, 'Unknown') AS payment_method_fixed
FROM ecommerce_raw
WHERE customer_id IS NOT NULL;

--Грешен формат на дата → td_invalid_date_format--
CREATE OR REPLACE TABLE td_invalid_date_format AS
SELECT *
FROM ecommerce_step_1
WHERE TRY_TO_DATE(order_date, 'YYYY-MM-DD') IS NULL;

--Отрицателни или нулеви стойности за количество и цена → отделна таблица--
CREATE OR REPLACE TABLE td_invalid_qty_price AS
SELECT *
FROM ecommerce_step_1
WHERE quantity <= 0 OR price <= 0;

--Отстъпки под 0 или над 50% → нормализация--
CREATE OR REPLACE TABLE ecommerce_step_2 AS
SELECT *,
       CASE 
           WHEN discount < 0 THEN 0
           WHEN discount > 50 THEN 50
           ELSE discount
       END AS discount_fixed
FROM ecommerce_step_1
WHERE quantity > 0 AND price > 0
  AND TRY_TO_DATE(order_date, 'YYYY-MM-DD') IS NOT NULL;

--Грешна крайна цена → коригиране--
CREATE OR REPLACE TABLE ecommerce_step_3 AS
SELECT *,
       ROUND((quantity * price) * (1 - discount_fixed / 100), 2) AS total_price_corrected
FROM ecommerce_step_2;

--Неконсистентен статус (Delivered, но без адрес)--
CREATE OR REPLACE TABLE ecommerce_step_4 AS
SELECT *,
       CASE
           WHEN LOWER(status) = 'delivered' AND delivery_address IS NULL THEN 'Pending'
           ELSE status
       END AS final_status
FROM ecommerce_step_3;

--Премахване на дупликати (по order_id + product_id като ключ)--
CREATE OR REPLACE TABLE td_clean_records AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY order_date) AS rn
    FROM ecommerce_step_4
) t
WHERE rn = 1;
