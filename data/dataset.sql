CREATE DATABASE supply_chain;
USE supply_chain;
CREATE TABLE dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(100),
    product_category VARCHAR(50)
);CREATE TABLE dim_location (
    location_key INT AUTO_INCREMENT PRIMARY KEY,
    warehouse_location VARCHAR(100),
    region VARCHAR(50)
);CREATE TABLE dim_date (
    date_key DATE PRIMARY KEY,
    day INT,
    month INT,
    quarter INT,
    year INT
);CREATE TABLE fact_orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    date_key DATE,
    product_key INT,
    location_key INT,
    
    order_quantity INT,
    demand_forecast INT,
    actual_demand INT,

    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
);CREATE TABLE fact_shipments (
    shipment_id INT AUTO_INCREMENT PRIMARY KEY,
    date_key DATE,
    product_key INT,
    location_key INT,

    supplier_name VARCHAR(100),
    lead_time_days INT,
    delivery_time_days INT,
    delivery_status VARCHAR(20),

    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
);CREATE TABLE fact_inventory_snapshot (
    snapshot_id INT AUTO_INCREMENT PRIMARY KEY,
    date_key DATE,
    product_key INT,
    location_key INT,

    opening_stock INT,
    incoming_stock INT,
    units_sold INT,
    closing_stock INT,
    reorder_level INT,
    inventory_holding_cost DECIMAL(12,2),
    stockout_status VARCHAR(10),

    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
);CREATE INDEX idx_orders_date ON fact_orders(date_key);
CREATE INDEX idx_orders_product ON fact_orders(product_key);
CREATE INDEX idx_orders_location ON fact_orders(location_key);

CREATE INDEX idx_shipments_date ON fact_shipments(date_key);
CREATE INDEX idx_shipments_product ON fact_shipments(product_key);
CREATE INDEX idx_shipments_location ON fact_shipments(location_key);

CREATE INDEX idx_inventory_date ON fact_inventory_snapshot(date_key);
CREATE INDEX idx_inventory_product ON fact_inventory_snapshot(product_key);
CREATE INDEX idx_inventory_location ON fact_inventory_snapshot(location_key);

SHOW TABLES;

CREATE TABLE staging_supply_chain (
    date_key DATE,
    month INT,
    quarter INT,
    year INT,
    product_id VARCHAR(50),
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    supplier_name VARCHAR(100),
    warehouse_location VARCHAR(100),
    region VARCHAR(50),
    opening_stock INT,
    incoming_stock INT,
    units_sold INT,
    closing_stock INT,
    reorder_level INT,
    reorder_status VARCHAR(10),
    lead_time_days INT,
    order_quantity INT,
    delivery_status VARCHAR(20),
    delivery_time_days INT,
    inventory_holding_cost DECIMAL(12,2),
    stockout_status VARCHAR(10),
    demand_forecast INT,
    actual_demand INT,
    forecast_accuracy DECIMAL(5,2),
    inventory_turnover_ratio DECIMAL(5,2)
);

SELECT * FROM staging_supply_chain LIMIT 10;

INSERT INTO dim_product (product_id, product_name, product_category)
SELECT DISTINCT
    product_id,
    product_name,
    product_category
FROM staging_supply_chain;

INSERT INTO dim_location (warehouse_location, region)
SELECT DISTINCT
    warehouse_location,
    region
FROM staging_supply_chain;

INSERT INTO dim_date (date_key, day, month, quarter, year)
SELECT DISTINCT
    date_key,
    DAY(date_key),
    MONTH(date_key),
    QUARTER(date_key),
    YEAR(date_key)
FROM staging_supply_chain;

INSERT IGNORE INTO dim_product (product_id, product_name, product_category)
SELECT DISTINCT
    product_id,
    product_name,
    product_category
FROM staging_supply_chain;
TRUNCATE TABLE dim_product;

SET SQL_SAFE_UPDATES = 0;

DELETE FROM fact_orders;
DELETE FROM fact_shipments;
DELETE FROM fact_inventory_snapshot;

DELETE FROM dim_product;

SET SQL_SAFE_UPDATES = 1;

SELECT * FROM dim_product;

INSERT INTO dim_product (product_id, product_name, product_category)
SELECT DISTINCT
    product_id,
    product_name,
    product_category
FROM staging_supply_chain;

INSERT INTO dim_product (product_id, product_name, product_category)
SELECT 
    product_id,
    MAX(product_name),
    MAX(product_category)
FROM staging_supply_chain
GROUP BY product_id;

SELECT COUNT(DISTINCT product_id) FROM staging_supply_chain;
SELECT COUNT(*) FROM dim_product;

INSERT INTO dim_product (product_id, product_name, product_category)
SELECT DISTINCT
    product_id,
    product_name,
    product_category
FROM staging_supply_chain;

SELECT product_id, COUNT(*) 
FROM staging_supply_chain
GROUP BY product_id
HAVING COUNT(*) > 1;

SELECT *
FROM staging_supply_chain
WHERE product_id = 'P105';

DELETE FROM dim_product;

INSERT INTO dim_product (product_id, product_name, product_category)
SELECT 
    product_id,
    MAX(product_name),
    MAX(product_category)
FROM staging_supply_chain
GROUP BY product_id;

SELECT * FROM dim_product;

INSERT INTO dim_location (warehouse_location, region)
SELECT 
    warehouse_location,
    MAX(region)
FROM staging_supply_chain
GROUP BY warehouse_location;

SELECT * FROM dim_location;

INSERT INTO dim_date (date_key, day, month, quarter, year)
SELECT DISTINCT
    date_key,
    DAY(date_key),
    MONTH(date_key),
    QUARTER(date_key),
    YEAR(date_key)
FROM staging_supply_chain;

INSERT INTO fact_orders (
    date_key,
    product_key,
    location_key,
    order_quantity,
    demand_forecast,
    actual_demand
)
SELECT
    s.date_key,
    p.product_key,
    l.location_key,
    s.order_quantity,
    s.demand_forecast,
    s.actual_demand
FROM staging_supply_chain s
JOIN dim_product p 
    ON s.product_id = p.product_id
JOIN dim_location l 
    ON s.warehouse_location = l.warehouse_location;
    
    INSERT INTO fact_shipments (
    date_key,
    product_key,
    location_key,
    supplier_name,
    lead_time_days,
    delivery_time_days,
    delivery_status
)
SELECT
    s.date_key,
    p.product_key,
    l.location_key,
    s.supplier_name,
    s.lead_time_days,
    s.delivery_time_days,
    s.delivery_status
FROM staging_supply_chain s
JOIN dim_product p 
    ON s.product_id = p.product_id
JOIN dim_location l 
    ON s.warehouse_location = l.warehouse_location;
    
    INSERT INTO fact_inventory_snapshot (
    date_key,
    product_key,
    location_key,
    opening_stock,
    incoming_stock,
    units_sold,
    closing_stock,
    reorder_level,
    inventory_holding_cost,
    stockout_status
)
SELECT
    s.date_key,
    p.product_key,
    l.location_key,
    s.opening_stock,
    s.incoming_stock,
    s.units_sold,
    s.closing_stock,
    s.reorder_level,
    s.inventory_holding_cost,
    s.stockout_status
FROM staging_supply_chain s
JOIN dim_product p 
    ON s.product_id = p.product_id
JOIN dim_location l 
    ON s.warehouse_location = l.warehouse_location;
    
    SELECT COUNT(*) FROM staging_supply_chain;
SELECT COUNT(*) FROM fact_orders;
SELECT COUNT(*) FROM fact_shipments;
SELECT COUNT(*) FROM fact_inventory_snapshot;

SELECT 
    SUM(closing_stock) AS total_inventory
FROM fact_inventory_snapshot;

SELECT 
    SUM(units_sold) AS total_units_sold
FROM fact_inventory_snapshot;

SELECT 
    COUNT(*) AS stockout_count
FROM fact_inventory_snapshot
WHERE stockout_status = 'Yes';

SELECT 
    COUNT(*) AS reorder_count
FROM fact_inventory_snapshot
WHERE closing_stock < reorder_level;

SELECT 
    ROUND(
        SUM(CASE WHEN delivery_status = 'On Time' THEN 1 ELSE 0 END) * 100.0 
        / COUNT(*), 2
    ) AS on_time_delivery_pct
FROM fact_shipments;

SELECT 
    AVG(lead_time_days) AS avg_lead_time
FROM fact_shipments;

SELECT 
    ROUND(
        AVG(1 - ABS(demand_forecast - actual_demand) / actual_demand) * 100,
        2
    ) AS forecast_accuracy_pct
FROM fact_orders;

SELECT 
    ROUND(
        SUM(units_sold) /
        (AVG(opening_stock + closing_stock) / 2),
        2
    ) AS inventory_turnover_ratio
FROM fact_inventory_snapshot;

SELECT 
    p.product_category,
    SUM(f.closing_stock) AS total_inventory,
    SUM(f.units_sold) AS total_sales
FROM fact_inventory_snapshot f
JOIN dim_product p 
    ON f.product_key = p.product_key
GROUP BY p.product_category
ORDER BY total_sales DESC;

SELECT 
    supplier_name,
    ROUND(
        SUM(CASE WHEN delivery_status = 'On Time' THEN 1 ELSE 0 END) * 100.0 
        / COUNT(*), 2
    ) AS on_time_pct
FROM fact_shipments
GROUP BY supplier_name
ORDER BY on_time_pct DESC;

SELECT 
    l.region,
    SUM(f.units_sold) AS total_sales,
    SUM(f.closing_stock) AS inventory
FROM fact_inventory_snapshot f
JOIN dim_location l 
    ON f.location_key = l.location_key
GROUP BY l.region;

SELECT 
    d.year,
    d.month,
    SUM(f.units_sold) AS monthly_sales
FROM fact_inventory_snapshot f
JOIN dim_date d 
    ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


SELECT 
    p.product_name,
    SUM(f.units_sold) AS total_sales
FROM fact_inventory_snapshot f
JOIN dim_product p 
    ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_sales DESC
LIMIT 5;

SELECT 
    p.product_name,
    SUM(f.closing_stock) AS total_stock,
    SUM(f.units_sold) AS total_sales
FROM fact_inventory_snapshot f
JOIN dim_product p 
    ON f.product_key = p.product_key
GROUP BY p.product_name
HAVING total_sales < 1000
ORDER BY total_stock DESC;

SELECT 
    p.product_name,
    SUM(f.closing_stock) AS dead_stock
FROM fact_inventory_snapshot f
JOIN dim_product p 
    ON f.product_key = p.product_key
GROUP BY p.product_name
HAVING SUM(f.units_sold) = 0;

SELECT 
    p.product_name,
    COUNT(*) AS stockout_count
FROM fact_inventory_snapshot f
JOIN dim_product p 
    ON f.product_key = p.product_key
WHERE f.stockout_status = 'Yes'
GROUP BY p.product_name
ORDER BY stockout_count DESC;

SELECT 
    supplier_name,
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN delivery_status = 'Delayed' THEN 1 ELSE 0 END) AS delays
FROM fact_shipments
GROUP BY supplier_name
ORDER BY delays DESC;

SELECT product_key, COUNT(*) AS count_rows
FROM fact_inventory_snapshot
GROUP BY product_key;

SELECT COUNT(*) FROM staging_supply_chain;
SELECT COUNT(*) FROM fact_inventory_snapshot;

DELETE FROM fact_inventory_snapshot;

INSERT INTO fact_inventory_snapshot (
    date_key,
    product_key,
    location_key,
    opening_stock,
    incoming_stock,
    units_sold,
    closing_stock,
    reorder_level,
    inventory_holding_cost,
    stockout_status
)
SELECT
    s.date_key,
    p.product_key,
    l.location_key,
    s.opening_stock,
    s.incoming_stock,
    s.units_sold,
    s.closing_stock,
    s.reorder_level,
    s.inventory_holding_cost,
    s.stockout_status
FROM staging_supply_chain s
JOIN dim_product p 
    ON s.product_id = p.product_id
JOIN dim_location l 
    ON s.warehouse_location = l.warehouse_location;
    
    SELECT product_key, COUNT(*) 
FROM fact_inventory_snapshot
GROUP BY product_key;

SELECT date_key, COUNT(*) 
FROM fact_shipments
GROUP BY date_key
HAVING COUNT(*) > 1;

SELECT product_id, product_category, COUNT(*) 
FROM staging_supply_chain
GROUP BY product_id, product_category
ORDER BY product_id;

INSERT INTO dim_product (product_id, product_name, product_category)
SELECT product_id, product_name, product_category
FROM (
    SELECT 
        product_id,
        product_name,
        product_category,
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM staging_supply_chain
    GROUP BY product_id, product_name, product_category
) t
WHERE rn = 1;

SELECT 
    product_id,
    product_name,
    product_category,
    COUNT(*) AS cnt
FROM staging_supply_chain
GROUP BY product_id, product_name, product_category;

SET SQL_SAFE_UPDATES = 0;
DELETE FROM dim_product;

DELETE FROM fact_inventory_snapshot;
DELETE FROM fact_orders;
DELETE FROM fact_shipments;

DELETE FROM dim_product;

INSERT INTO dim_product (product_id, product_name, product_category)
SELECT product_id, product_name, product_category
FROM (
    SELECT 
        product_id,
        product_name,
        product_category,
        cnt,
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY cnt DESC, product_category ASC
        ) AS rn
    FROM (
        SELECT 
            product_id,
            product_name,
            product_category,
            COUNT(*) AS cnt
        FROM staging_supply_chain
        GROUP BY product_id, product_name, product_category
    ) t1
) t2
WHERE rn = 1;

INSERT INTO fact_inventory_snapshot (
    date_key,
    product_key,
    location_key,
    opening_stock,
    incoming_stock,
    units_sold,
    closing_stock,
    reorder_level,
    inventory_holding_cost,
    stockout_status
)
SELECT
    s.date_key,
    p.product_key,
    l.location_key,
    s.opening_stock,
    s.incoming_stock,
    s.units_sold,
    s.closing_stock,
    s.reorder_level,
    s.inventory_holding_cost,
    s.stockout_status
FROM staging_supply_chain s
JOIN dim_product p 
    ON s.product_id = p.product_id
JOIN dim_location l 
    ON s.warehouse_location = l.warehouse_location;
    
    INSERT INTO fact_orders (
    date_key,
    product_key,
    location_key,
    order_quantity,
    demand_forecast,
    actual_demand
)
SELECT
    s.date_key,
    p.product_key,
    l.location_key,
    s.order_quantity,
    s.demand_forecast,
    s.actual_demand
FROM staging_supply_chain s
JOIN dim_product p 
    ON s.product_id = p.product_id
JOIN dim_location l 
    ON s.warehouse_location = l.warehouse_location;
    
    INSERT INTO fact_shipments (
    date_key,
    product_key,
    location_key,
    supplier_name,
    lead_time_days,
    delivery_time_days,
    delivery_status
)
SELECT
    s.date_key,
    p.product_key,
    l.location_key,
    s.supplier_name,
    s.lead_time_days,
    s.delivery_time_days,
    s.delivery_status
FROM staging_supply_chain s
JOIN dim_product p 
    ON s.product_id = p.product_id
JOIN dim_location l 
    ON s.warehouse_location = l.warehouse_location;
    
    SELECT COUNT(*) FROM staging_supply_chain;
SELECT COUNT(*) FROM fact_inventory_snapshot;
SELECT COUNT(*) FROM fact_orders;
SELECT COUNT(*) FROM fact_shipments;

SELECT 
    s.product_id,
    p.product_key,
    p.product_category
FROM staging_supply_chain s
JOIN dim_product p 
ON s.product_id = p.product_id
LIMIT 10;

INSERT INTO dim_product (product_id, product_name, product_category)
SELECT product_id, product_name, product_category
FROM (
    SELECT 
        s.product_id,
        s.product_name,
        s.product_category,
        s.date_key,
        ROW_NUMBER() OVER (
            PARTITION BY s.product_id
            ORDER BY s.date_key DESC
        ) AS rn
    FROM staging_supply_chain s
) t
WHERE rn = 1;

SELECT * FROM dim_product;

DELETE FROM dim_product
WHERE product_id IS NULL;

SELECT COUNT(*) FROM dim_product;

SELECT * FROM dim_product WHERE product_id IS NULL;
