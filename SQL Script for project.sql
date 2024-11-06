-- Step 1: Create the GoSales schema and use it
Create Schema GoSales;
use GoSales;

-- Step 2: Drop tables if they exist for clean execution
DROP TABLE IF EXISTS go_daily_sales;
DROP TABLE IF EXISTS go_products;
DROP TABLE IF EXISTS go_methods;
DROP TABLE IF EXISTS go_retailers;

-- Step 3: Create the necessary tables
CREATE TABLE go_retailers (
    retailer_code INT NOT NULL PRIMARY KEY,
    retailer_name VARCHAR(100),
    type VARCHAR(100),
    country VARCHAR(50)
);

CREATE TABLE go_products (
    product_number INT NOT NULL PRIMARY KEY,
    product_line VARCHAR(100),
    product_type VARCHAR(100),
    product_name VARCHAR(100),
    product_brand VARCHAR(50),
    product_color VARCHAR(50),
    unit_cost DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    unit_price DECIMAL(10, 2) NOT NULL DEFAULT 0.00
);

CREATE TABLE go_methods (
    order_method_code INT NOT NULL PRIMARY KEY,
    order_method_type VARCHAR(50)
);

CREATE TABLE go_daily_sales (
    retailer_code INT NOT NULL,
    product_number INT NOT NULL,
    order_method_code INT NOT NULL,
    sale_date DATE NOT NULL,
    quantity INT NOT NULL DEFAULT 0,
    unit_price DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    unit_sale_price DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    FOREIGN KEY (retailer_code) REFERENCES go_retailers(retailer_code),
    FOREIGN KEY (product_number) REFERENCES go_products(product_number),
    FOREIGN KEY (order_method_code) REFERENCES go_methods(order_method_code)
);
-- Step 4: Load data into tables using relative paths

-- Load data into go_retailers table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/go_retailers.csv'
INTO TABLE go_retailers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load data into go_products table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/go_products.csv'
INTO TABLE go_products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_number, product_line, product_type, product_name, product_brand, product_color, unit_cost, unit_price);

-- Load data into go_methods table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/go_methods.csv'
INTO TABLE go_methods
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load data into go_daily_sales table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/go_daily_sales.csv'
INTO TABLE go_daily_sales
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(retailer_code, product_number, order_method_code, @sale_date, quantity, unit_price, unit_sale_price)
SET sale_date = STR_TO_DATE(@sale_date, '%d/%m/%Y');

-- Step 5: Create the denormalized table stored procedure (TRANSFORMATION & LOADING)

DELIMITER //

CREATE PROCEDURE RefreshSalesDenormalized()
BEGIN
    -- Drop the existing denormalized table if it exists
    DROP TABLE IF EXISTS sales_denormalized;

    -- Create the denormalized table
    CREATE TABLE sales_denormalized AS
    SELECT 
        ds.retailer_code, 
        r.retailer_name, 
        ds.product_number, 
        p.product_name, 
        ds.order_method_code, 
        m.order_method_type, 
        ds.sale_date, 
        ds.quantity, 
        ds.unit_sale_price, 
        ds.unit_price, 
        
        -- Tranformation: Calculate total sales for each row
        (ds.unit_sale_price * ds.quantity) AS total_sales,
        
        -- Tranformation: Calculate total profit for each row
        (ds.unit_sale_price - p.unit_cost) * ds.quantity AS total_profit,

        -- Tranformation: Calculate profit percentage for each row
        CASE 
            WHEN p.unit_cost > 0 THEN 
                ROUND(((ds.unit_sale_price - p.unit_cost) / p.unit_cost) * 100, 2)
            ELSE 
                NULL 
        END AS profit_percentage,

        -- Tranformation: Order method category (Online, Not Online)
        CASE 
            WHEN m.order_method_type IN ('Web', 'E-mail') THEN 'Online'
            ELSE 'Offline'
        END AS order_method_category

    FROM go_daily_sales ds
    JOIN go_retailers r ON ds.retailer_code = r.retailer_code
    JOIN go_products p ON ds.product_number = p.product_number
    JOIN go_methods m ON ds.order_method_code = m.order_method_code;
END //

DELIMITER ;

CALL RefreshSalesDenormalized();

-- Testing for sales_denormalized
SELECT * FROM sales_denormalized LIMIT 10;

DELIMITER //

-- Step 6: Create data marts for monthly views with date filters
-- Product Performance Data Mart
DROP VIEW IF EXISTS product_performance;

CREATE VIEW product_performance AS
SELECT 
    product_number,
    product_name,
    DATE_FORMAT(sale_date, '%m-%Y') AS sale_month,
    SUM(total_sales) AS total_revenue,
    SUM(total_profit) AS total_profit,
    AVG(profit_percentage) AS avg_profit_margin,
    SUM(quantity) AS total_quantity_sold,

    -- Classify products based on average profit margin
    CASE 
        WHEN AVG(profit_percentage) > 20 THEN 'High Profit Margin'
        WHEN AVG(profit_percentage) BETWEEN 10 AND 20 THEN 'Medium Profit Margin'
        ELSE 'Low Profit Margin'
    END AS profit_margin_category

FROM sales_denormalized
GROUP BY product_number, product_name, sale_month;

-- Retailer Sales Contribution Data Mart
DROP VIEW IF EXISTS retailer_sales_contribution;

CREATE VIEW retailer_sales_contribution AS
SELECT 
    retailer_code,
    retailer_name,
    DATE_FORMAT(sale_date, '%m-%Y') AS sale_month,
    SUM(total_sales) AS total_revenue,
    SUM(total_profit) AS total_profit,
    ROUND((SUM(total_sales) / (SELECT SUM(total_sales) FROM sales_denormalized WHERE DATE_FORMAT(sale_date, '%m-%Y') = sale_month) * 100), 2) AS sales_contribution_percentage,
    ROUND((SUM(total_profit) / (SELECT SUM(total_profit) FROM sales_denormalized WHERE DATE_FORMAT(sale_date, '%m-%Y') = sale_month) * 100), 2) AS profit_contribution_percentage
FROM sales_denormalized
GROUP BY retailer_code, retailer_name, sale_month;

-- Product Line Performance Data Mart
DROP VIEW IF EXISTS product_line_performance;

CREATE VIEW product_line_performance AS
SELECT 
    p.product_line,
    DATE_FORMAT(ds.sale_date, '%m-%Y') AS sale_month,
    SUM(ds.total_sales) AS total_revenue,
    SUM(ds.total_profit) AS total_profit,
    AVG(ds.profit_percentage) AS avg_profit_margin,
    SUM(ds.quantity) AS total_quantity_sold,

    -- Classify product lines based on average profit margin
    CASE 
        WHEN AVG(ds.profit_percentage) > 20 THEN 'High Profit Margin'
        WHEN AVG(ds.profit_percentage) BETWEEN 10 AND 20 THEN 'Medium Profit Margin'
        ELSE 'Low Profit Margin'
    END AS profit_margin_category

FROM sales_denormalized ds
JOIN go_products p ON ds.product_number = p.product_number
GROUP BY p.product_line, sale_month;

-- Order Method Profitability Data Mart
DROP VIEW IF EXISTS order_method_profitability;

CREATE VIEW order_method_profitability AS
SELECT 
    order_method_category,
    DATE_FORMAT(sale_date, '%m-%Y') AS sale_month,
    SUM(total_sales) AS total_revenue,
    SUM(total_profit) AS total_profit,
    AVG(profit_percentage) AS avg_profit_margin,
    COUNT(*) AS total_orders,
    SUM(quantity) AS total_quantity_sold
FROM sales_denormalized
GROUP BY order_method_category, sale_month;

-- Most Sold Product Data Mart
DROP VIEW IF EXISTS most_sold_product;

CREATE VIEW most_sold_product AS
SELECT 
    product_number,
    product_name,
    DATE_FORMAT(sale_date, '%m-%Y') AS sale_month,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_sales) AS total_revenue
FROM sales_denormalized
GROUP BY product_number, product_name, sale_month
ORDER BY total_quantity_sold DESC;

-- Testing queries for all data marts in GoSales with month (MM-YYYY format) and specific dimension filters, selecting all columns

-- Test for Product Performance Data Mart
-- Verifies if the view contains data for a specific product and specific month
SELECT 
    product_number,
    product_name,
    sale_month,
    total_quantity_sold,
    total_revenue,
    total_profit,
    avg_profit_margin,
    profit_margin_category
FROM product_performance
WHERE sale_month = '07-2018' -- Example specific month (July 2018)
AND product_number = 126140 -- Example product number
ORDER BY total_quantity_sold DESC
LIMIT 10;

-- Test for Retailer Sales Contribution Data Mart
-- Verifies if the view contains data for a specific retailer and specific month
SELECT 
    retailer_code,
    retailer_name,
    sale_month,
    total_revenue,
    total_profit,
    sales_contribution_percentage,
    profit_contribution_percentage
FROM retailer_sales_contribution
WHERE sale_month = '07-2018' -- Example specific month (July 2018)
AND retailer_code = 1275 -- Example retailer code
ORDER BY total_revenue DESC
LIMIT 10;

-- Test for Product Line Performance Data Mart
-- Verifies if the view contains data for a specific product line and specific month
SELECT 
    product_line,
    sale_month,
    total_revenue,
    total_profit,
    avg_profit_margin,
    total_quantity_sold,
    profit_margin_category
FROM product_line_performance
WHERE sale_month = '07-2018' -- Example specific month (July 2018)
AND product_line = 'Camping Equipment' -- Example product line
ORDER BY total_revenue DESC
LIMIT 10;

-- Test for Order Method Profitability Data Mart
-- Verifies if the view contains data for specific month
SELECT 
    order_method_category,
    sale_month,
    total_revenue,
    total_profit,
    avg_profit_margin,
    total_orders,
    total_quantity_sold
FROM order_method_profitability
WHERE sale_month = '07-2018' -- Example specific month (July 2018)
ORDER BY total_revenue DESC
LIMIT 10;

-- Test for Most Sold Product Data Mart
-- Verifies if the view contains data for a specific month
SELECT 
    product_number,
    product_name,
    sale_month,
    total_quantity_sold,
    total_revenue
FROM most_sold_product
WHERE sale_month = '07-2018' -- Example specific month (July 2018)
ORDER BY total_quantity_sold DESC
LIMIT 10;


-- Step 7: Create triggers for Insert, Update and Delete in go_daily_sales
CREATE TRIGGER refresh_denormalized_after_insert
AFTER INSERT ON go_daily_sales
FOR EACH ROW
BEGIN
    CALL RefreshSalesDenormalized();
END //

DELIMITER ;

DELIMITER //

CREATE TRIGGER refresh_denormalized_after_update
AFTER UPDATE ON go_daily_sales
FOR EACH ROW
BEGIN
    CALL RefreshSalesDenormalized();
END //

DELIMITER ;

DELIMITER //

CREATE TRIGGER refresh_denormalized_after_delete
AFTER DELETE ON go_daily_sales
FOR EACH ROW
BEGIN
    CALL RefreshSalesDenormalized();
END //

DELIMITER ;


