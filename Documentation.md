# GoSales Analysis Project

## Project Overview
This project is focused on analyzing sales data from a relational dataset, a simulated e-commerce dataset of a company called **"GoSales"** with data from **2015 - 2018**. The goal is to build a comprehensive **ETL (Extract, Transform, Load)** pipeline and create data marts for analytical reporting using **MySQL**.

## Dataset Description
The **GoSales** dataset consists of multiple interrelated tables representing various aspects of a retail business. The dataset includes:

- **go_retailers**: Contains retailer information, such as retailer code, name, type, and country.
- **go_products**: Contains product information, including product number, product line, type, name, brand, color, cost, and price.
- **go_methods**: Represents different sales methods, such as web, email, telephone, etc.
- **go_daily_sales**: Holds daily sales transactions, including retailer code, product number, order method code, sale date, quantity, unit price, and sale price.

## 1. Operational Layer
The operational layer focuses on setting up and managing the raw data structure using MySQL. It involves importing and organizing the **GoSales** dataset to build a robust, relational database that serves as the foundation for further analytics.

### Tables
- **go_retailers**: Contains details about retailers, including retailer code (primary key), name, type, and country.
- **go_products**: Includes product information such as product number (primary key), product line, type, name, brand, color, cost, and price.
- **go_methods**: Represents different sales methods (e.g., web, email) using a primary key (`order_method_code`).
- **go_daily_sales**: Stores daily sales data, linked to the other tables via foreign keys (`retailer_code`, `product_number`, `order_method_code`), and includes columns for sale date, quantity, and prices.

### Key Steps
- **Schema and Table Creation**: Define tables with appropriate primary and foreign keys to maintain relational integrity and ensure normalized structure.
- **Data Loading**: Use `LOAD DATA INFILE` with relative paths to load data from CSV files into the tables, ensuring the operational layer is populated with raw sales, product, and retailer data.

## 2. Analytical Layer
The analytical layer transforms and structures the operational data to support advanced analytics and reporting. It involves creating a **denormalized table** for efficient querying.

### Denormalized Table
- **sales_denormalized**: A single table combining data from multiple tables (`go_daily_sales`, `go_retailers`, `go_products`, `go_methods`) to provide a comprehensive view of sales activities. This table includes **transformation** such as the following additional calculated fields:
  - **Total Sales**: `unit_sale_price * quantity`
  - **Total Profit**: `(unit_sale_price - unit_cost) * quantity`
  - **Profit Percentage**: `(unit_sale_price - unit_cost) / unit_cost * 100`
  - **Order Method Category**: Categorizes methods as "Online" or "Not Online."

## 3. ETL Pipeline
- **Stored Procedures**: Automate the process of transforming and loading data into the denormalized table (`sales_denormalized`), ensuring data is consistently refreshed.
- **Triggers**: Implement triggers (`AFTER INSERT`, `AFTER UPDATE`, `AFTER DELETE`) on `go_daily_sales` to keep the denormalized table up-to-date in real-time.

## 4. Data Marts
The show layer provides data marts, which are simplified views designed for specific analyses, enhancing the speed and efficiency of reporting.

### Data Marts
Since performances are usually evaluated on a monthly basis, data marts are created providing views grouped by months:

- **Product Performance**: Evaluates monthly product performance based on total sales, total profit, and profit margins, categorized into "High," "Medium," and "Low" profit levels.
- **Retailer Sales Contribution**: Shows the monthly contribution of each retailer to the overall sales and profit, expressed as percentages.
- **Product Line Performance**: Analyzes monthly sales and profit performance for each product line with categories based on profit margins.
- **Order Method Profitability**: Compares monthly profitability between different order methods (e.g., "Online" vs. "Not Online").
- **Most Sold Product**: Lists the most sold products per month, ranked by quantity sold.

## 5. Testing and Documentation
- Each data mart is validated through specific testing queries that filter data by month and relevant dimensions (e.g., product number, retailer code).
- Documentation is maintained using SQL comments and GitHub markdown to explain each step.

**Note**: Scripts and data files are available in the GitHub repository for full reproducibility.
