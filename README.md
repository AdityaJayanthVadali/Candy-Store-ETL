# Candy Store Analytics System

A comprehensive data processing and analytics system for candy store operations that handles batch transaction processing, inventory management, and sales forecasting.
## Overview

The Candy Store Analytics System is a PySpark-based application that processes daily transactions from MongoDB, manages inventory, and generates sales/profit forecasts. The system connects to multiple data sources, processes transactions while respecting inventory constraints, and provides actionable business intelligence through summary reports and forecasts.
10 Days worth of transaction information, in the form of JSON files, product information and customer information were provided as csv files.

## Data Load
The transaction data was loaded onto MongoDB:

```bash
    mongoimport --db mydatabase --collection "$collection_name" --file "$file" --jsonArray
```

The Products and Customer csv files were loaded onto mySQL:

```bash
# LOAD INTO MYSQL IN CLI USING THE FOLLOWING:
    mysql --local-infile=1 -u your_user -p
``` 

```sql
-- ENABLE local-infile for import with the following command:
SET GLOBAL local_infile = 1;

-- CREATE DATABASE AND SET IT AS CURRENT:
CREATE DATABASE candy_store;
USE candy_store;

-- CREATE TABLE CUSTOMERS :
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    address VARCHAR(255),
    phone VARCHAR(50)
);

-- LOAD CSV INTO THE TABLE:
LOAD DATA LOCAL INFILE ‘/home/adityajayanthvadali/Desktop/DSCI-644/project-2/data/dataset_5/customers.csv’
INTO TABLE products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

-- CREATE TABLE PRODUCTS:
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    product_category VARCHAR(255),
    product_subcategory VARCHAR(255),
    product_shape VARCHAR(255),
    sales_price DECIMAL(10,2),
    cost_to_make DECIMAL(10,2),
    stock INT
);

-- LOAD CSV INTO THE TABLE:
LOAD DATA LOCAL INFILE ‘/home/adityajayanthvadali/Desktop/DSCI-644/project-2/data/dataset_5/products.csv’
INTO TABLE products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;
```

## Features

- **Transaction Processing**: Processes daily sales transactions from MongoDB
- **Inventory Management**: Tracks and updates product inventory levels
- **Order Processing**: Validates orders against available inventory
- **Data Integration**: Connects to MongoDB for transactions and MySQL for product/customer data
- **Reporting**: Generates detailed order line items, order summaries, and daily business reports
- **Forecasting**: Uses time series analysis to predict future sales and profits

## System Architecture

The system consists of two main components:

1. **DataProcessor**: Core class that handles data processing, inventory management, and reporting
2. **Main Application**: Orchestrates the overall processing flow and configuration
3. **ProphetForecaster**: Configures and implements system for time series forecasting

## Dependencies

- PySpark (with MongoDB and MySQL connectors)
- Python 3.x
- MongoDB
- MySQL
- Additional Python packages:
  - python-dotenv
  - numpy
  - Prophet (via custom time_series module)

## Configuration

The system uses environment variables for configuration:

```
# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017
MONGO_DB=candy_store
MONGO_COLLECTION_PREFIX=transactions_
MONGO_START_DATE=20230101
MONGO_END_DATE=20230105

# MySQL Configuration
MYSQL_CONNECTOR_PATH=/path/to/mysql-connector.jar
MYSQL_URL=jdbc:mysql://localhost:3306/candy_store
MYSQL_USER=username
MYSQL_PASSWORD=password
MYSQL_DB=candy_store
CUSTOMERS_TABLE=customers
PRODUCTS_TABLE=products

# Output Configuration
OUTPUT_PATH=./output
RELOAD_INVENTORY_DAILY=false
```

## Usage

1. Set up the required environment variables (see Configuration)
2. Run the main application:

```bash
<<<<<<< HEAD
spark-submit main.py
=======
# in src directory
python3 main.python
>>>>>>> 981e598ae1bb67914406b26192f402b1bbf1f8f2
```

## Data Flow

1. **Configuration**: Environment variables are loaded
2. **Data Source Connection**: Connects to MongoDB and MySQL
3. **Transaction Processing**:
   - Loads product and customer data from MySQL
   - Processes daily transactions from MongoDB
   - Validates transactions against inventory
   - Updates inventory levels
4. **Report Generation**:
   - Generates order line items report
   - Produces order summary report
   - Creates daily business summary
5. **Forecasting**:
   - Builds time series models for sales and profits
   - Generates forecasts for future periods

## Output Files

The system generates the following output files in the configured output directory:

- `order_line_items.csv`: Detailed order items with pricing
- `products_updated.csv`: Updated product inventory levels
- `orders.csv`: Order summaries with customer information
- `daily_summary.csv`: Daily aggregated business metrics
- `sales_profit_forecast.csv`: Predicted future sales and profits

## Inventory Management

The system implements just-in-time inventory management:
- Orders are processed chronologically
- Items are canceled if inventory is insufficient
- Inventory levels are updated after each successful transaction
- Canceled items are reported with detailed messages

## Time Series Forecasting

The system provides sales and profit forecasting capabilities:
- Uses Prophet forecasting algorithms (via custom implementation)
- Includes accuracy metrics (MAE, MSE)
- Generates short-term forecasts based on historical data

## Error Handling

The system includes robust error handling:
- Graceful handling of missing data
- Detailed error reporting
- Transaction validation
- Data quality checks

## Best Practices

This project demonstrates several software development best practices:
- Strong typing with type hints
- Comprehensive error handling
- Clear separation of concerns
- Modular design
- Detailed logging and reporting

## Technical Implementation Notes

### Spark Integration

The system uses PySpark for distributed data processing, connecting to both MongoDB and MySQL data sources:

- MongoDB connector for transaction data
- JDBC connector for product and customer data
- PySpark SQL for data transformation and aggregation

### Forecasting Implementation

The `ProphetForecaster` class provides a clean interface to Facebook Prophet:

- Handles data formatting required by Prophet
- Manages date ranges automatically
- Provides performance metrics
- Abstracts away Prophet-specific implementation details

## System Requirements

- Apache Spark 3.x
- Python 3.7+
- Sufficient memory for data processing (4GB minimum recommended)
- Database connectivity to MongoDB and MySQL
- Disk space for output files
