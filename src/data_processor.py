from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode,
    col,
    round as spark_round,
    sum as spark_sum,
    count,
    abs as spark_abs,
    when,
)
from typing import Dict, Tuple
import os
import glob
import shutil
import decimal
import numpy as np
from time_series import ProphetForecaster
from datetime import datetime, timedelta
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    TimestampType,
    DecimalType,
    StringType,
    DoubleType,
)


class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Initialize all class properties
        self.config = None
        self.current_inventory = None
        self.inventory_initialized = False
        self.original_products_df = None  # Store original products data
        self.reload_inventory_daily = False  # New flag for inventory reload
        self.order_items = None
        self.products_df = None
        self.customers_df = None
        self.transactions_df = None
        self.orders_df = None
        self.order_line_items_df = None
        self.daily_summary_df = None
        self.total_cancelled_items = 0

    def configure(self, config: Dict) -> None:
        """Configure the data processor with environment settings"""
        self.config = config
        self.reload_inventory_daily = config.get("reload_inventory_daily", False)
        print("\nINITIALIZING DATA SOURCES")
        print("-" * 80)
        if self.reload_inventory_daily:
            print("Daily inventory reload: ENABLED")
        else:
            print("Daily inventory reload: DISABLED")

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:

        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)

    def load_mysql_data(self, spark: SparkSession, table_name: str) -> DataFrame:

        print(f"\nLoading MySQL data from table: {table_name}")

        df = (
            spark.read.format("jdbc")
            .option("url", self.config["mysql_url"])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", table_name)
            .option("user", self.config["mysql_user"])
            .option("password", self.config["mysql_password"])
            .load()
        )

        return df

    def load_mongodb_data(self, date: str) -> DataFrame:

        collection_name = f"{self.config['mongodb_collection_prefix']}{date}"
        print(f"\nLoading MongoDB data from collection: {collection_name}")

        # Clean the database name by removing any comments and spaces
        db_name = self.config["mongodb_db"].split("#")[0].strip()

        # Debug information
        print(f"Database name: '{db_name}'")
        print(f"Collection name: '{collection_name}'")

        # Load transaction data from MongoDB
        transaction_info = (
            self.spark.read.format("mongo")
            .option("uri", f"{self.config['mongodb_uri']}/{db_name}.{collection_name}")
            .load()
        )

        # Explode items array to get individual transaction items
        transaction_items = transaction_info.withColumn(
            "item", explode("items")
        ).select(
            col("transaction_id"),
            col("customer_id"),
            col("timestamp"),
            col("item.product_id"),
            col("item.product_name"),
            col("item.qty"),
        )

        return transaction_items

    def process_daily_transactions(self, date_range: list[str]) -> DataFrame:

        print("\nPROCESSING DAILY TRANSACTIONS")
        print("-" * 80)

        # Initialize variables
        all_processed_transactions = []
        self.total_cancelled_items = 0

        # Load product and customer data
        self.products_df = self.load_mysql_data(
            self.spark, self.config["products_table"]
        )
        self.customers_df = self.load_mysql_data(
            self.spark, self.config["customers_table"]
        )

        # Create inventory DataFrame
        inventory_df = self.products_df.select(
            "product_id", "product_name", col("stock").alias("current_stock")
        )

        print("\nINITIAL INVENTORY LEVELS (SAMPLE):")
        inventory_df.orderBy("product_id").show(truncate=False)

        schema = StructType(
            [
                StructField("order_id", IntegerType(), False),
                StructField("product_id", IntegerType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("unit_price", DoubleType(), False),
                StructField("line_total", DoubleType(), False),
            ]
        )

        # Process each day's transactions
        for date in date_range:
            print(f"\nProcessing transactions for date: {date}")

            # Load transactions for this date
            daily_transactions = self.load_mongodb_data(date)

            # Filter out items with null quantities
            filtered_transactions = daily_transactions.filter(col("qty").isNotNull())

            # Join with products table to get pricing information
            order_items = filtered_transactions.join(
                self.products_df.select("product_id", "sales_price"),
                on="product_id",
                how="inner",
            )

            # Collect transactions to process and check inventory
            transactions_to_process = []

            # Convert inventory_df to a dictionary for easier lookup
            inventory_dict = {
                row["product_id"]: row["current_stock"]
                for row in inventory_df.collect()
            }

            # Check inventory and mark items that need to be cancelled
            for row in order_items.collect():
                product_id = row["product_id"]
                qty = row["qty"]
                sales_price = row["sales_price"]

                # Check if we have enough inventory
                if product_id in inventory_dict and inventory_dict[product_id] >= qty:
                    # Sufficient inventory, process normally
                    inventory_dict[product_id] -= qty

                    # Create processed item with full quantity
                    processed_item = {
                        "order_id": int(row["transaction_id"]),
                        "product_id": int(product_id),
                        "quantity": int(qty),
                        "unit_price": float(sales_price),
                        "line_total": float(round(qty * sales_price, 2)),
                    }
                else:
                    # Insufficient inventory, cancel the item
                    product_name = row["product_name"]
                    avail_stock = inventory_dict.get(product_id, 0)

                    # Print cancellation message
                    print(
                        f"❌ Cancelled: Order {row['transaction_id']} - {product_name} - "
                        f"Requested: {qty}, Available: {avail_stock}"
                    )

                    # Create cancelled item (quantity and line_total set to 0)
                    processed_item = {
                        "order_id": int(row["transaction_id"]),
                        "product_id": int(product_id),
                        "quantity": 0,
                        "unit_price": float(sales_price),
                        "line_total": 0.0,
                    }
                    self.total_cancelled_items += 1

                transactions_to_process.append(processed_item)

            # Update inventory_df with new stock levels from the dictionary
            inventory_updates = []
            for pid, stock in inventory_dict.items():
                inventory_updates.append((int(pid), int(stock)))  # Ensure correct types

            if inventory_updates:
                updated_inventory = self.spark.createDataFrame(
                    inventory_updates, ["product_id", "updated_stock"]
                )

                # Join and update inventory DataFrame
                inventory_df = (
                    inventory_df.join(updated_inventory, on="product_id", how="left")
                    .withColumn(
                        "current_stock",
                        when(
                            col("updated_stock").isNotNull(), col("updated_stock")
                        ).otherwise(col("current_stock")),
                    )
                    .drop("updated_stock")
                )

            # Add processed transactions to the collection
            all_processed_transactions.extend(transactions_to_process)

        # Create  DataFrame from all processed transactions with explicit schema
        result_df = self.spark.createDataFrame(
            all_processed_transactions, schema
        ).orderBy("order_id", "product_id")
        orders_updated_df = inventory_df.orderBy("product_id")

        # Save the result to a CSV file
        output_path = self.config["output_path"]
        order_line_items = "order_line_items.csv"
        output_products = "products_updated.csv"

        # Save order_line_items
        self.save_to_csv(result_df, output_path, order_line_items)
        print(f"Saved processed order items to {output_path}/{order_line_items}")

        # Save products_updated
        self.save_to_csv(orders_updated_df, output_path, output_products)
        print(f"Saved Updated Product items to {output_path}/{output_products}")

        # Show sample of final inventory levels
        print("\nFINAL INVENTORY LEVELS (SAMPLE):")
        orders_updated_df.show(5, truncate=False)

        return result_df

    def generate_order_summary(
        self, order_items_df: DataFrame, date_range: list[str]
    ) -> DataFrame:

        print("\nGENERATING ORDER SUMMARY")
        print("-" * 80)

        if order_items_df is None or order_items_df.count() == 0:
            print("No order items available to generate summary")
            return None

        # Aggregate line_total and quantity for each order

        order_summary = order_items_df.groupBy("order_id").agg(
            spark_round(spark_sum("line_total"), 2).alias("total_amount"),
            count("*").alias("num_items"),
        )

        # Load all transaction data for the date range to get order_datetime and customer_id
        all_transactions = None

        for date in date_range:
            # Load transactions for this date
            daily_transactions = self.load_mongodb_data(date)

            if daily_transactions.count() > 0:
                # Select only the needed columns and rename them
                transaction_info = daily_transactions.select(
                    col("transaction_id").alias("order_id"),
                    col("timestamp").alias("order_datetime"),
                    "customer_id",
                ).dropDuplicates(["order_id"])

                # Union with previous days' transactions
                if all_transactions is None:
                    all_transactions = transaction_info
                else:
                    all_transactions = all_transactions.union(transaction_info)

        if all_transactions is None:
            print("No transaction data found for the date range")
            return None

        # Join order summary with transaction info to get order_datetime and customer_id
        # Cast order_id to string to ensure proper join
        order_summary = order_summary.withColumn(
            "order_id", col("order_id").cast("string")
        )
        all_transactions = all_transactions.withColumn(
            "order_id", col("order_id").cast("string")
        )

        order_summary_with_info = order_summary.join(
            all_transactions, on="order_id", how="inner"
        )

        # Select and order columns
        result_df = order_summary_with_info.select(
            col("order_id").cast("int").alias("order_id"),
            "order_datetime",
            "customer_id",
            "total_amount",
            "num_items",
        ).orderBy(col("order_id").cast("int"))

        # Save the result to a CSV file
        output_path = self.config["output_path"]
        output_filename = "orders.csv"

        self.save_to_csv(result_df, output_path, output_filename)
        print(f"Saved order summary to {output_path}/{output_filename}")

        return result_df

    def daily_summary(
        self, orders_df: DataFrame, order_items_df: DataFrame
    ) -> DataFrame:

        print("\nGENERATING DAILY SUMMARY")
        print("-" * 80)

        if orders_df is None or orders_df.count() == 0:
            print("No orders available to generate daily summary")
            return None

        # Import necessary functions
        from pyspark.sql.functions import (
            col,
            to_date,
            count,
            sum as spark_sum,
            round as spark_round,
            date_format,
        )

        # Extract date from timestamp and group by date to calculate sales and order count
        daily_orders = (
            orders_df.withColumn("date", to_date(col("order_datetime")))
            .groupBy("date")
            .agg(
                count("order_id").alias("num_orders"),
                spark_round(spark_sum("total_amount"), 2).alias("total_sales"),
            )
        )

        # Calculate profit by joining order_items with products to get cost_to_make
        if order_items_df is None or order_items_df.count() == 0:
            print("No order items available to calculate profit")
            return None

        # Join order items with products to get cost information
        items_with_cost = order_items_df.join(
            self.products_df.select("product_id", "cost_to_make"),
            on="product_id",
            how="inner",
        )

        # Calculate profit for each line item
        profit_items = items_with_cost.withColumn(
            "line_profit", col("line_total") - (col("quantity") * col("cost_to_make"))
        )

        # Join with orders to get the date
        profit_items_with_date = profit_items.join(
            orders_df.select("order_id", "order_datetime"), on="order_id", how="inner"
        ).withColumn("date", to_date(col("order_datetime")))

        # Group by date to calculate total profit
        daily_profit = profit_items_with_date.groupBy("date").agg(
            spark_round(spark_sum("line_profit"), 2).alias("total_profit")
        )

        # Join daily orders and profit information
        daily_summary_df = daily_orders.join(
            daily_profit, on="date", how="inner"
        ).orderBy("date")

        final_summary_df = daily_summary_df.withColumn(
            "date", date_format(col("date"), "yyyy-M-dd")
        )

        # Save the daily summary to CSV
        output_path = self.config["output_path"]
        output_filename = "daily_summary.csv"

        self.save_to_csv(final_summary_df, output_path, output_filename)
        print(f"Saved daily summary to {output_path}/{output_filename}")

        self.daily_summary_df = daily_summary_df

        return final_summary_df

    def finalize_processing(self) -> None:
        """Finalize processing and create summary"""
        print("\nPROCESSING COMPLETE")
        print("=" * 80)
        print(f"Total Cancelled Items: {self.total_cancelled_items}")

    # ------------------------------------------------------------------------------------------------
    # Try not to change the logic of the time series forecasting model
    # DO NOT change functions with prefix _
    # ------------------------------------------------------------------------------------------------
    def forecast_sales_and_profits(
        self, daily_summary_df: DataFrame, forecast_days: int = 1
    ) -> DataFrame:
        """
        Main forecasting function that coordinates the forecasting process
        """
        try:
            # Build model
            model_data = self.build_time_series_model(daily_summary_df)

            # Calculate accuracy metrics
            metrics = self.calculate_forecast_metrics(model_data)

            # Generate forecasts
            forecast_df = self.make_forecasts(model_data, forecast_days)

            return forecast_df

        except Exception as e:
            print(
                f"Error in forecast_sales_and_profits: {str(e)}, please check the data"
            )
            return None

    def print_inventory_levels(self) -> None:
        """Print current inventory levels for all products"""
        print("\nCURRENT INVENTORY LEVELS")
        print("-" * 40)

        inventory_data = self.current_inventory.orderBy("product_id").collect()
        for row in inventory_data:
            print(
                f"• {row['product_name']:<30} (ID: {row['product_id']:>3}): {row['current_stock']:>4} units"
            )
        print("-" * 40)

    def build_time_series_model(self, daily_summary_df: DataFrame) -> dict:
        """Build Prophet models for sales and profits"""
        print("\n" + "=" * 80)
        print("TIME SERIES MODEL CONSTRUCTION")
        print("-" * 80)

        model_data = self._prepare_time_series_data(daily_summary_df)
        return self._fit_forecasting_models(model_data)

    def calculate_forecast_metrics(self, model_data: dict) -> dict:
        """Calculate forecast accuracy metrics for both models"""
        print("\nCalculating forecast accuracy metrics...")

        # Get metrics from each model
        sales_metrics = model_data["sales_model"].get_metrics()
        profit_metrics = model_data["profit_model"].get_metrics()

        metrics = {
            "sales_mae": sales_metrics["mae"],
            "sales_mse": sales_metrics["mse"],
            "profit_mae": profit_metrics["mae"],
            "profit_mse": profit_metrics["mse"],
        }

        # Print metrics and model types
        print("\nForecast Error Metrics:")
        print(f"Sales Model Type: {sales_metrics['model_type']}")
        print(f"Sales MAE: ${metrics['sales_mae']:.2f}")
        print(f"Sales MSE: ${metrics['sales_mse']:.2f}")
        print(f"Profit Model Type: {profit_metrics['model_type']}")
        print(f"Profit MAE: ${metrics['profit_mae']:.2f}")
        print(f"Profit MSE: ${metrics['profit_mse']:.2f}")

        return metrics

    def make_forecasts(self, model_data: dict, forecast_days: int = 7) -> DataFrame:
        """Generate forecasts using Prophet models"""
        print(f"\nGenerating {forecast_days}-day forecast...")

        forecasts = self._generate_model_forecasts(model_data, forecast_days)
        forecast_dates = self._generate_forecast_dates(
            model_data["training_data"]["dates"][-1], forecast_days
        )

        return self._create_forecast_dataframe(forecast_dates, forecasts)

    def _prepare_time_series_data(self, daily_summary_df: DataFrame) -> dict:
        """Prepare data for time series modeling"""
        data = (
            daily_summary_df.select("date", "total_sales", "total_profit")
            .orderBy("date")
            .collect()
        )

        dates = np.array([row["date"] for row in data])
        sales_series = np.array([float(row["total_sales"]) for row in data])
        profit_series = np.array([float(row["total_profit"]) for row in data])

        self._print_dataset_info(dates, sales_series, profit_series)

        return {"dates": dates, "sales": sales_series, "profits": profit_series}

    def _print_dataset_info(
        self, dates: np.ndarray, sales: np.ndarray, profits: np.ndarray
    ) -> None:
        """Print time series dataset information"""
        print("Dataset Information:")
        print(f"• Time Period:          {dates[0]} to {dates[-1]}")
        print(f"• Number of Data Points: {len(dates)}")
        print(f"• Average Daily Sales:   ${np.mean(sales):.2f}")
        print(f"• Average Daily Profit:  ${np.mean(profits):.2f}")

    def _fit_forecasting_models(self, data: dict) -> dict:
        """Fit Prophet models to the prepared data"""
        print("\nFitting Models...")
        sales_forecaster = ProphetForecaster()
        profit_forecaster = ProphetForecaster()

        sales_forecaster.fit(data["sales"])
        profit_forecaster.fit(data["profits"])
        print("Model fitting completed successfully")
        print("=" * 80)

        return {
            "sales_model": sales_forecaster,
            "profit_model": profit_forecaster,
            "training_data": data,
        }

    def _generate_model_forecasts(self, model_data: dict, forecast_days: int) -> dict:
        """Generate forecasts from both models"""
        return {
            "sales": model_data["sales_model"].predict(forecast_days),
            "profits": model_data["profit_model"].predict(forecast_days),
        }

    def _generate_forecast_dates(self, last_date: datetime, forecast_days: int) -> list:
        """Generate dates for the forecast period"""
        return [last_date + timedelta(days=i + 1) for i in range(forecast_days)]

    def _create_forecast_dataframe(self, dates: list, forecasts: dict) -> DataFrame:
        """Create Spark DataFrame from forecast data"""
        forecast_rows = [
            (date, float(sales), float(profits))
            for date, sales, profits in zip(
                dates, forecasts["sales"], forecasts["profits"]
            )
        ]

        return self.spark.createDataFrame(
            forecast_rows, ["date", "forecasted_sales", "forecasted_profit"]
        )
