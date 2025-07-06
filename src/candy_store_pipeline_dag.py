from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import traceback
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Dict, Tuple, List
from dotenv import load_dotenv

# Import the DataProcessor class - assuming it's in a module accessible to Airflow
from data_processor import DataProcessor

# Default arguments for our DAG
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 23),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    "candy_store_pipeline",
    default_args=default_args,
    description="Process candy store data from MongoDB to generate daily summaries and forecasts",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["candy_store", "sales", "analytics"],
)


# Helper functions (moved from main.py)
def create_spark_session(app_name: str = "CandyStoreAnalytics") -> SparkSession:
    """
    Create and configure Spark session with MongoDB and MySQL connectors
    """
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.mongodb.input.uri", os.getenv("MONGODB_URI"))
        .getOrCreate()
    )


def get_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate a list of dates between start and end date"""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    date_list = []

    current = start
    while current <= end:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return date_list


def load_config() -> Dict:
    """Load configuration from environment variables"""
    return {
        "mongodb_uri": os.getenv("MONGODB_URI"),
        "mongodb_db": os.getenv("MONGO_DB"),
        "mongodb_collection_prefix": os.getenv("MONGO_COLLECTION_PREFIX"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "mysql_db": os.getenv("MYSQL_DB"),
        "customers_table": os.getenv("CUSTOMERS_TABLE"),
        "products_table": os.getenv("PRODUCTS_TABLE"),
        "output_path": os.getenv("OUTPUT_PATH"),
        "reload_inventory_daily": os.getenv("RELOAD_INVENTORY_DAILY", "false").lower()
        == "true",
    }


def display_dataframe_preview(df: DataFrame, name: str, num_rows: int = 5) -> None:
    """Display a preview of the DataFrame with dimensions"""
    print(f"\n{name.upper()} DATA PREVIEW:")
    print("-" * 80)
    df.show(num_rows, truncate=False)
    print(f"Dimensions: {df.count()} rows x {len(df.columns)} columns")
    print("Schema:")
    df.printSchema()
    print("-" * 80)


# Task functions
def setup_environment(**context):
    """Set up the environment variables and configuration"""
    print("*" * 80)
    print("                        CANDY STORE DATA PROCESSING SYSTEM")
    print("                               Analysis Pipeline")
    print("*" * 80)

    # Load environment variables
    load_dotenv()

    # Set up configuration
    config = load_config()

    # Get date range (either from Airflow or use default from env)
    execution_date = context["execution_date"]

    # By default, process yesterday's data
    yesterday = (execution_date - timedelta(days=1)).strftime("%Y%m%d")

    # Use environment variables or override with execution date
    start_date = os.getenv("MONGO_START_DATE", yesterday).strip("'").strip('"')
    end_date = os.getenv("MONGO_END_DATE", yesterday).strip("'").strip('"')

    date_range = get_date_range(start_date, end_date)

    print("\n" + "=" * 80)
    print("PROCESSING PERIOD")
    print("-" * 80)
    print(f"Start Date: {date_range[0]}")
    print(f"End Date:   {date_range[-1]}")
    print("=" * 80)

    # Pass data to the next task
    context["ti"].xcom_push(key="config", value=config)
    context["ti"].xcom_push(key="date_range", value=date_range)

    return "Environment setup complete"


def process_daily_transactions(**context):
    """Process daily transactions from MongoDB"""
    # Get config and date_range from previous task
    config = context["ti"].xcom_pull(task_ids="setup_environment", key="config")
    date_range = context["ti"].xcom_pull(task_ids="setup_environment", key="date_range")

    print("\nINITIALIZING DATA SOURCES")
    print("-" * 80)

    # Create Spark session
    spark = create_spark_session()

    try:
        # Initialize data processor
        data_processor = DataProcessor(spark)
        data_processor.configure(config)

        # Process daily transactions
        print(f"Processing transactions for dates: {date_range}")
        order_line_items_df = data_processor.process_daily_transactions(date_range)

        # Preview the data
        display_dataframe_preview(order_line_items_df, "Processed Order Line Items")

        # Save intermediate results for the next task
        order_line_items_df.createOrReplaceTempView("order_line_items_temp")

        # Generate order summary
        orders_df = data_processor.generate_order_summary(
            order_line_items_df, date_range
        )
        display_dataframe_preview(orders_df, "Processed Order Items")

        # Save for the next task
        orders_df.createOrReplaceTempView("orders_temp")

        # Store the data processor for next task to use
        context["ti"].xcom_push(key="dp_configured", value=True)

        return "Daily transactions processed successfully"
    except Exception as e:
        print(f"\n❌ Error processing transactions: {str(e)}")
        print(traceback.format_exc())
        raise
    finally:
        # Don't stop Spark session yet as we need it for the next task
        pass


def generate_daily_summary(**context):
    """Generate daily summary and forecasts"""
    # Check if data processor was configured
    dp_configured = context["ti"].xcom_pull(
        task_ids="process_daily_transactions", key="dp_configured"
    )
    config = context["ti"].xcom_pull(task_ids="setup_environment", key="config")

    if not dp_configured:
        raise ValueError("Data processor was not properly configured in previous task")

    # Create Spark session (or reuse if possible)
    spark = create_spark_session()

    try:
        # Initialize data processor
        data_processor = DataProcessor(spark)
        data_processor.configure(config)

        # Get the dataframes from temp tables
        orders_df = spark.table("orders_temp")
        order_line_items_df = spark.table("order_line_items_temp")

        # Generate daily summary
        daily_summary_df = data_processor.daily_summary(orders_df, order_line_items_df)
        display_dataframe_preview(daily_summary_df, "Daily Summary")

        # Save daily summary
        data_processor.save_to_csv(
            daily_summary_df, config["output_path"], "daily_summary.csv"
        )

        return "Daily summary generated successfully"
    except Exception as e:
        print(f"\n❌ Error generating daily summary: {str(e)}")
        print(traceback.format_exc())
        raise
    finally:
        # Don't stop Spark session yet as we need it for the next task
        pass


def generate_forecasts(**context):
    """Generate sales and profit forecasts"""
    # Check if data processor was configured
    dp_configured = context["ti"].xcom_pull(
        task_ids="process_daily_transactions", key="dp_configured"
    )
    config = context["ti"].xcom_pull(task_ids="setup_environment", key="config")

    if not dp_configured:
        raise ValueError("Data processor was not properly configured in previous task")

    # Create Spark session (or reuse if possible)
    spark = create_spark_session()

    try:
        # Initialize data processor
        data_processor = DataProcessor(spark)
        data_processor.configure(config)

        print("\nGENERATING FORECASTS")
        print("-" * 80)

        # Attempt to generate forecasts
        try:
            forecast_df = data_processor.forecast_sales_and_profits(
                data_processor.daily_summary_df
            )

            if forecast_df is not None:
                data_processor.save_to_csv(
                    forecast_df, config["output_path"], "sales_profit_forecast.csv"
                )
                print("Forecasts generated and saved successfully.")
            else:
                print("Warning: No forecast data generated.")
        except Exception as e:
            print(f"⚠️  Warning: Could not generate forecasts: {str(e)}")
            print(traceback.format_exc())

        return "Forecast generation attempt completed"
    except Exception as e:
        print(f"\n❌ Error in forecast task: {str(e)}")
        print(traceback.format_exc())
        raise
    finally:
        # Now we can stop the Spark session
        spark.stop()


def cleanup(**context):
    """Perform any necessary cleanup operations"""
    print("\nCleaning up...")
    print("Pipeline execution completed successfully.")
    return "Cleanup completed"


# Define the tasks
setup_task = PythonOperator(
    task_id="setup_environment",
    python_callable=setup_environment,
    provide_context=True,
    dag=dag,
)

process_transactions_task = PythonOperator(
    task_id="process_daily_transactions",
    python_callable=process_daily_transactions,
    provide_context=True,
    dag=dag,
)

generate_summary_task = PythonOperator(
    task_id="generate_daily_summary",
    python_callable=generate_daily_summary,
    provide_context=True,
    dag=dag,
)

generate_forecasts_task = PythonOperator(
    task_id="generate_forecasts",
    python_callable=generate_forecasts,
    provide_context=True,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id="cleanup",
    python_callable=cleanup,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
(
    setup_task
    >> process_transactions_task
    >> generate_summary_task
    >> generate_forecasts_task
    >> cleanup_task
)

# Enable CLI for testing
if __name__ == "__main__":
    dag.cli()
