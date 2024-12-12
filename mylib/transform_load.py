import logging
from pyspark.sql import SparkSession

# Constants for paths
DATASET_PATH = "dbfs:/FileStore/skim_project11/titanic.csv"
OUTPUT_PATH = "dbfs:/FileStore/skim_project11"

def create_spark(app_name="ChessTransfersPipeline"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data(spark, dataset=DATASET_PATH):
    logging.info(f"Loading data from: {dataset}")
    df = spark.read.csv(dataset, header=True, inferSchema=True)
    print("Data loaded successfully:")
    df.show()
    return df

def transform_data(df):
    """Perform transformations on the data."""
    logging.info("Transforming data...")

    # Rename columns
    transformed_df = (
        df.withColumnRenamed("ID", "player_id")
          .withColumnRenamed("Federation", "federation")
          .withColumnRenamed("Form.Fed", "former_fed")
          .withColumnRenamed("Transfer Date", "transfer_date")
    )

    # Fill missing values
    transformed_df = transformed_df.fillna({"former_fed": "UNKNOWN"})
    print("Data transformation complete:")
    transformed_df.show()
    return transformed_df

def save_data(df, path=OUTPUT_PATH):
    temp_path = f"{path}_temp"
    final_file = f"{path}/transformed_transfer.csv"

    # Coalesce to a single file
    df.coalesce(1).write.mode("overwrite").csv(temp_path, header=True)

    # Move the single file to the final location
    files = dbutils.fs.ls(temp_path)
    for file in files:
        if file.path.endswith(".csv"):
            dbutils.fs.mv(file.path, final_file)
            break

    # Clean up temporary directory
    dbutils.fs.rm(temp_path, recurse=True)
    print(f"Transformed data saved as: {final_file}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark = create_spark()
    raw_df = load_data(spark)
    transformed_df = transform_data(raw_df)
    save_data(transformed_df)