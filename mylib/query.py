import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Constants for paths
INPUT_PATH = "dbfs:/FileStore/skim_project11/titanic_transformed.csv"
OUTPUT_PATH = "dbfs:/FileStore/skim_project11"
FINAL_FILE = "dbfs:/FileStore/skim_project11/summary.csv"

def create_spark(app_name="TitanicCRUD"):
    """Initialize the Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data(spark, input_path=INPUT_PATH):
    """Load the transformed data."""
    logging.info(f"Loading data from: {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    print("Data loaded successfully:")
    df.show()
    return df

def create_record(df):
    """Create a new record in the DataFrame."""
    logging.info("Adding a new passenger record...")
    new_record = {
        "PassengerId": 892,
        "Survived": 1,
        "Pclass": 2,
        "Name": "Doe, John",
        "Sex": "male",
        "Age": 29.0,
        "SibSp": 0,
        "Parch": 0,
        "Ticket": "A123456",
        "Fare": 15.75,
        "Cabin": "B50",
        "Embarked": "S"
    }
    new_row = spark.createDataFrame([new_record])
    updated_df = df.union(new_row)
    updated_df.show()
    return updated_df

def read_records(df, num_records=5):
    """Display a specified number of records."""
    logging.info(f"Reading the first {num_records} records...")
    df.show(num_records)
    return df

def update_record(df, passenger_id, new_age):
    """Update a record in the DataFrame."""
    logging.info(f"Updating passenger with ID {passenger_id}...")
    updated_df = df.withColumn(
        "Age",
        lit(new_age).where(df["PassengerId"] == passenger_id).otherwise(df["Age"])
    )
    updated_df.show()
    return updated_df

def delete_record(df, passenger_id):
    """Delete a record from the DataFrame."""
    logging.info(f"Deleting passenger with ID {passenger_id}...")
    updated_df = df.filter(df["PassengerId"] != passenger_id)
    updated_df.show()
    return updated_df

def save_data(df, path=OUTPUT_PATH):
    """Save the updated DataFrame."""
    temp_path = f"{path}_temp"
    final_file = f"{path}/titanic_crud.csv"

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
    print(f"Updated data saved as: {final_file}")

def query_data():
    """Perform CRUD operations on the Titanic dataset."""
    spark = create_spark()

    # Load data
    df = load_data(spark)

    # Perform CRUD operations
    df = create_record(df)  # Add a new record
    read_records(df)  # Read a subset of records
    df = update_record(df, passenger_id=1, new_age=30)  # Update a record
    df = delete_record(df, passenger_id=2)  # Delete a record

    # Save the final DataFrame
    save_data(df)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    query_data()
