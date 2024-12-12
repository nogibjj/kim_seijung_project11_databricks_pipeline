from mylib.log import init_log
from mylib.transform_load import create_spark, load_data, transform_data, save_data
from mylib.query import query_data


def main():
    # Initialize logging
    logger = init_log()
    logger.info("Starting Spark Session...")

    # Create Spark session
    logger.info("Initializing Spark session...")
    spark = create_spark("TitanicPipeline")

    # Load data
    logger.info("Loading data...")
    data = load_data(spark)

    # Transform data
    logger.info("Transforming data...")
    transformed_data = transform_data(data)

    # Save transformed data
    logger.info("Saving transformed data...")
    save_data(transformed_data)

    # Perform CRUD operations and queries
    logger.info("Performing CRUD operations and queries...")
    query_data()

    # Stop Spark session
    logger.info("Stopping Spark session...")
    spark.stop()
    logger.info("Spark session completed!")


if __name__ == "__main__":
    main()
