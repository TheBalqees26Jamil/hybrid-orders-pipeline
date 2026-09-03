import sys
import time
import uuid
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import (
    SPARK_MASTER,
    SPARK_APP_NAME,
    SPARK_MONGO_CONNECTOR_PACKAGE,
    MONGO_URI,
    MONGO_DB_NAME,
    COLLECTION_RAW,
    RAW_CSV_COLUMNS,
)


def _get_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.jars.packages", SPARK_MONGO_CONNECTOR_PACKAGE)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )
    return spark


def _build_fixed_schema():

    from pyspark.sql.types import StructType, StructField, StringType

    return StructType([StructField(col, StringType(), True) for col in RAW_CSV_COLUMNS])


def load_pyspark(input_path: str, id_run: str = None) -> dict:

    from pyspark.sql import functions as F

    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"File not found: {input_path}")

    if id_run is None:
        id_run = str(uuid.uuid4())

    engine_used = "pyspark"
    file_source = str(input_file)

    print("=" * 60)
    print(f"PySpark Loader - id_run = {id_run}")
    print(f"File: {file_source}")
    print("=" * 60)

    start_time = time.time()
    spark = _get_spark_session()

    try:
        schema = _build_fixed_schema()

        
        df = (
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .option("quote", "\"")
            .option("escape", "\"")
            .option("multiLine", "true")
            .schema(schema)
            .csv(str(input_file))
        )

        input_partitions = df.rdd.getNumPartitions()
        read_rows = df.count()

        print(f"Input partitions : {input_partitions}")
        print(f"Rows read         : {read_rows}")

       
        indexed_df = df.withColumn(
            "number_row_source", F.monotonically_increasing_id() + F.lit(1)
        )

        raw_struct_cols = [F.col(c) for c in RAW_CSV_COLUMNS]

        enriched_df = (
            indexed_df
            .withColumn("record_raw", F.struct(*raw_struct_cols))
            .withColumn("id_run", F.lit(id_run))
            .withColumn("file_source", F.lit(file_source))
            .withColumn("engine_used", F.lit(engine_used))
            .withColumn("at_ingested", F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssXXX"))
            .select(
                "id_run", "file_source", "number_row_source",
                "at_ingested", "engine_used", "record_raw",
            )
        )

        print("-" * 60)
        print("Writing to MongoDB via Spark MongoDB Connector ...")

        (
            enriched_df.write
            .format("mongodb")
            .mode("append")
            .option("database", MONGO_DB_NAME)
            .option("collection", COLLECTION_RAW)
            .save()
        )

        elapsed = time.time() - start_time
        throughput = round(read_rows / elapsed, 2) if elapsed > 0 else 0.0

        metrics = {
            "id_run": id_run,
            "used_engine": engine_used,
            "file_name": input_file.name,
            "read_rows": read_rows,
            "loaded_raw": read_rows,
            "input_partitions": input_partitions,
            "seconds_elapsed": round(elapsed, 3),
            "throughput": throughput,
        }

        print("-" * 60)
        print("PySpark Loader finished:")
        for k, v in metrics.items():
            print(f"  {k}: {v}")
        print("=" * 60)

        return metrics

    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Standalone test for the PySpark engine.")
    parser.add_argument("--input", type=str, required=True, help="Path to the CSV file")
    args = parser.parse_args()

    load_pyspark(args.input)