import os
import sys
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

# Hadoop helper directory for Windows
#HADOOP_HOME = Path("G:/hadoop")

# winutils.exe should be inside:
# G:/hadoop/bin/winutils.exe


# ============================================================
# DATASETS
# ============================================================

DATASETS = [
    "suppliers",
    "products",
    "warehouses",
    "customers",
    "orders",
    "inventory",
    "shipments",
]


# ============================================================
# WINDOWS HADOOP CONFIGURATION
# ============================================================


def configure_hadoop():

    """
    Configure Hadoop environment variables for
    running PySpark locally on Windows.
    """

    hadoop_bin = HADOOP_HOME / "bin"

    if not HADOOP_HOME.exists():

        print(
            "\n[ERROR] Hadoop directory not found:"
        )

        print(
            f"        {HADOOP_HOME}"
        )

        print(
            "\nCreate the Hadoop directory and "
            "install a compatible winutils.exe."
        )

        return False

    if not hadoop_bin.exists():

        print(
            "\n[ERROR] Hadoop bin directory not found:"
        )

        print(
            f"        {hadoop_bin}"
        )

        return False

    winutils = hadoop_bin / "winutils.exe"

    if not winutils.exists():

        print(
            "\n[ERROR] winutils.exe not found:"
        )

        print(
            f"        {winutils}"
        )

        return False

    # Set environment variables
    os.environ["HADOOP_HOME"] = str(HADOOP_HOME)

    os.environ["hadoop.home.dir"] = str(
        HADOOP_HOME
    )

    # Add Hadoop bin to PATH
    os.environ["PATH"] = (
        str(hadoop_bin)
        + os.pathsep
        + os.environ.get("PATH", "")
    )

    print(
        "\n[INFO] Hadoop configuration:"
    )

    print(
        f"       HADOOP_HOME = {HADOOP_HOME}"
    )

    print(
        f"       winutils   = {winutils}"
    )

    return True


# ============================================================
# SPARK SESSION
# ============================================================


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(
            "SupplyChain-Bronze-Ingestion"
        )
        .master("local[*]")
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy",
        )
        .config(
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.LocalFileSystem",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    return spark


# ============================================================
# INGEST DATASET
# ============================================================


def ingest_dataset(
    spark,
    dataset_name,
):

    input_file = (
        RAW_DIR /
        f"{dataset_name}.csv"
    )

    output_directory = (
        BRONZE_DIR /
        dataset_name
    )

    if not input_file.exists():

        print(
            f"[ERROR] Input file not found: "
            f"{input_file}"
        )

        return False

    print("\n" + "-" * 70)

    print(
        f"Processing dataset: "
        f"{dataset_name}"
    )

    print("-" * 70)

    print(
        f"Input : {input_file}"
    )

    print(
        f"Output: {output_directory}"
    )

    # --------------------------------------------------------
    # Read CSV
    # --------------------------------------------------------

    df = (
        spark.read
        .option(
            "header",
            True
        )
        .option(
            "inferSchema",
            True
        )
        .csv(
            str(input_file)
        )
    )

    source_record_count = df.count()

    print(
        f"Source records: "
        f"{source_record_count:,}"
    )

    # --------------------------------------------------------
    # Bronze metadata
    # --------------------------------------------------------

    ingestion_time = (
        datetime.now()
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    df = (
        df
        .withColumn(
            "_source_file",
            input_file_name(),
        )
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp(),
        )
        .withColumn(
            "_ingestion_date",
            lit(
                ingestion_time[:10]
            ),
        )
    )

    # --------------------------------------------------------
    # Write Parquet
    # --------------------------------------------------------

    try:

        (
            df.write
            .mode("overwrite")
            .parquet(
                str(output_directory)
            )
        )

    except Exception as e:

        print(
            "\n[ERROR] Failed to write "
            f"{dataset_name} to Bronze."
        )

        print(
            f"\n{e}"
        )

        return False

    # --------------------------------------------------------
    # Verify
    # --------------------------------------------------------

    try:

        verification_df = (
            spark.read
            .parquet(
                str(output_directory)
            )
        )

        bronze_count = (
            verification_df.count()
        )

    except Exception as e:

        print(
            "\n[ERROR] Failed to verify "
            f"{dataset_name}."
        )

        print(
            f"\n{e}"
        )

        return False

    print(
        f"Bronze records: "
        f"{bronze_count:,}"
    )

    if (
        source_record_count
        != bronze_count
    ):

        print(
            "[WARNING] Record count "
            "mismatch!"
        )

        return False

    print(
        f"[SUCCESS] {dataset_name} "
        "ingested successfully."
    )

    return True


# ============================================================
# MAIN
# ============================================================


def main():

    print("=" * 70)

    print(
        "SUPPLY CHAIN DATA PLATFORM"
    )

    print(
        "BRONZE LAYER INGESTION"
    )

    print("=" * 70)

    # --------------------------------------------------------
    # Configure Hadoop
    # --------------------------------------------------------

    

    # --------------------------------------------------------
    # Paths
    # --------------------------------------------------------

    print(
        f"\nRaw directory:\n{RAW_DIR}"
    )

    print(
        f"\nBronze directory:\n{BRONZE_DIR}"
    )

    BRONZE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # Create Spark
    # --------------------------------------------------------

    spark = (
        create_spark_session()
    )

    successful = 0
    failed = 0

    try:

        for dataset in DATASETS:

            result = ingest_dataset(
                spark,
                dataset,
            )

            if result:

                successful += 1

            else:

                failed += 1

    finally:

        spark.stop()

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "BRONZE INGESTION SUMMARY"
    )

    print(
        "=" * 70
    )

    print(
        f"Successful datasets : "
        f"{successful}"
    )

    print(
        f"Failed datasets     : "
        f"{failed}"
    )

    print(
        f"Total datasets      : "
        f"{len(DATASETS)}"
    )

    if failed > 0:

        print(
            "\n[ERROR] Bronze ingestion "
            "completed with failures."
        )

        sys.exit(1)

    print(
        "\n[SUCCESS] All datasets "
        "successfully ingested."
    )


# ============================================================
# ENTRY POINT
# ============================================================


if __name__ == "__main__":

    main()