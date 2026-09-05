from pathlib import Path
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    when,
    regexp_replace,
    to_timestamp,
    to_date,
    current_timestamp,
    lit,
    row_number,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
)


SPARK_TEMP_DIR = r"G:\spark-temp"

os.makedirs(SPARK_TEMP_DIR, exist_ok=True)

os.environ["TEMP"] = SPARK_TEMP_DIR
os.environ["TMP"] = SPARK_TEMP_DIR
# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("SupplyChain-Bronze-Silver")
        .master("local[*]")

        # ====================================================
        # WINDOWS + SPARK TEMP DIRECTORY
        # Keep Spark temporary files on G: instead of C:
        # ====================================================

        .config(
            "spark.local.dir",
            "G:/spark-temp"
        )

        .config(
            "spark.sql.warehouse.dir",
            "G:/spark-temp/warehouse"
        )

        # ====================================================
        # LOCAL FILESYSTEM
        # ====================================================

        .config(
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.LocalFileSystem"
        )

        .config(
            "spark.hadoop.fs.permissions.umask-mode",
            "000"
        )

        # ====================================================
        # PARQUET
        # ====================================================

        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print()
    print("=" * 70)
    print("SPARK SESSION CREATED")
    print("=" * 70)
    print(f"Spark Version : {spark.version}")
    print(f"Spark Temp    : {spark.conf.get('spark.local.dir')}")
    print("=" * 70)

    return spark

# ============================================================
# GENERIC HELPERS
# ============================================================

def read_bronze(spark, dataset):

    path = BRONZE_DIR / dataset

    print(f"\nReading Bronze: {path}")

    return spark.read.parquet(str(path))


def write_silver(df, dataset):

    output_path = SILVER_DIR / dataset

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(
        f"[SUCCESS] Silver written: {output_path}"
    )


def add_processing_metadata(df):

    return (
        df
        .withColumn(
            "_silver_processed_timestamp",
            current_timestamp(),
        )
        .withColumn(
            "_data_layer",
            lit("silver"),
        )
    )


# ============================================================
# SUPPLIERS
# ============================================================

def transform_suppliers(spark):

    print("\n" + "=" * 70)
    print("TRANSFORMING SUPPLIERS")
    print("=" * 70)

    df = read_bronze(
        spark,
        "suppliers",
    )

    print(
        f"Bronze records: {df.count():,}"
    )

    # --------------------------------------------------------
    # Clean strings
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "supplier_id",
            trim(col("supplier_id")),
        )
        .withColumn(
            "supplier_name",
            trim(col("supplier_name")),
        )
        .withColumn(
            "country",
            trim(col("country")),
        )
        .withColumn(
            "contact_email",
            lower(trim(col("contact_email"))),
        )
        .withColumn(
            "status",
            upper(trim(col("status"))),
        )
    )

    # --------------------------------------------------------
    # Cast numeric columns
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "rating",
            col("rating").cast(DoubleType()),
        )
        .withColumn(
            "lead_time_days",
            col("lead_time_days").cast(
                IntegerType()
            ),
        )
    )

    # --------------------------------------------------------
    # Validate rating
    # --------------------------------------------------------

    df = df.withColumn(
        "rating",
        when(
            (col("rating") >= 0)
            & (col("rating") <= 5),
            col("rating"),
        ).otherwise(None),
    )

    # --------------------------------------------------------
    # Remove records without supplier ID
    # --------------------------------------------------------

    df = df.filter(
        col("supplier_id").isNotNull()
        & (col("supplier_id") != "")
    )

    # --------------------------------------------------------
    # Remove duplicate suppliers
    # --------------------------------------------------------

    df = df.dropDuplicates(
        ["supplier_id"]
    )

    # --------------------------------------------------------
    # Metadata
    # --------------------------------------------------------

    df = add_processing_metadata(df)

    write_silver(
        df,
        "suppliers",
    )

    print(
        f"Silver records: {df.count():,}"
    )


# ============================================================
# PRODUCTS
# ============================================================

def transform_products(spark):

    print("\n" + "=" * 70)
    print("TRANSFORMING PRODUCTS")
    print("=" * 70)

    df = read_bronze(
        spark,
        "products",
    )

    print(
        f"Bronze records: {df.count():,}"
    )

    # --------------------------------------------------------
    # Clean strings
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "product_id",
            trim(col("product_id")),
        )
        .withColumn(
            "product_name",
            trim(col("product_name")),
        )
        .withColumn(
            "category",
            trim(col("category")),
        )
        .withColumn(
            "supplier_id",
            trim(col("supplier_id")),
        )
        .withColumn(
            "status",
            upper(trim(col("status"))),
        )
    )

    # --------------------------------------------------------
    # Cast numeric columns
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "unit_cost",
            col("unit_cost").cast(
                DoubleType()
            ),
        )
        .withColumn(
            "selling_price",
            col("selling_price").cast(
                DoubleType()
            ),
        )
        .withColumn(
            "weight_kg",
            col("weight_kg").cast(
                DoubleType()
            ),
        )
        .withColumn(
            "reorder_level",
            col("reorder_level").cast(
                IntegerType()
            ),
        )
    )

    # --------------------------------------------------------
    # Validate prices
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "unit_cost",
            when(
                col("unit_cost") >= 0,
                col("unit_cost"),
            ).otherwise(None),
        )
        .withColumn(
            "selling_price",
            when(
                col("selling_price") >= 0,
                col("selling_price"),
            ).otherwise(None),
        )
    )

    # --------------------------------------------------------
    # Remove invalid IDs
    # --------------------------------------------------------

    df = df.filter(
        col("product_id").isNotNull()
        & (col("product_id") != "")
    )

    # --------------------------------------------------------
    # Remove duplicates
    # --------------------------------------------------------

    df = df.dropDuplicates(
        ["product_id"]
    )

    # --------------------------------------------------------
    # Metadata
    # --------------------------------------------------------

    df = add_processing_metadata(df)

    write_silver(
        df,
        "products",
    )

    print(
        f"Silver records: {df.count():,}"
    )


# ============================================================
# WAREHOUSES
# ============================================================

def transform_warehouses(spark):

    print("\n" + "=" * 70)
    print("TRANSFORMING WAREHOUSES")
    print("=" * 70)

    df = read_bronze(
        spark,
        "warehouses",
    )

    df = (
        df
        .withColumn(
            "warehouse_id",
            trim(col("warehouse_id")),
        )
        .withColumn(
            "warehouse_name",
            trim(col("warehouse_name")),
        )
        .withColumn(
            "warehouse_type",
            upper(trim(col("warehouse_type"))),
        )
        .withColumn(
            "city",
            trim(col("city")),
        )
        .withColumn(
            "state",
            trim(col("state")),
        )
        .withColumn(
            "country",
            trim(col("country")),
        )
        .withColumn(
            "status",
            upper(trim(col("status"))),
        )
    )

    df = df.withColumn(
        "capacity_units",
        col("capacity_units").cast(
            IntegerType()
        ),
    )

    # --------------------------------------------------------
    # Validate capacity
    # --------------------------------------------------------

    df = df.withColumn(
        "capacity_units",
        when(
            col("capacity_units") > 0,
            col("capacity_units"),
        ).otherwise(None),
    )

    df = df.filter(
        col("warehouse_id").isNotNull()
        & (col("warehouse_id") != "")
    )

    df = df.dropDuplicates(
        ["warehouse_id"]
    )

    df = add_processing_metadata(df)

    write_silver(
        df,
        "warehouses",
    )

    print(
        f"Silver records: {df.count():,}"
    )


# ============================================================
# CUSTOMERS
# ============================================================

def transform_customers(spark):

    print("\n" + "=" * 70)
    print("TRANSFORMING CUSTOMERS")
    print("=" * 70)

    df = read_bronze(
        spark,
        "customers",
    )

    # --------------------------------------------------------
    # Clean strings
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "customer_id",
            trim(col("customer_id")),
        )
        .withColumn(
            "customer_name",
            trim(col("customer_name")),
        )
        .withColumn(
            "email",
            lower(trim(col("email"))),
        )
        .withColumn(
            "phone",
            regexp_replace(
                trim(col("phone")),
                "[^0-9]",
                "",
            ),
        )
        .withColumn(
            "city",
            trim(col("city")),
        )
        .withColumn(
            "state",
            trim(col("state")),
        )
        .withColumn(
            "country",
            trim(col("country")),
        )
        .withColumn(
            "customer_segment",
            upper(
                trim(
                    col("customer_segment")
                )
            ),
        )
    )

    # --------------------------------------------------------
    # Convert registration date
    # --------------------------------------------------------

    df = df.withColumn(
        "registration_date",
        to_date(
            col("registration_date"),
            "yyyy-MM-dd",
        ),
    )

    # --------------------------------------------------------
    # Remove invalid customer IDs
    # --------------------------------------------------------

    df = df.filter(
        col("customer_id").isNotNull()
        & (col("customer_id") != "")
    )

    # --------------------------------------------------------
    # Remove duplicates
    # --------------------------------------------------------

    df = df.dropDuplicates(
        ["customer_id"]
    )

    df = add_processing_metadata(df)

    write_silver(
        df,
        "customers",
    )

    print(
        f"Silver records: {df.count():,}"
    )


# ============================================================
# ORDERS
# ============================================================

def transform_orders(spark):

    print("\n" + "=" * 70)
    print("TRANSFORMING ORDERS")
    print("=" * 70)

    df = read_bronze(
        spark,
        "orders",
    )

    bronze_count = df.count()

    print(
        f"Bronze records: {bronze_count:,}"
    )

    # --------------------------------------------------------
    # Clean string fields
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "order_id",
            trim(col("order_id")),
        )
        .withColumn(
            "customer_id",
            trim(col("customer_id")),
        )
        .withColumn(
            "product_id",
            trim(col("product_id")),
        )
        .withColumn(
            "warehouse_id",
            trim(col("warehouse_id")),
        )
        .withColumn(
            "order_status",
            upper(trim(col("order_status"))),
        )
        .withColumn(
            "payment_status",
            upper(trim(col("payment_status"))),
        )
    )

    # --------------------------------------------------------
    # Convert timestamp
    # --------------------------------------------------------

    df = df.withColumn(
        "order_date",
        to_timestamp(
            col("order_date"),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )

    # --------------------------------------------------------
    # Cast numeric fields
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "quantity",
            col("quantity").cast(
                IntegerType()
            ),
        )
        .withColumn(
            "unit_price",
            col("unit_price").cast(
                DoubleType()
            ),
        )
        .withColumn(
            "total_amount",
            col("total_amount").cast(
                DoubleType()
            ),
        )
    )

    # --------------------------------------------------------
    # Data quality status
    # --------------------------------------------------------

    df = df.withColumn(
        "data_quality_status",
        when(
            col("order_id").isNull()
            | (col("order_id") == ""),
            "INVALID_ORDER_ID",
        )
        .when(
            col("customer_id").isNull()
            | (col("customer_id") == ""),
            "INVALID_CUSTOMER_ID",
        )
        .when(
            col("product_id").isNull()
            | (col("product_id") == ""),
            "INVALID_PRODUCT_ID",
        )
        .when(
            col("warehouse_id").isNull()
            | (col("warehouse_id") == ""),
            "INVALID_WAREHOUSE_ID",
        )
        .when(
            col("quantity").isNull()
            | (col("quantity") <= 0),
            "INVALID_QUANTITY",
        )
        .when(
            col("unit_price").isNull()
            | (col("unit_price") < 0),
            "INVALID_UNIT_PRICE",
        )
        .when(
            col("order_date").isNull(),
            "INVALID_ORDER_DATE",
        )
        .otherwise("VALID"),
    )

    # --------------------------------------------------------
    # Remove duplicate orders
    # --------------------------------------------------------

    window = (
        Window
        .partitionBy("order_id")
        .orderBy(
            col("_ingestion_timestamp").desc()
        )
    )

    df = (
        df
        .withColumn(
            "_row_number",
            row_number().over(window),
        )
        .filter(
            col("_row_number") == 1
        )
        .drop("_row_number")
    )

    # --------------------------------------------------------
    # Keep only valid records
    # --------------------------------------------------------

    df = df.filter(
        col("data_quality_status") == "VALID"
    )

    # --------------------------------------------------------
    # Derived date fields
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "order_day",
            to_date(col("order_date")),
        )
        .withColumn(
            "order_year",
            col("order_date").cast("date")
            .substr(1, 4),
        )
    )

    df = add_processing_metadata(df)

    silver_count = df.count()

    print(
        f"Silver records: {silver_count:,}"
    )

    print(
        f"Records removed: "
        f"{bronze_count - silver_count:,}"
    )

    write_silver(
        df,
        "orders",
    )


# ============================================================
# INVENTORY
# ============================================================

def transform_inventory(spark):

    print("\n" + "=" * 70)
    print("TRANSFORMING INVENTORY")
    print("=" * 70)

    df = read_bronze(
        spark,
        "inventory",
    )

    bronze_count = df.count()

    # --------------------------------------------------------
    # Clean IDs
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "inventory_id",
            trim(col("inventory_id")),
        )
        .withColumn(
            "warehouse_id",
            trim(col("warehouse_id")),
        )
        .withColumn(
            "product_id",
            trim(col("product_id")),
        )
    )

    # --------------------------------------------------------
    # Dates
    # --------------------------------------------------------

    df = df.withColumn(
        "inventory_date",
        to_date(
            col("inventory_date"),
            "yyyy-MM-dd",
        ),
    )

    # --------------------------------------------------------
    # Numeric fields
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "quantity_on_hand",
            col("quantity_on_hand").cast(
                IntegerType()
            ),
        )
        .withColumn(
            "reserved_quantity",
            col("reserved_quantity").cast(
                IntegerType()
            ),
        )
        .withColumn(
            "reorder_level",
            col("reorder_level").cast(
                IntegerType()
            ),
        )
    )

    # --------------------------------------------------------
    # Quality status
    # --------------------------------------------------------

    df = df.withColumn(
        "data_quality_status",
        when(
            col("inventory_id").isNull(),
            "INVALID_INVENTORY_ID",
        )
        .when(
            col("warehouse_id").isNull(),
            "INVALID_WAREHOUSE_ID",
        )
        .when(
            col("product_id").isNull(),
            "INVALID_PRODUCT_ID",
        )
        .when(
            col("quantity_on_hand").isNull()
            | (col("quantity_on_hand") < 0),
            "INVALID_QUANTITY",
        )
        .when(
            col("reserved_quantity").isNull()
            | (col("reserved_quantity") < 0),
            "INVALID_RESERVED_QUANTITY",
        )
        .otherwise("VALID"),
    )

    # --------------------------------------------------------
    # Remove duplicates
    # --------------------------------------------------------

    df = df.dropDuplicates(
        ["inventory_id"]
    )

    # --------------------------------------------------------
    # Keep valid data
    # --------------------------------------------------------

    df = df.filter(
        col("data_quality_status") == "VALID"
    )

    # --------------------------------------------------------
    # Inventory status
    # --------------------------------------------------------

    df = df.withColumn(
        "inventory_status",
        when(
            col("quantity_on_hand") == 0,
            "OUT_OF_STOCK",
        )
        .when(
            col("quantity_on_hand")
            <= col("reorder_level"),
            "LOW_STOCK",
        )
        .otherwise("SUFFICIENT"),
    )

    df = add_processing_metadata(df)

    silver_count = df.count()

    print(
        f"Silver records: {silver_count:,}"
    )

    print(
        f"Records removed: "
        f"{bronze_count - silver_count:,}"
    )

    write_silver(
        df,
        "inventory",
    )


# ============================================================
# SHIPMENTS
# ============================================================

def transform_shipments(spark):

    print("\n" + "=" * 70)
    print("TRANSFORMING SHIPMENTS")
    print("=" * 70)

    df = read_bronze(
        spark,
        "shipments",
    )

    bronze_count = df.count()

    # --------------------------------------------------------
    # Clean strings
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "shipment_id",
            trim(col("shipment_id")),
        )
        .withColumn(
            "order_id",
            trim(col("order_id")),
        )
        .withColumn(
            "warehouse_id",
            trim(col("warehouse_id")),
        )
        .withColumn(
            "carrier",
            trim(col("carrier")),
        )
        .withColumn(
            "shipping_status",
            upper(
                trim(
                    col("shipping_status")
                )
            ),
        )
    )

    # --------------------------------------------------------
    # Timestamp conversion
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "shipment_date",
            to_timestamp(
                col("shipment_date"),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn(
            "delivery_date",
            to_timestamp(
                col("delivery_date"),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
    )

    # --------------------------------------------------------
    # Shipping cost
    # --------------------------------------------------------

    df = df.withColumn(
        "shipping_cost",
        col("shipping_cost").cast(
            DoubleType()
        ),
    )

    # --------------------------------------------------------
    # Data quality
    # --------------------------------------------------------

    df = df.withColumn(
        "data_quality_status",
        when(
            col("shipment_id").isNull()
            | (col("shipment_id") == ""),
            "INVALID_SHIPMENT_ID",
        )
        .when(
            col("order_id").isNull()
            | (col("order_id") == ""),
            "INVALID_ORDER_ID",
        )
        .when(
            col("shipment_date").isNull(),
            "INVALID_SHIPMENT_DATE",
        )
        .when(
            col("shipping_cost").isNull()
            | (col("shipping_cost") < 0),
            "INVALID_SHIPPING_COST",
        )
        .when(
            (
                col("shipping_status")
                == "DELIVERED"
            )
            & col("delivery_date").isNull(),
            "MISSING_DELIVERY_DATE",
        )
        .otherwise("VALID"),
    )

    # --------------------------------------------------------
    # Remove duplicate shipments
    # --------------------------------------------------------

    df = df.dropDuplicates(
        ["shipment_id"]
    )

    # --------------------------------------------------------
    # Keep valid records
    # --------------------------------------------------------

    df = df.filter(
        col("data_quality_status") == "VALID"
    )

    # --------------------------------------------------------
    # Delivery duration
    # --------------------------------------------------------

    df = df.withColumn(
        "delivery_days",
        when(
            col("delivery_date").isNotNull(),
            (
                col("delivery_date").cast("long")
                - col("shipment_date").cast("long")
            ) / 86400,
        ).otherwise(None),
    )

    # --------------------------------------------------------
    # On-time status
    # --------------------------------------------------------

    df = df.withColumn(
        "delivery_performance",
        when(
            col("shipping_status")
            == "DELAYED",
            "DELAYED",
        )
        .when(
            col("shipping_status")
            == "DELIVERED",
            "ON_TIME",
        )
        .otherwise("PENDING"),
    )

    df = add_processing_metadata(df)

    silver_count = df.count()

    print(
        f"Silver records: {silver_count:,}"
    )

    print(
        f"Records removed: "
        f"{bronze_count - silver_count:,}"
    )

    write_silver(
        df,
        "shipments",
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("SUPPLY CHAIN DATA PLATFORM")
    print("BRONZE -> SILVER TRANSFORMATION")
    print("=" * 70)

    SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    spark = create_spark_session()

    try:

        # Dimension datasets
        transform_suppliers(spark)
        transform_products(spark)
        transform_warehouses(spark)
        transform_customers(spark)

        # Fact datasets
        transform_orders(spark)
        transform_inventory(spark)
        transform_shipments(spark)

    finally:

        spark.stop()

    print("\n" + "=" * 70)
    print("BRONZE -> SILVER COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()