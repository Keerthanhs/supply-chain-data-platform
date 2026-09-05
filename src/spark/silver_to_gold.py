from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    year,
    month,
    dayofmonth,
    quarter,
    dayofweek,
    weekofyear,
    date_format,
    min,
    max,
    sum,
    count,
    avg,
    when,
    round,
)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("SupplyChain-Silver-To-Gold")
        .master("local[*]")
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# HELPERS
# ============================================================

def read_silver(spark, dataset):

    path = SILVER_DIR / dataset

    print(f"Reading Silver: {path}")

    return spark.read.parquet(str(path))


def write_gold(df, table_name):

    output_path = GOLD_DIR / table_name

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(
        f"[SUCCESS] Gold table created: "
        f"{table_name}"
    )

    print(
        f"Records: {df.count():,}"
    )


# ============================================================
# DIMENSION: CUSTOMER
# ============================================================

def create_dim_customer(spark):

    print("\n" + "=" * 70)
    print("CREATING DIM_CUSTOMER")
    print("=" * 70)

    customers = read_silver(
        spark,
        "customers",
    )

    dim_customer = customers.select(
        col("customer_id"),
        col("customer_name"),
        col("email"),
        col("phone"),
        col("city"),
        col("state"),
        col("country"),
        col("customer_segment"),
        col("registration_date"),
    )

    dim_customer = (
        dim_customer
        .withColumn(
            "customer_sk",
            col("customer_id"),
        )
        .withColumn(
            "created_at",
            current_timestamp(),
        )
    )

    write_gold(
        dim_customer,
        "dim_customer",
    )


# ============================================================
# DIMENSION: SUPPLIER
# ============================================================

def create_dim_supplier(spark):

    print("\n" + "=" * 70)
    print("CREATING DIM_SUPPLIER")
    print("=" * 70)

    suppliers = read_silver(
        spark,
        "suppliers",
    )

    dim_supplier = suppliers.select(
        col("supplier_id"),
        col("supplier_name"),
        col("country"),
        col("rating"),
        col("lead_time_days"),
        col("contact_email"),
        col("status"),
    )

    dim_supplier = (
        dim_supplier
        .withColumn(
            "supplier_sk",
            col("supplier_id"),
        )
        .withColumn(
            "created_at",
            current_timestamp(),
        )
    )

    write_gold(
        dim_supplier,
        "dim_supplier",
    )


# ============================================================
# DIMENSION: PRODUCT
# ============================================================

def create_dim_product(spark):

    print("\n" + "=" * 70)
    print("CREATING DIM_PRODUCT")
    print("=" * 70)

    products = read_silver(
        spark,
        "products",
    )

    dim_product = products.select(
        col("product_id"),
        col("product_name"),
        col("category"),
        col("supplier_id"),
        col("unit_cost"),
        col("selling_price"),
        col("weight_kg"),
        col("reorder_level"),
        col("status"),
    )

    dim_product = (
        dim_product
        .withColumn(
            "product_sk",
            col("product_id"),
        )
        .withColumn(
            "profit_per_unit",
            round(
                col("selling_price")
                - col("unit_cost"),
                2,
            ),
        )
        .withColumn(
            "created_at",
            current_timestamp(),
        )
    )

    write_gold(
        dim_product,
        "dim_product",
    )


# ============================================================
# DIMENSION: WAREHOUSE
# ============================================================

def create_dim_warehouse(spark):

    print("\n" + "=" * 70)
    print("CREATING DIM_WAREHOUSE")
    print("=" * 70)

    warehouses = read_silver(
        spark,
        "warehouses",
    )

    dim_warehouse = warehouses.select(
        col("warehouse_id"),
        col("warehouse_name"),
        col("warehouse_type"),
        col("city"),
        col("state"),
        col("country"),
        col("capacity_units"),
        col("status"),
    )

    dim_warehouse = (
        dim_warehouse
        .withColumn(
            "warehouse_sk",
            col("warehouse_id"),
        )
        .withColumn(
            "created_at",
            current_timestamp(),
        )
    )

    write_gold(
        dim_warehouse,
        "dim_warehouse",
    )


# ============================================================
# DIMENSION: DATE
# ============================================================

def create_dim_date(spark):

    print("\n" + "=" * 70)
    print("CREATING DIM_DATE")
    print("=" * 70)

    orders = read_silver(
        spark,
        "orders",
    )

    date_df = (
        orders
        .select(
            col("order_day").alias("date")
        )
        .filter(
            col("date").isNotNull()
        )
        .distinct()
    )

    dim_date = (
        date_df
        .withColumn(
            "year",
            year(col("date")),
        )
        .withColumn(
            "quarter",
            quarter(col("date")),
        )
        .withColumn(
            "month",
            month(col("date")),
        )
        .withColumn(
            "month_name",
            date_format(
                col("date"),
                "MMMM",
            ),
        )
        .withColumn(
            "week_of_year",
            weekofyear(col("date")),
        )
        .withColumn(
            "day",
            dayofmonth(col("date")),
        )
        .withColumn(
            "day_of_week",
            dayofweek(col("date")),
        )
        .withColumn(
            "day_name",
            date_format(
                col("date"),
                "EEEE",
            ),
        )
        .withColumn(
            "is_weekend",
            when(
                dayofweek(col("date")).isin(
                    1,
                    7,
                ),
                True,
            ).otherwise(False),
        )
    )

    write_gold(
        dim_date,
        "dim_date",
    )


# ============================================================
# FACT: ORDERS
# ============================================================

def create_fact_orders(spark):

    print("\n" + "=" * 70)
    print("CREATING FACT_ORDERS")
    print("=" * 70)

    orders = read_silver(
        spark,
        "orders",
    )

    fact_orders = (
        orders.select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("warehouse_id"),
            col("order_day").alias(
                "order_date"
            ),
            col("quantity"),
            col("unit_price"),
            col("total_amount"),
            col("order_status"),
            col("payment_status"),
        )
    )

    # --------------------------------------------------------
    # Business metrics
    # --------------------------------------------------------

    fact_orders = (
        fact_orders
        .withColumn(
            "total_cost",
            round(
                col("quantity")
                * col("unit_price")
                * lit(0.70),
                2,
            ),
        )
        .withColumn(
            "estimated_profit",
            round(
                col("total_amount")
                - col("total_cost"),
                2,
            ),
        )
        .withColumn(
            "created_at",
            current_timestamp(),
        )
    )

    write_gold(
        fact_orders,
        "fact_orders",
    )


# ============================================================
# FACT: INVENTORY
# ============================================================

def create_fact_inventory(spark):

    print("\n" + "=" * 70)
    print("CREATING FACT_INVENTORY")
    print("=" * 70)

    inventory = read_silver(
        spark,
        "inventory",
    )

    fact_inventory = inventory.select(
        col("inventory_id"),
        col("warehouse_id"),
        col("product_id"),
        col("inventory_date"),
        col("quantity_on_hand"),
        col("reserved_quantity"),
        col("reorder_level"),
        col("inventory_status"),
    )

    fact_inventory = (
        fact_inventory
        .withColumn(
            "available_quantity",
            col("quantity_on_hand")
            - col("reserved_quantity"),
        )
        .withColumn(
            "inventory_value",
            col("quantity_on_hand")
            * lit(1.0),
        )
        .withColumn(
            "created_at",
            current_timestamp(),
        )
    )

    write_gold(
        fact_inventory,
        "fact_inventory",
    )


# ============================================================
# FACT: SHIPMENTS
# ============================================================

def create_fact_shipments(spark):

    print("\n" + "=" * 70)
    print("CREATING FACT_SHIPMENTS")
    print("=" * 70)

    shipments = read_silver(
        spark,
        "shipments",
    )

    fact_shipments = shipments.select(
        col("shipment_id"),
        col("order_id"),
        col("warehouse_id"),
        col("carrier"),
        col("shipment_date"),
        col("delivery_date"),
        col("shipping_status"),
        col("shipping_cost"),
        col("delivery_days"),
        col("delivery_performance"),
    )

    fact_shipments = (
        fact_shipments
        .withColumn(
            "created_at",
            current_timestamp(),
        )
    )

    write_gold(
        fact_shipments,
        "fact_shipments",
    )


# ============================================================
# BUSINESS KPI: DAILY SALES
# ============================================================

def create_daily_sales(spark):

    print("\n" + "=" * 70)
    print("CREATING DAILY_SALES")
    print("=" * 70)

    orders = read_silver(
        spark,
        "orders",
    )

    daily_sales = (
        orders
        .groupBy(
            col("order_day").alias(
                "sales_date"
            )
        )
        .agg(
            count("order_id").alias(
                "total_orders"
            ),
            sum("quantity").alias(
                "units_sold"
            ),
            round(
                sum("total_amount"),
                2,
            ).alias(
                "total_revenue"
            ),
            round(
                avg("total_amount"),
                2,
            ).alias(
                "average_order_value"
            ),
        )
        .orderBy("sales_date")
    )

    write_gold(
        daily_sales,
        "daily_sales",
    )


# ============================================================
# BUSINESS KPI: SUPPLIER PERFORMANCE
# ============================================================

def create_supplier_performance(spark):

    print("\n" + "=" * 70)
    print("CREATING SUPPLIER_PERFORMANCE")
    print("=" * 70)

    suppliers = read_silver(
        spark,
        "suppliers",
    )

    products = read_silver(
        spark,
        "products",
    )

    supplier_products = (
        products
        .groupBy("supplier_id")
        .agg(
            count("product_id").alias(
                "product_count"
            ),
            round(
                avg("unit_cost"),
                2,
            ).alias(
                "average_product_cost"
            ),
        )
    )

    supplier_performance = (
        suppliers
        .join(
            supplier_products,
            on="supplier_id",
            how="left",
        )
        .select(
            "supplier_id",
            "supplier_name",
            "country",
            "rating",
            "lead_time_days",
            "product_count",
            "average_product_cost",
            "status",
        )
    )

    write_gold(
        supplier_performance,
        "supplier_performance",
    )


# ============================================================
# BUSINESS KPI: WAREHOUSE PERFORMANCE
# ============================================================

def create_warehouse_performance(spark):

    print("\n" + "=" * 70)
    print("CREATING WAREHOUSE_PERFORMANCE")
    print("=" * 70)

    warehouses = read_silver(
        spark,
        "warehouses",
    )

    orders = read_silver(
        spark,
        "orders",
    )

    warehouse_orders = (
        orders
        .groupBy("warehouse_id")
        .agg(
            count("order_id").alias(
                "total_orders"
            ),
            sum("quantity").alias(
                "units_processed"
            ),
            round(
                sum("total_amount"),
                2,
            ).alias(
                "revenue_generated"
            ),
        )
    )

    warehouse_performance = (
        warehouses
        .join(
            warehouse_orders,
            on="warehouse_id",
            how="left",
        )
        .fillna(
            {
                "total_orders": 0,
                "units_processed": 0,
                "revenue_generated": 0,
            }
        )
    )

    warehouse_performance = (
        warehouse_performance
        .withColumn(
            "capacity_utilization_estimate",
            round(
                col("units_processed")
                / col("capacity_units")
                * 100,
                2,
            ),
        )
    )

    write_gold(
        warehouse_performance,
        "warehouse_performance",
    )


# ============================================================
# BUSINESS KPI: INVENTORY METRICS
# ============================================================

def create_inventory_metrics(spark):

    print("\n" + "=" * 70)
    print("CREATING INVENTORY_METRICS")
    print("=" * 70)

    inventory = read_silver(
        spark,
        "inventory",
    )

    products = read_silver(
        spark,
        "products",
    )

    inventory_metrics = (
        inventory
        .join(
            products.select(
                "product_id",
                "product_name",
                "category",
                "unit_cost",
            ),
            on="product_id",
            how="left",
        )
        .groupBy(
            "product_id",
            "product_name",
            "category",
        )
        .agg(
            sum(
                "quantity_on_hand"
            ).alias(
                "total_inventory_units"
            ),
            sum(
                "reserved_quantity"
            ).alias(
                "total_reserved_units"
            ),
            count(
                "inventory_id"
            ).alias(
                "inventory_records"
            ),
        )
    )

    inventory_metrics = (
        inventory_metrics
        .withColumn(
            "available_units",
            col("total_inventory_units")
            - col("total_reserved_units"),
        )
        .withColumn(
            "stock_status",
            when(
                col("available_units") <= 0,
                "OUT_OF_STOCK",
            )
            .when(
                col("available_units") < 100,
                "LOW_STOCK",
            )
            .otherwise("HEALTHY"),
        )
    )

    write_gold(
        inventory_metrics,
        "inventory_metrics",
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("SUPPLY CHAIN DATA PLATFORM")
    print("SILVER -> GOLD TRANSFORMATION")
    print("=" * 70)

    GOLD_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    spark = create_spark_session()

    try:

        # ----------------------------------------------------
        # Dimensions
        # ----------------------------------------------------

        create_dim_customer(spark)
        create_dim_supplier(spark)
        create_dim_product(spark)
        create_dim_warehouse(spark)
        create_dim_date(spark)

        # ----------------------------------------------------
        # Facts
        # ----------------------------------------------------

        create_fact_orders(spark)
        create_fact_inventory(spark)
        create_fact_shipments(spark)

        # ----------------------------------------------------
        # Business analytics
        # ----------------------------------------------------

        create_daily_sales(spark)
        create_supplier_performance(spark)
        create_warehouse_performance(spark)
        create_inventory_metrics(spark)

    finally:

        spark.stop()

    print("\n" + "=" * 70)
    print("SILVER -> GOLD COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()