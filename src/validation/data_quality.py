from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_DIR = PROJECT_ROOT / "data" / "gold"
REPORT_DIR = PROJECT_ROOT / "data" / "quality_reports"


# ============================================================
# SPARK
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("SupplyChain-Data-Quality")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# RESULT STORAGE
# ============================================================

results = []


def record_result(
    check_name,
    dataset,
    status,
    details,
):

    results.append(
        {
            "check_name": check_name,
            "dataset": dataset,
            "status": status,
            "details": details,
        }
    )


# ============================================================
# LOAD GOLD
# ============================================================

def load_gold(
    spark,
    dataset,
):

    path = GOLD_DIR / dataset

    return spark.read.parquet(
        str(path)
    )


# ============================================================
# CHECK 1
# REQUIRED COLUMNS
# ============================================================

def check_required_columns(
    df,
    dataset,
    required_columns,
):

    actual_columns = set(
        df.columns
    )

    missing = [
        column
        for column in required_columns
        if column not in actual_columns
    ]

    if missing:

        record_result(
            "required_columns",
            dataset,
            "FAIL",
            f"Missing columns: {missing}",
        )

        return False

    record_result(
        "required_columns",
        dataset,
        "PASS",
        "All required columns exist",
    )

    return True


# ============================================================
# CHECK 2
# NULL VALUES
# ============================================================

def check_nulls(
    df,
    dataset,
    columns,
):

    failed = False

    for column in columns:

        null_count = (
            df.filter(
                col(column).isNull()
            ).count()
        )

        if null_count > 0:

            record_result(
                f"null_check_{column}",
                dataset,
                "FAIL",
                f"{null_count:,} null values",
            )

            failed = True

        else:

            record_result(
                f"null_check_{column}",
                dataset,
                "PASS",
                "No null values",
            )

    return not failed


# ============================================================
# CHECK 3
# DUPLICATES
# ============================================================

def check_duplicates(
    df,
    dataset,
    key_column,
):

    total_count = df.count()

    distinct_count = (
        df.select(key_column)
        .distinct()
        .count()
    )

    duplicates = (
        total_count
        - distinct_count
    )

    if duplicates > 0:

        record_result(
            "duplicate_check",
            dataset,
            "FAIL",
            f"{duplicates:,} duplicate records",
        )

        return False

    record_result(
        "duplicate_check",
        dataset,
        "PASS",
        "No duplicate records",
    )

    return True


# ============================================================
# CHECK 4
# POSITIVE VALUES
# ============================================================

def check_positive_values(
    df,
    dataset,
    column,
):

    invalid_count = (
        df.filter(
            col(column) <= 0
        ).count()
    )

    if invalid_count > 0:

        record_result(
            f"positive_check_{column}",
            dataset,
            "FAIL",
            f"{invalid_count:,} invalid values",
        )

        return False

    record_result(
        f"positive_check_{column}",
        dataset,
        "PASS",
        "All values are positive",
    )

    return True


# ============================================================
# CHECK 5
# NON-NEGATIVE VALUES
# ============================================================

def check_non_negative(
    df,
    dataset,
    column,
):

    invalid_count = (
        df.filter(
            col(column) < 0
        ).count()
    )

    if invalid_count > 0:

        record_result(
            f"non_negative_{column}",
            dataset,
            "FAIL",
            f"{invalid_count:,} negative values",
        )

        return False

    record_result(
        f"non_negative_{column}",
        dataset,
        "PASS",
        "No negative values",
    )

    return True


# ============================================================
# CHECK 6
# REFERENTIAL INTEGRITY
# ============================================================

def check_foreign_key(
    child_df,
    parent_df,
    child_column,
    parent_column,
    relationship_name,
):

    invalid_count = (
        child_df
        .select(
            col(child_column)
            .alias("key")
        )
        .filter(
            col("key").isNotNull()
        )
        .join(
            parent_df.select(
                col(parent_column)
                .alias("parent_key")
            ),
            col("key")
            == col("parent_key"),
            "left_anti",
        )
        .count()
    )

    if invalid_count > 0:

        record_result(
            "referential_integrity",
            relationship_name,
            "FAIL",
            f"{invalid_count:,} orphan records",
        )

        return False

    record_result(
        "referential_integrity",
        relationship_name,
        "PASS",
        "All keys have matching parent records",
    )

    return True


# ============================================================
# ORDERS VALIDATION
# ============================================================

def validate_orders(spark):

    print("\n" + "=" * 70)
    print("VALIDATING FACT_ORDERS")
    print("=" * 70)

    orders = load_gold(
        spark,
        "fact_orders",
    )

    customers = load_gold(
        spark,
        "dim_customer",
    )

    products = load_gold(
        spark,
        "dim_product",
    )

    warehouses = load_gold(
        spark,
        "dim_warehouse",
    )

    check_required_columns(
        orders,
        "fact_orders",
        [
            "order_id",
            "customer_id",
            "product_id",
            "warehouse_id",
            "quantity",
            "total_amount",
        ],
    )

    check_duplicates(
        orders,
        "fact_orders",
        "order_id",
    )

    check_positive_values(
        orders,
        "fact_orders",
        "quantity",
    )

    check_non_negative(
        orders,
        "fact_orders",
        "total_amount",
    )

    check_foreign_key(
        orders,
        customers,
        "customer_id",
        "customer_id",
        "orders -> customers",
    )

    check_foreign_key(
        orders,
        products,
        "product_id",
        "product_id",
        "orders -> products",
    )

    check_foreign_key(
        orders,
        warehouses,
        "warehouse_id",
        "warehouse_id",
        "orders -> warehouses",
    )


# ============================================================
# INVENTORY VALIDATION
# ============================================================

def validate_inventory(spark):

    print("\n" + "=" * 70)
    print("VALIDATING FACT_INVENTORY")
    print("=" * 70)

    inventory = load_gold(
        spark,
        "fact_inventory",
    )

    products = load_gold(
        spark,
        "dim_product",
    )

    warehouses = load_gold(
        spark,
        "dim_warehouse",
    )

    check_required_columns(
        inventory,
        "fact_inventory",
        [
            "inventory_id",
            "product_id",
            "warehouse_id",
            "quantity_on_hand",
            "reserved_quantity",
        ],
    )

    check_duplicates(
        inventory,
        "fact_inventory",
        "inventory_id",
    )

    check_non_negative(
        inventory,
        "fact_inventory",
        "quantity_on_hand",
    )

    check_non_negative(
        inventory,
        "fact_inventory",
        "reserved_quantity",
    )

    check_foreign_key(
        inventory,
        products,
        "product_id",
        "product_id",
        "inventory -> products",
    )

    check_foreign_key(
        inventory,
        warehouses,
        "warehouse_id",
        "warehouse_id",
        "inventory -> warehouses",
    )


# ============================================================
# SHIPMENT VALIDATION
# ============================================================

def validate_shipments(spark):

    print("\n" + "=" * 70)
    print("VALIDATING FACT_SHIPMENTS")
    print("=" * 70)

    shipments = load_gold(
        spark,
        "fact_shipments",
    )

    warehouses = load_gold(
        spark,
        "dim_warehouse",
    )

    check_required_columns(
        shipments,
        "fact_shipments",
        [
            "shipment_id",
            "order_id",
            "warehouse_id",
            "shipment_date",
            "shipping_cost",
        ],
    )

    check_duplicates(
        shipments,
        "fact_shipments",
        "shipment_id",
    )

    check_non_negative(
        shipments,
        "fact_shipments",
        "shipping_cost",
    )

    check_foreign_key(
        shipments,
        warehouses,
        "warehouse_id",
        "warehouse_id",
        "shipments -> warehouses",
    )


# ============================================================
# DIMENSION VALIDATION
# ============================================================

def validate_dimensions(spark):

    print("\n" + "=" * 70)
    print("VALIDATING DIMENSIONS")
    print("=" * 70)

    dimensions = {
        "dim_customer": "customer_id",
        "dim_product": "product_id",
        "dim_supplier": "supplier_id",
        "dim_warehouse": "warehouse_id",
        "dim_date": "date",
    }

    for dataset, key in dimensions.items():

        df = load_gold(
            spark,
            dataset,
        )

        check_duplicates(
            df,
            dataset,
            key,
        )


# ============================================================
# GENERATE REPORT
# ============================================================

def generate_report():

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_path = (
        REPORT_DIR
        / "data_quality_report.csv"
    )

    import csv

    with open(
        report_path,
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "check_name",
                "dataset",
                "status",
                "details",
            ],
        )

        writer.writeheader()

        writer.writerows(
            results
        )

    print(
        f"\nQuality report: "
        f"{report_path}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("SUPPLY CHAIN DATA QUALITY FRAMEWORK")
    print("=" * 70)

    spark = create_spark_session()

    try:

        validate_dimensions(spark)

        validate_orders(spark)

        validate_inventory(spark)

        validate_shipments(spark)

    finally:

        spark.stop()

    generate_report()

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    passed = sum(
        1
        for result in results
        if result["status"] == "PASS"
    )

    failed = sum(
        1
        for result in results
        if result["status"] == "FAIL"
    )

    print("\n" + "=" * 70)
    print("DATA QUALITY SUMMARY")
    print("=" * 70)

    print(f"Checks passed : {passed}")
    print(f"Checks failed : {failed}")

    if failed > 0:

        print(
            "\n❌ DATA QUALITY FAILED"
        )

        raise SystemExit(1)

    print(
        "\n✅ DATA QUALITY PASSED"
    )


if __name__ == "__main__":
    main()