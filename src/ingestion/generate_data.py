import csv
import os
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"

# Dataset sizes
NUM_SUPPLIERS = 50
NUM_PRODUCTS = 500
NUM_WAREHOUSES = 10
NUM_CUSTOMERS = 10_000
NUM_ORDERS = 100_000

START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2026, 8, 15)

# ============================================================
# REFERENCE DATA
# ============================================================

COUNTRIES = [
    "India",
    "USA",
    "Germany",
    "UK",
    "Canada",
    "Australia",
    "Singapore",
    "UAE",
]

INDIAN_STATES = [
    "Karnataka",
    "Maharashtra",
    "Tamil Nadu",
    "Telangana",
    "Kerala",
    "Gujarat",
    "Delhi",
    "Rajasthan",
    "West Bengal",
    "Andhra Pradesh",
]

CITIES = [
    "Bengaluru",
    "Mumbai",
    "Chennai",
    "Hyderabad",
    "Kochi",
    "Pune",
    "Ahmedabad",
    "Delhi",
    "Jaipur",
    "Kolkata",
]

PRODUCT_CATEGORIES = [
    "Electronics",
    "Home Appliances",
    "Furniture",
    "Clothing",
    "Footwear",
    "Groceries",
    "Sports",
    "Beauty",
    "Automotive",
    "Books",
]

PRODUCT_PREFIXES = {
    "Electronics": ["Laptop", "Smartphone", "Monitor", "Tablet", "Headphones"],
    "Home Appliances": ["Refrigerator", "Microwave", "Mixer", "Vacuum", "Oven"],
    "Furniture": ["Chair", "Table", "Desk", "Sofa", "Bed"],
    "Clothing": ["Shirt", "Jeans", "Jacket", "T-Shirt", "Sweater"],
    "Footwear": ["Shoes", "Sneakers", "Boots", "Sandals", "Slippers"],
    "Groceries": ["Rice", "Oil", "Sugar", "Flour", "Cereal"],
    "Sports": ["Cricket Bat", "Football", "Tennis Racket", "Cycle", "Dumbbell"],
    "Beauty": ["Shampoo", "Perfume", "Face Wash", "Lotion", "Cream"],
    "Automotive": ["Brake Pad", "Engine Oil", "Battery", "Filter", "Tyre"],
    "Books": ["Novel", "Textbook", "Notebook", "Magazine", "Dictionary"],
}

CARRIERS = [
    "DHL",
    "FedEx",
    "BlueDart",
    "Delhivery",
    "DTDC",
    "Ecom Express",
    "XpressBees",
]

ORDER_STATUSES = [
    "PLACED",
    "PROCESSING",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
]

SHIPMENT_STATUSES = [
    "IN_TRANSIT",
    "DELIVERED",
    "DELAYED",
    "CANCELLED",
]

WAREHOUSE_TYPES = [
    "REGIONAL",
    "DISTRIBUTION",
    "FULFILLMENT",
]

# ============================================================
# UTILITY FUNCTIONS
# ============================================================


def random_date(start_date, end_date):
    """Generate a random datetime between two dates."""
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_date + timedelta(seconds=random_seconds)


def random_date_string(start_date, end_date):
    return random_date(start_date, end_date).strftime("%Y-%m-%d")


def random_datetime_string(start_date, end_date):
    return random_date(start_date, end_date).strftime("%Y-%m-%d %H:%M:%S")


def random_name():
    first_names = [
        "Aarav",
        "Vivaan",
        "Aditya",
        "Arjun",
        "Rahul",
        "Rohan",
        "Karan",
        "Ananya",
        "Diya",
        "Priya",
        "Sneha",
        "Isha",
        "Meera",
        "Neha",
        "Kavya",
    ]

    last_names = [
        "Sharma",
        "Patel",
        "Reddy",
        "Iyer",
        "Nair",
        "Mehta",
        "Singh",
        "Gupta",
        "Rao",
        "Kumar",
    ]

    return f"{random.choice(first_names)} {random.choice(last_names)}"


def random_email(name):
    clean_name = name.lower().replace(" ", ".")
    suffix = random.randint(1, 9999)
    return f"{clean_name}{suffix}@example.com"


def random_phone():
    return "9" + "".join(random.choices(string.digits, k=9))


def write_csv(filename, rows, fieldnames):
    """
    Write records to CSV.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    filepath = OUTPUT_DIR / filename

    with open(filepath, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(rows)

    print(
        f"Created {filename:<20} "
        f"{len(rows):>8,} records"
    )


# ============================================================
# 1. SUPPLIERS
# ============================================================


def generate_suppliers():
    suppliers = []

    for i in range(1, NUM_SUPPLIERS + 1):

        supplier_id = f"SUP{i:04d}"

        suppliers.append(
            {
                "supplier_id": supplier_id,
                "supplier_name": f"Supplier Corporation {i:03d}",
                "country": random.choice(COUNTRIES),
                "rating": round(random.uniform(2.5, 5.0), 2),
                "lead_time_days": random.randint(2, 30),
                "contact_email": f"supplier{i:03d}@example.com",
                "status": random.choice(
                    ["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE"]
                ),
            }
        )

    write_csv(
        "suppliers.csv",
        suppliers,
        [
            "supplier_id",
            "supplier_name",
            "country",
            "rating",
            "lead_time_days",
            "contact_email",
            "status",
        ],
    )

    return suppliers


# ============================================================
# 2. PRODUCTS
# ============================================================


def generate_products(suppliers):
    products = []

    for i in range(1, NUM_PRODUCTS + 1):

        category = random.choice(PRODUCT_CATEGORIES)
        prefix = random.choice(PRODUCT_PREFIXES[category])

        unit_cost = round(random.uniform(5, 2000), 2)

        # Selling price is normally higher than cost.
        selling_price = round(
            unit_cost * random.uniform(1.10, 1.80),
            2,
        )

        products.append(
            {
                "product_id": f"PROD{i:05d}",
                "product_name": f"{prefix} {i:04d}",
                "category": category,
                "supplier_id": random.choice(suppliers)["supplier_id"],
                "unit_cost": unit_cost,
                "selling_price": selling_price,
                "weight_kg": round(random.uniform(0.1, 100), 2),
                "reorder_level": random.randint(10, 200),
                "status": random.choice(
                    ["ACTIVE", "ACTIVE", "ACTIVE", "DISCONTINUED"]
                ),
            }
        )

    write_csv(
        "products.csv",
        products,
        [
            "product_id",
            "product_name",
            "category",
            "supplier_id",
            "unit_cost",
            "selling_price",
            "weight_kg",
            "reorder_level",
            "status",
        ],
    )

    return products


# ============================================================
# 3. WAREHOUSES
# ============================================================


def generate_warehouses():
    warehouses = []

    for i in range(1, NUM_WAREHOUSES + 1):

        city_index = (i - 1) % len(CITIES)

        warehouses.append(
            {
                "warehouse_id": f"WH{i:03d}",
                "warehouse_name": f"Warehouse {i:02d}",
                "warehouse_type": random.choice(WAREHOUSE_TYPES),
                "city": CITIES[city_index],
                "state": INDIAN_STATES[city_index],
                "country": "India",
                "capacity_units": random.randint(
                    50_000,
                    500_000,
                ),
                "status": "ACTIVE",
            }
        )

    write_csv(
        "warehouses.csv",
        warehouses,
        [
            "warehouse_id",
            "warehouse_name",
            "warehouse_type",
            "city",
            "state",
            "country",
            "capacity_units",
            "status",
        ],
    )

    return warehouses


# ============================================================
# 4. CUSTOMERS
# ============================================================


def generate_customers():
    customers = []

    for i in range(1, NUM_CUSTOMERS + 1):

        name = random_name()

        customers.append(
            {
                "customer_id": f"CUST{i:06d}",
                "customer_name": name,
                "email": random_email(name),
                "phone": random_phone(),
                "city": random.choice(CITIES),
                "state": random.choice(INDIAN_STATES),
                "country": "India",
                "customer_segment": random.choice(
                    [
                        "CONSUMER",
                        "SMALL_BUSINESS",
                        "ENTERPRISE",
                    ]
                ),
                "registration_date": random_date_string(
                    datetime(2022, 1, 1),
                    END_DATE,
                ),
            }
        )

    write_csv(
        "customers.csv",
        customers,
        [
            "customer_id",
            "customer_name",
            "email",
            "phone",
            "city",
            "state",
            "country",
            "customer_segment",
            "registration_date",
        ],
    )

    return customers


# ============================================================
# 5. ORDERS
# ============================================================


def generate_orders(customers, products, warehouses):
    orders = []

    for i in range(1, NUM_ORDERS + 1):

        product = random.choice(products)
        customer = random.choice(customers)
        warehouse = random.choice(warehouses)

        quantity = random.randint(1, 10)

        order_date = random_date(
            START_DATE,
            END_DATE - timedelta(days=10),
        )

        status = random.choices(
            ORDER_STATUSES,
            weights=[10, 10, 15, 60, 5],
            k=1,
        )[0]

        order_id = f"ORD{i:08d}"

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "warehouse_id": warehouse["warehouse_id"],
                "order_date": order_date.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "quantity": quantity,
                "unit_price": product["selling_price"],
                "total_amount": round(
                    quantity * product["selling_price"],
                    2,
                ),
                "order_status": status,
                "payment_status": random.choice(
                    [
                        "PAID",
                        "PAID",
                        "PAID",
                        "PENDING",
                        "REFUNDED",
                    ]
                ),
            }
        )

    # --------------------------------------------------------
    # Inject duplicate orders
    # --------------------------------------------------------

    duplicate_count = max(1, int(NUM_ORDERS * 0.005))

    duplicates = random.sample(
        orders,
        duplicate_count,
    )

    orders.extend(duplicates)

    # --------------------------------------------------------
    # Inject invalid quantities
    # --------------------------------------------------------

    invalid_count = max(
        1,
        int(len(orders) * 0.002),
    )

    for record in random.sample(
        orders,
        invalid_count,
    ):
        record["quantity"] = random.choice(
            [-1, -5, 0]
        )

    # --------------------------------------------------------
    # Inject NULL customer IDs
    # --------------------------------------------------------

    null_count = max(
        1,
        int(len(orders) * 0.001),
    )

    for record in random.sample(
        orders,
        null_count,
    ):
        record["customer_id"] = ""

    write_csv(
        "orders.csv",
        orders,
        [
            "order_id",
            "customer_id",
            "product_id",
            "warehouse_id",
            "order_date",
            "quantity",
            "unit_price",
            "total_amount",
            "order_status",
            "payment_status",
        ],
    )

    return orders


# ============================================================
# 6. INVENTORY
# ============================================================


def generate_inventory(products, warehouses):
    inventory = []

    inventory_id = 1

    # Generate multiple snapshots for each
    # warehouse/product combination.

    for warehouse in warehouses:

        selected_products = random.sample(
            products,
            min(500, len(products)),
        )

        for product in selected_products:

            inventory.append(
                {
                    "inventory_id": f"INV{inventory_id:08d}",
                    "warehouse_id": warehouse["warehouse_id"],
                    "product_id": product["product_id"],
                    "inventory_date": random_date_string(
                        START_DATE,
                        END_DATE,
                    ),
                    "quantity_on_hand": random.randint(
                        0,
                        5000,
                    ),
                    "reserved_quantity": random.randint(
                        0,
                        500,
                    ),
                    "reorder_level": product[
                        "reorder_level"
                    ],
                }
            )

            inventory_id += 1

    # --------------------------------------------------------
    # Inject some zero-stock records
    # --------------------------------------------------------

    zero_stock_count = max(
        1,
        int(len(inventory) * 0.01),
    )

    for record in random.sample(
        inventory,
        zero_stock_count,
    ):
        record["quantity_on_hand"] = 0

    write_csv(
        "inventory.csv",
        inventory,
        [
            "inventory_id",
            "warehouse_id",
            "product_id",
            "inventory_date",
            "quantity_on_hand",
            "reserved_quantity",
            "reorder_level",
        ],
    )

    return inventory


# ============================================================
# 7. SHIPMENTS
# ============================================================


def generate_shipments(orders):
    shipments = []

    shipment_counter = 1

    # Only a subset of cancelled orders will not be shipped.
    shippable_orders = [
        order
        for order in orders
        if order["order_status"] != "CANCELLED"
    ]

    for order in shippable_orders:

        order_date = datetime.strptime(
            order["order_date"],
            "%Y-%m-%d %H:%M:%S",
        )

        shipment_date = order_date + timedelta(
            days=random.randint(1, 5)
        )

        delivery_date = None

        shipment_status = random.choices(
            SHIPMENT_STATUSES,
            weights=[20, 65, 10, 5],
            k=1,
        )[0]

        if shipment_status in ["DELIVERED", "DELAYED"]:

            delivery_date = shipment_date + timedelta(
                days=random.randint(1, 10)
            )

        shipments.append(
            {
                "shipment_id": f"SHP{shipment_counter:08d}",
                "order_id": order["order_id"],
                "warehouse_id": order["warehouse_id"],
                "carrier": random.choice(CARRIERS),
                "shipment_date": shipment_date.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "delivery_date": (
                    delivery_date.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    if delivery_date
                    else ""
                ),
                "shipping_status": shipment_status,
                "shipping_cost": round(
                    random.uniform(50, 1500),
                    2,
                ),
            }
        )

        shipment_counter += 1

    # --------------------------------------------------------
    # Inject duplicate shipment records
    # --------------------------------------------------------

    duplicate_count = max(
        1,
        int(len(shipments) * 0.003),
    )

    duplicates = random.sample(
        shipments,
        duplicate_count,
    )

    shipments.extend(duplicates)

    # --------------------------------------------------------
    # Inject missing delivery dates
    # --------------------------------------------------------

    missing_delivery_count = max(
        1,
        int(len(shipments) * 0.005),
    )

    for record in random.sample(
        shipments,
        missing_delivery_count,
    ):
        if record["shipping_status"] == "DELIVERED":
            record["delivery_date"] = ""

    write_csv(
        "shipments.csv",
        shipments,
        [
            "shipment_id",
            "order_id",
            "warehouse_id",
            "carrier",
            "shipment_date",
            "delivery_date",
            "shipping_status",
            "shipping_cost",
        ],
    )

    return shipments


# ============================================================
# MAIN
# ============================================================


def main():

    print("=" * 70)
    print("SUPPLY CHAIN DATA GENERATOR")
    print("=" * 70)

    print(f"\nOutput directory:")
    print(OUTPUT_DIR)

    print("\nGenerating datasets...\n")

    # --------------------------------------------------------
    # Master datasets
    # --------------------------------------------------------

    suppliers = generate_suppliers()

    products = generate_products(
        suppliers
    )

    warehouses = generate_warehouses()

    customers = generate_customers()

    # --------------------------------------------------------
    # Transaction datasets
    # --------------------------------------------------------

    orders = generate_orders(
        customers,
        products,
        warehouses,
    )

    inventory = generate_inventory(
        products,
        warehouses,
    )

    shipments = generate_shipments(
        orders
    )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("DATA GENERATION COMPLETE")
    print("=" * 70)

    print("\nDataset Summary:")
    print(
        f"Suppliers      : {len(suppliers):,}"
    )
    print(
        f"Products       : {len(products):,}"
    )
    print(
        f"Warehouses     : {len(warehouses):,}"
    )
    print(
        f"Customers      : {len(customers):,}"
    )
    print(
        f"Orders         : {len(orders):,}"
    )
    print(
        f"Inventory      : {len(inventory):,}"
    )
    print(
        f"Shipments      : {len(shipments):,}"
    )

    print("\nFiles generated in:")
    print(OUTPUT_DIR)

    print("=" * 70)


if __name__ == "__main__":
    main()