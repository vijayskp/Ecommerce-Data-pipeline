from faker import Faker
import random
from datetime import datetime, timedelta
import mysql.connector

fake = Faker()

# -------------------------------------------------------------------
# MySQL connection
# -------------------------------------------------------------------

def get_mysql_conn():
    """
    Adjust host/user/password/database to match your MySQL instance.
    """
    return mysql.connector.connect(
        host="localhost",
        user="vijay",
        password="Rohit@45",
        database="ecommerce_source",
        port=3307
    )

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def random_datetime(start: datetime, end: datetime) -> datetime:
    """Return a random datetime between start and end."""
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)

# -------------------------------------------------------------------
# Data generation functions
# -------------------------------------------------------------------

def generate_customers(n=5000):
    """
    Generate a list of customer rows as tuples matching:
    (customer_id, first_name, last_name, email, signup_date, country, city, status, updated_at)
    """
    countries = ["United States", "Canada", "United Kingdom", "Germany", "Australia", "India"]
    statuses = ["active", "inactive"]

    customers = []
    now = datetime.utcnow()
    start_signup = now - timedelta(days=730)  # last ~2 years

    for customer_id in range(1, n + 1):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}{customer_id}@example.com"

        signup_date = random_datetime(start_signup, now)
        country = random.choice(countries)
        city = fake.city()
        status = random.choices(statuses, weights=[0.8, 0.2])[0]

        # updated_at on or after signup_date
        updated_at = random_datetime(signup_date, now)

        customers.append(
            (
                customer_id,
                first_name,
                last_name,
                email,
                signup_date,
                country,
                city,
                status,
                updated_at,
            )
        )

    return customers


def generate_products(n=500):
    """
    Generate a list of product rows as tuples matching:
    (product_id, product_name, category, price, currency, updated_at)
    """
    categories = [
        "Electronics",
        "Clothing",
        "Home & Kitchen",
        "Sports",
        "Books",
        "Beauty",
        "Toys",
    ]
    currency = "USD"

    products = []
    now = datetime.utcnow()
    start_date = now - timedelta(days=730)

    for product_id in range(1, n + 1):
        product_name = f"{fake.word().title()} {fake.word().title()}"
        category = random.choice(categories)
        price = round(random.uniform(5, 500), 2)
        updated_at = random_datetime(start_date, now)

        products.append(
            (
                product_id,
                product_name,
                category,
                price,
                currency,
                updated_at,
            )
        )

    return products


def generate_orders_and_order_items(
    n_orders,
    customer_ids,
    products,
    min_items_per_order=1,
    max_items_per_order=5
):
    """
    Generate orders and order_items together so that order totals
    match the sum of their items.

    Returns:
        orders: list of tuples matching:
            (order_id, customer_id, order_date, order_status, total_amount, currency, updated_at)
        order_items: list of tuples matching:
            (order_item_id, order_id, product_id, quantity, unit_price, currency, updated_at)
    """
    statuses = ["completed", "shipped", "cancelled", "returned", "pending"]
    status_weights = [0.6, 0.2, 0.05, 0.05, 0.1]
    currency = "USD"

    now = datetime.utcnow()
    start_date = now - timedelta(days=730)

    # Build lookup for product prices
    # products tuples: (product_id, product_name, category, price, currency, updated_at)
    price_lookup = {p[0]: p[3] for p in products}
    product_ids = list(price_lookup.keys())

    orders = []
    order_items = []
    order_item_id = 1

    for order_id in range(1, n_orders + 1):
        customer_id = random.choice(customer_ids)
        order_date = random_datetime(start_date, now)
        order_status = random.choices(statuses, weights=status_weights)[0]

        num_items = random.randint(min_items_per_order, max_items_per_order)
        total_amount = 0.0

        for _ in range(num_items):
            product_id = random.choice(product_ids)
            base_price = price_lookup[product_id]

            # Add small variation to price
            unit_price = round(base_price * random.uniform(0.9, 1.1), 2)
            quantity = random.randint(1, 5)
            line_amount = unit_price * quantity
            total_amount += line_amount

            updated_at_item = order_date  # simple: align with order_date

            order_items.append(
                (
                    order_item_id,
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                    currency,
                    updated_at_item,
                )
            )
            order_item_id += 1

        total_amount = round(total_amount, 2)
        updated_at_order = order_date

        orders.append(
            (
                order_id,
                customer_id,
                order_date,
                order_status,
                total_amount,
                currency,
                updated_at_order,
            )
        )

    return orders, order_items

# -------------------------------------------------------------------
# Insert functions
# -------------------------------------------------------------------

def insert_customers(customers):
    conn = get_mysql_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO customers
        (customer_id, first_name, last_name, email, signup_date, country, city, status, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(sql, customers)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(customers)} customers.")


def insert_products(products):
    conn = get_mysql_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO products
        (product_id, product_name, category, price, currency, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.executemany(sql, products)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(products)} products.")


def insert_orders(orders):
    conn = get_mysql_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO orders
        (order_id, customer_id, order_date, order_status, total_amount, currency, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(sql, orders)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(orders)} orders.")


def insert_order_items(order_items):
    conn = get_mysql_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO order_items
        (order_item_id, order_id, product_id, quantity, unit_price, currency, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(sql, order_items)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(order_items)} order_items.")

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

if __name__ == "__main__":
    random.seed(42)  # for reproducibility

    # 1) Generate data in memory
    print("Generating customers...")
    customers = generate_customers(n=5000)

    print("Generating products...")
    products = generate_products(n=500)

    customer_ids = [c[0] for c in customers]   # customer_id position in tuple
    print("Generating orders and order_items...")
    orders, order_items = generate_orders_and_order_items(
        n_orders=20000,
        customer_ids=customer_ids,
        products=products,
        min_items_per_order=1,
        max_items_per_order=5
    )

    # 2) Insert into MySQL in FK-safe order
    # NOTE: If you get duplicate key errors, TRUNCATE the tables first or reset DB.
    print("Inserting customers...")
    insert_customers(customers)

    print("Inserting products...")
    insert_products(products)

    print("Inserting orders...")
    insert_orders(orders)

    print("Inserting order_items...")
    insert_order_items(order_items)

    print("Sample data generation completed.")
