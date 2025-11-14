import os
import random
from datetime import timedelta, datetime

import numpy as np
import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)
np.random.seed(42)

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_USERS = 200
NUM_PRODUCTS = 150
NUM_ORDERS = 180
NUM_ORDER_ITEMS = 400
NUM_PAYMENTS = 220

def build_users(n=NUM_USERS):
    records = []
    for user_id in range(1, n + 1):
        name = fake.name()
        first, last = name.split(" ")[0], name.split(" ")[-1]
        records.append(
            {
                "user_id": user_id,
                "first_name": first,
                "last_name": last,
                "email": fake.unique.email(),
                "phone": fake.phone_number(),
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "postal_code": fake.postcode(),
                "country": fake.country(),
                "date_joined": fake.date_between(start_date="-3y", end_date="today"),
                "is_active": fake.boolean(chance_of_getting_true=85),
            }
        )
    return pd.DataFrame(records)

def build_products(n=NUM_PRODUCTS):
    categories = ["Electronics", "Home", "Fashion", "Sports", "Beauty", "Books", "Toys"]
    records = []
    for product_id in range(1, n + 1):
        price = round(random.uniform(5, 800), 2)
        records.append(
            {
                "product_id": product_id,
                "name": fake.sentence(nb_words=3).replace(".", ""),
                "category": random.choice(categories),
                "brand": fake.company(),
                "price": price,
                "stock": random.randint(0, 500),
                "weight_kg": round(random.uniform(0.1, 10), 2),
                "created_at": fake.date_between(start_date="-2y", end_date="today"),
                "is_active": fake.boolean(chance_of_getting_true=90),
            }
        )
    return pd.DataFrame(records)

def build_orders(n=NUM_ORDERS, users_df=None):
    statuses = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
    records = []
    for order_id in range(1, n + 1):
        user_id = random.randint(1, len(users_df))
        order_date = fake.date_time_between(start_date="-18m", end_date="now")
        ship_date = order_date + timedelta(days=random.randint(1, 7))
        records.append(
            {
                "order_id": order_id,
                "user_id": user_id,
                "order_date": order_date,
                "ship_date": ship_date if random.random() > 0.1 else None,
                "status": random.choices(
                    statuses, weights=[15, 20, 20, 30, 10, 5], k=1
                )[0],
                "shipping_address": fake.street_address(),
                "shipping_city": fake.city(),
                "shipping_state": fake.state_abbr(),
                "shipping_postal_code": fake.postcode(),
                "shipping_country": fake.country(),
            }
        )
    return pd.DataFrame(records)

def build_order_items(n=NUM_ORDER_ITEMS, orders_df=None, products_df=None):
    records = []
    for item_id in range(1, n + 1):
        order_id = random.randint(1, len(orders_df))
        product_id = random.randint(1, len(products_df))
        product_price = products_df.loc[product_id - 1, "price"]
        quantity = random.randint(1, 5)
        discount = round(random.uniform(0, 0.25), 2)
        records.append(
            {
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": product_price,
                "discount_rate": discount,
                "line_total": round(quantity * product_price * (1 - discount), 2),
            }
        )
    return pd.DataFrame(records)

def build_payments(n=NUM_PAYMENTS, orders_df=None):
    methods = ["card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
    statuses = ["authorized", "captured", "refunded", "failed"]
    records = []
    for payment_id in range(1, n + 1):
        order_id = random.randint(1, len(orders_df))
        amount = round(random.uniform(10, 1500), 2)
        payment_date = fake.date_time_between(start_date="-18m", end_date="now")
        records.append(
            {
                "payment_id": payment_id,
                "order_id": order_id,
                "amount": amount,
                "currency": "USD",
                "payment_method": random.choice(methods),
                "status": random.choices(statuses, weights=[50, 35, 10, 5], k=1)[0],
                "transaction_id": fake.uuid4(),
                "payment_date": payment_date,
            }
        )
    return pd.DataFrame(records)

def main():
    users_df = build_users()
    products_df = build_products()
    orders_df = build_orders(users_df=users_df)
    order_items_df = build_order_items(orders_df=orders_df, products_df=products_df)
    payments_df = build_payments(orders_df=orders_df)

    datasets = {
        "users.csv": users_df,
        "products.csv": products_df,
        "orders.csv": orders_df,
        "order_items.csv": order_items_df,
        "payments.csv": payments_df,
    }

    for filename, df in datasets.items():
        path = os.path.join(OUTPUT_DIR, filename)
        df.to_csv(path, index=False)
        print(f"Saved {len(df):4d} rows -> {path}")

if __name__ == "__main__":
    main()