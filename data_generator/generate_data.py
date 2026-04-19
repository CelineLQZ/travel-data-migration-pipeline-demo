"""
generate_data.py
Generates synthetic hotel booking data and saves it as Parquet files.

Outputs (written to local/hdfs_simulation/):
  - hotels/hotels.parquet      (50 rows)
  - customers/customers.parquet (200 rows)
  - bookings/bookings.parquet  (500 rows)

Usage:
    python data_generator/generate_data.py
"""

import os
import random
from datetime import date, timedelta

import pandas as pd
from faker import Faker

# --- Config ---
OUTPUT_BASE_DIR = os.path.join("local", "hdfs_simulation")
NUM_HOTELS    = 50
NUM_CUSTOMERS = 200
NUM_BOOKINGS  = 500
RANDOM_SEED   = 42

HOTELS_NULL_CITY       = 3
HOTELS_ZERO_STAR       = 2
CUSTOMERS_NULL_EMAIL   = 10
CUSTOMERS_BAD_DATE     = 5
BOOKINGS_INVALID_STATUS = 20
BOOKINGS_NULL_PRICE    = 10

VALID_STATUSES = [1, 2, 3]
CURRENCIES     = ["USD", "EUR", "GBP", "JPY", "AUD"]

fake = Faker()
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)


# --- Hotels ---
print("[hotels] Generating hotel records...")
hotels = []
for hotel_id in range(1, NUM_HOTELS + 1):
    hotels.append({
        "hotel_id":     hotel_id,
        "name":         fake.company() + " Hotel",
        "city":         fake.city(),
        "country_code": random.randint(1, 999),
        "star_rating":  random.randint(1, 5),
    })
hotels_df = pd.DataFrame(hotels)

null_city_idx = random.sample(range(NUM_HOTELS), HOTELS_NULL_CITY)
hotels_df.loc[null_city_idx, "city"] = None
print(f"[hotels]   Injected NULL city at rows: {null_city_idx}")

zero_star_idx = random.sample(range(NUM_HOTELS), HOTELS_ZERO_STAR)
hotels_df.loc[zero_star_idx, "star_rating"] = 0
print(f"[hotels]   Injected star_rating=0 at rows: {zero_star_idx}")

hotels_df["hotel_id"]     = hotels_df["hotel_id"].astype("int32")
hotels_df["country_code"] = hotels_df["country_code"].astype("int32")
hotels_df["star_rating"]  = hotels_df["star_rating"].astype("int32")
print(f"[hotels] Generated {len(hotels_df)} rows ({HOTELS_NULL_CITY} null city, {HOTELS_ZERO_STAR} zero star_rating)")


# --- Customers ---
print("\n[customers] Generating customer records...")
signup_start, signup_end = date(2018, 1, 1), date(2024, 1, 1)
signup_range = (signup_end - signup_start).days

customers = []
for customer_id in range(1, NUM_CUSTOMERS + 1):
    signup = signup_start + timedelta(days=random.randint(0, signup_range - 1))
    customers.append({
        "customer_id": customer_id,
        "name":        fake.name(),
        "email":       fake.email(),
        "signup_date": signup.strftime("%Y%m%d"),
    })
customers_df = pd.DataFrame(customers)

null_email_idx = random.sample(range(NUM_CUSTOMERS), CUSTOMERS_NULL_EMAIL)
customers_df.loc[null_email_idx, "email"] = None
print(f"[customers]   Injected NULL email at rows: {null_email_idx}")

bad_date_idx = random.sample(range(NUM_CUSTOMERS), CUSTOMERS_BAD_DATE)
bad_date_values = ["not-a-date", "2023/07/15", "15-06-2022", "20230230", "XXXXXXXX"]
for i, idx in enumerate(bad_date_idx):
    customers_df.at[idx, "signup_date"] = bad_date_values[i % len(bad_date_values)]
print(f"[customers]   Injected malformed signup_date at rows: {bad_date_idx}")

customers_df["customer_id"] = customers_df["customer_id"].astype("int32")
print(f"[customers] Generated {len(customers_df)} rows ({CUSTOMERS_NULL_EMAIL} null email, {CUSTOMERS_BAD_DATE} bad dates)")


# --- Bookings ---
print("\n[bookings] Generating booking records...")
checkin_start, checkin_end = date(2022, 1, 1), date(2024, 6, 1)
checkin_range = (checkin_end - checkin_start).days

hotel_ids    = hotels_df["hotel_id"].tolist()
customer_ids = customers_df["customer_id"].tolist()

bookings = []
for booking_id in range(1, NUM_BOOKINGS + 1):
    check_in  = checkin_start + timedelta(days=random.randint(0, checkin_range - 1))
    check_out = check_in + timedelta(days=random.randint(1, 14))
    bookings.append({
        "booking_id":     booking_id,
        "customer_id":    random.choice(customer_ids),
        "hotel_id":       random.choice(hotel_ids),
        "check_in":       check_in.strftime("%Y%m%d"),
        "check_out":      check_out.strftime("%Y%m%d"),
        "price":          round(random.uniform(50.0, 2000.0), 2),
        "currency":       random.choice(CURRENCIES),
        "booking_status": random.choice(VALID_STATUSES),
    })
bookings_df = pd.DataFrame(bookings)

invalid_status_idx = random.sample(range(NUM_BOOKINGS), BOOKINGS_INVALID_STATUS)
bookings_df.loc[invalid_status_idx, "booking_status"] = 4
print(f"[bookings]   Injected booking_status=4 (INVALID) at rows: {invalid_status_idx}")

null_price_idx = random.sample(range(NUM_BOOKINGS), BOOKINGS_NULL_PRICE)
bookings_df.loc[null_price_idx, "price"] = None
print(f"[bookings]   Injected NULL price at rows: {null_price_idx}")

bookings_df["booking_id"]     = bookings_df["booking_id"].astype("int32")
bookings_df["customer_id"]    = bookings_df["customer_id"].astype("int32")
bookings_df["hotel_id"]       = bookings_df["hotel_id"].astype("int32")
bookings_df["booking_status"] = bookings_df["booking_status"].astype("int32")
print(f"[bookings] Generated {len(bookings_df)} rows ({BOOKINGS_INVALID_STATUS} invalid status, {BOOKINGS_NULL_PRICE} null price)")


# --- Save to Parquet ---
for name, df in [("hotels", hotels_df), ("customers", customers_df), ("bookings", bookings_df)]:
    out_dir  = os.path.join(OUTPUT_BASE_DIR, name)
    out_path = os.path.join(out_dir, f"{name}.parquet")
    os.makedirs(out_dir, exist_ok=True)
    df.to_parquet(out_path, index=False, engine="pyarrow")
    print(f"Saved {name} → {out_path}")

print("\nDone.")
