"""
DA-friendly data generation entry script.

This script keeps output data identical to `generate_data.py`
by reusing the same generation functions and write logic.

Usage:
    python data_generator/generate_data_da.py
"""

from generate_data import (
    generate_bookings,
    generate_customers,
    generate_hotels,
    save_parquet,
)


def main() -> None:
    print("=" * 60)
    print("  Booking Migration Demo - Data Generator (Simple)")
    print("=" * 60)

    hotels_df = generate_hotels()
    hotels_path = save_parquet(hotels_df, "hotels")
    print(f"[hotels] Saved to: {hotels_path}\n")

    customers_df = generate_customers()
    customers_path = save_parquet(customers_df, "customers")
    print(f"[customers] Saved to: {customers_path}\n")

    hotel_ids = hotels_df["hotel_id"].tolist()
    customer_ids = customers_df["customer_id"].tolist()
    bookings_df = generate_bookings(hotel_ids, customer_ids)
    bookings_path = save_parquet(bookings_df, "bookings")
    print(f"[bookings] Saved to: {bookings_path}\n")

    print("=" * 60)
    print("  Data generation complete. Summary:")
    print(f"  hotels    : {len(hotels_df):>5} rows  -> {hotels_path}")
    print(f"  customers : {len(customers_df):>5} rows  -> {customers_path}")
    print(f"  bookings  : {len(bookings_df):>5} rows  -> {bookings_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
