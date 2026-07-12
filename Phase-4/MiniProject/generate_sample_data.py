"""
generate_sample_data.py

Creates a synthetic sales dataset (data/sales.csv) so the pipeline can be
run end-to-end without needing a real production data source.

Columns:
    order_id      - unique order identifier
    customer_id   - customer identifier (some rows intentionally null)
    customer_name - customer display name
    city          - city of the order
    order_date    - date of the order (YYYY-MM-DD)
    amount        - order amount (some rows intentionally negative / invalid)

Run:
    python data/generate_sample_data.py
"""

import csv
import random
from datetime import date, timedelta

random.seed(42)

CITIES = ["Hyderabad", "Bengaluru", "Chennai", "Mumbai", "Delhi", "Pune"]
CUSTOMERS = [
    (1, "Customer_A"), (2, "Customer_B"), (3, "Customer_C"), (4, "Customer_D"),
    (5, "Customer_E"), (6, "Customer_F"), (7, "Customer_G"), (8, "Customer_H"),
    (9, "Customer_I"), (10, "Customer_J"),
]

START_DATE = date(2025, 1, 1)
NUM_ROWS = 500
OUTPUT_PATH = "data/sales.csv"


def random_date():
    offset = random.randint(0, 59)
    return (START_DATE + timedelta(days=offset)).isoformat()


def build_rows(n):
    rows = []
    for i in range(1, n + 1):
        cust_id, cust_name = random.choice(CUSTOMERS)

        # Inject some dirty data on purpose, so the cleaning step has real work to do
        if random.random() < 0.04:          # ~4% missing customer_id
            cust_id = ""
        amount = round(random.uniform(50, 5000), 2)
        if random.random() < 0.03:          # ~3% negative / invalid amounts
            amount = -amount

        rows.append({
            "order_id": i,
            "customer_id": cust_id,
            "customer_name": cust_name,
            "city": random.choice(CITIES),
            "order_date": random_date(),
            "amount": amount,
        })

    # Inject a handful of exact duplicate rows on purpose
    for _ in range(10):
        rows.append(dict(random.choice(rows)))

    return rows


def main():
    rows = build_rows(NUM_ROWS)
    fieldnames = ["order_id", "customer_id", "customer_name", "city", "order_date", "amount"]

    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
