
import csv
import random
from pathlib import Path
from datetime import date, timedelta

# Configuration
SEED = 42
TOTAL_ROWS = 1_000_000
rng = random.Random(SEED)

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = DATA_DIR / "orders.csv"
REPORT_FILE = ROOT / "generator" / "orders_dirty_report.csv"

# Generate reproducible, non-rounded anomaly counts
anomaly_counts = {
    "missing_customer_id": rng.randint(700, 1450),
    "invalid_quantity_text": rng.randint(700, 1450),
    "negative_quantity": rng.randint(700, 1450),
    "invalid_date": rng.randint(700, 1450),
    "future_date": rng.randint(700, 1450),
    "inconsistent_status": rng.randint(700, 1450),
    "negative_price": rng.randint(700, 1450),
}

EXACT_DUPLICATES = rng.randint(3800, 6200)
KEY_DUPLICATES = rng.randint(3800, 6200)

BASE_ROWS = TOTAL_ROWS - EXACT_DUPLICATES - KEY_DUPLICATES

# Randomly distribute errors without overlapping positions
selected_rows = iter(
    rng.sample(range(1, BASE_ROWS + 1), sum(anomaly_counts.values()))
)

anomaly_by_row = {}

for anomaly, count in anomaly_counts.items():
    for _ in range(count):
        anomaly_by_row[next(selected_rows)] = anomaly

categories = ["Electronics", "Clothing", "Books", "Home"]
countries = ["Bangladesh", "India", "USA", "UK"]
statuses = ["COMPLETED", "CANCELLED", "PENDING", "SHIPPED"]
prices = ["19.99", "49.50", "99.00", "199.99", "499.99"]

columns = [
    "order_id", "customer_id", "order_date", "category",
    "quantity", "unit_price", "order_status", "country"
]

duplicate_samples = []

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(columns)

    for i in range(1, BASE_ROWS + 1):
        order_date = date(2025, 1, 1) + timedelta(
            days=rng.randrange(600)
        )

        row = [
            f"O{i:07d}",
            f"C{rng.randint(1, 200000):06d}",
            str(order_date),
            rng.choice(categories),
            str(rng.randint(1, 5)),
            rng.choice(prices),
            rng.choice(statuses),
            rng.choice(countries),
        ]

        # Inject at most one designated anomaly per base row
        anomaly = anomaly_by_row.get(i)

        if anomaly == "missing_customer_id":
            row[1] = ""
        elif anomaly == "invalid_quantity_text":
            row[4] = "abc"
        elif anomaly == "negative_quantity":
            row[4] = "-3"
        elif anomaly == "invalid_date":
            row[2] = "2026-99-99"
        elif anomaly == "future_date":
            row[2] = "2099-01-01"
        elif anomaly == "inconsistent_status":
            row[6] = " completed "
        elif anomaly == "negative_price":
            row[5] = "-9.99"

        # Save clean rows to use for controlled duplicates
        if (
            i > 20000
            and anomaly is None
            and len(duplicate_samples) < max(EXACT_DUPLICATES, KEY_DUPLICATES)
        ):
            duplicate_samples.append(row.copy())

        writer.writerow(row)

    # Add exact duplicates
    for row in duplicate_samples[:EXACT_DUPLICATES]:
        writer.writerow(row)

    # Add same-key rows with a different quantity
    for row in duplicate_samples[:KEY_DUPLICATES]:
        modified = row.copy()
        modified[4] = str(int(modified[4]) + 1)
        writer.writerow(modified)

# Create the injection inventory report
with open(REPORT_FILE, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["anomaly_type", "injected_count", "percentage"])

    report_counts = {
        **anomaly_counts,
        "exact_duplicate_rows": EXACT_DUPLICATES,
        "duplicate_business_keys": KEY_DUPLICATES,
    }

    for name, count in report_counts.items():
        writer.writerow([
            name,
            count,
            round(count / TOTAL_ROWS * 100, 4),
        ])

print(f"Generated {TOTAL_ROWS:,} orders")
print(f"Base records: {BASE_ROWS:,}")
print(f"Exact duplicates: {EXACT_DUPLICATES:,}")
print(f"Key duplicates: {KEY_DUPLICATES:,}")
print(f"CSV file: {OUTPUT_FILE}")
print(f"Dirty-data report: {REPORT_FILE}")
