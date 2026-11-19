
import csv
import random
from pathlib import Path
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation

SEED = 2026
TOTAL_ROWS = 1_000_000
rng = random.Random(SEED)

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

ORDERS_FILE = DATA_DIR / "orders.csv"
ORDERS_REPORT = ROOT / "generator" / "orders_dirty_report.csv"

OUTPUT_FILE = DATA_DIR / "transactions.csv"
REPORT_FILE = ROOT / "generator" / "transactions_dirty_report.csv"

# 1. Read the orders generation report
with open(ORDERS_REPORT, newline="", encoding="utf-8") as f:
    order_report = {
        row["anomaly_type"]: int(row["injected_count"])
        for row in csv.DictReader(f)
    }

ORDER_BASE_ROWS = (
    1_000_000
    - order_report["exact_duplicate_rows"]
    - order_report["duplicate_business_keys"]
)

# 2. Collect usable source orders for payment generation
usable_orders = []

with open(ORDERS_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for index, row in enumerate(reader, start=1):
        if index > ORDER_BASE_ROWS:
            break

        try:
            order_date = date.fromisoformat(row["order_date"])
            quantity = int(row["quantity"])
            price = Decimal(row["unit_price"])
        except (ValueError, InvalidOperation):
            continue

        if (
            not row["customer_id"].strip()
            or quantity <= 0
            or price <= 0
            or order_date > date(2026, 11, 19)
        ):
            continue

        usable_orders.append((
            row["order_id"],
            order_date,
            price * quantity,
            row["country"]
        ))

assert usable_orders, "No usable orders found!"

# 3. Choose reproducible non-rounded anomaly counts
anomaly_counts = {
    "missing_txn_id": rng.randint(650, 1350),
    "missing_order_id": rng.randint(650, 1350),
    "invalid_amount_text": rng.randint(650, 1350),
    "negative_amount": rng.randint(650, 1350),
    "invalid_timestamp": rng.randint(650, 1350),
    "future_timestamp": rng.randint(650, 1350),
    "unsupported_currency": rng.randint(650, 1350),
    "inconsistent_status": rng.randint(650, 1350),
    "orphan_order_id": rng.randint(650, 1350),
}

EXACT_DUPLICATES = rng.randint(3800, 6200)
KEY_DUPLICATES = rng.randint(3800, 6200)

BASE_ROWS = TOTAL_ROWS - EXACT_DUPLICATES - KEY_DUPLICATES

# 4. Distribute anomalies across random row positions
positions = rng.sample(
    range(1, BASE_ROWS + 1),
    sum(anomaly_counts.values())
)

anomaly_by_row = {}
offset = 0

for anomaly, count in anomaly_counts.items():
    for index in positions[offset:offset + count]:
        anomaly_by_row[index] = anomaly
    offset += count

columns = [
    "txn_id",
    "order_id",
    "payment_method",
    "amount",
    "currency",
    "txn_status",
    "txn_ts",
]

currency_by_country = {
    "Bangladesh": "BDT",
    "India": "INR",
    "USA": "USD",
    "UK": "GBP",
}

payment_methods = [
    "CARD",
    "BANK_TRANSFER",
    "MOBILE_BANKING",
    "CASH_ON_DELIVERY",
]

statuses = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]

duplicate_samples = []

# 5. Write the transactions file
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(columns)

    for i in range(1, BASE_ROWS + 1):

        order_id, order_date, order_value, country = (
            rng.choice(usable_orders)
        )

        # Full payment or a partial payment
        portion = rng.choice([
            Decimal("1.00"),
            Decimal("0.50")
        ])

        amount = (order_value * portion).quantize(
            Decimal("0.01")
        )

        # Normal transaction time follows the order date
        timestamp = datetime.combine(
            order_date,
            time(hour=rng.randint(0, 23))
        ) + timedelta(
            hours=rng.randint(0, 48),
            minutes=rng.randint(0, 59)
        )

        row = [
            f"T{i:07d}",
            order_id,
            rng.choice(payment_methods),
            str(amount),
            currency_by_country[country],
            rng.choices(
                statuses,
                weights=[75, 12, 10, 3],
                k=1
            )[0],
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        ]

        # 6. Inject one designated anomaly when selected
        anomaly = anomaly_by_row.get(i)

        if anomaly == "missing_txn_id":
            row[0] = ""

        elif anomaly == "missing_order_id":
            row[1] = ""

        elif anomaly == "invalid_amount_text":
            row[3] = "abc"

        elif anomaly == "negative_amount":
            row[3] = "-25.00"

        elif anomaly == "invalid_timestamp":
            row[6] = "not-a-date"

        elif anomaly == "future_timestamp":
            row[6] = "2099-01-01 10:00:00"

        elif anomaly == "unsupported_currency":
            row[4] = "XYZ"

        elif anomaly == "inconsistent_status":
            row[5] = f" {row[5].lower()} "

        elif anomaly == "orphan_order_id":
            row[1] = f"O9{i:06d}"

        # Keep clean samples for deliberate duplicates
        if (
            i > 10000
            and anomaly is None
            and len(duplicate_samples)
                < max(EXACT_DUPLICATES, KEY_DUPLICATES)
        ):
            duplicate_samples.append(row.copy())

        writer.writerow(row)

    assert len(duplicate_samples) >= max(
        EXACT_DUPLICATES, KEY_DUPLICATES
    )

    # 7. Add exact duplicates
    for row in duplicate_samples[:EXACT_DUPLICATES]:
        writer.writerow(row)

    # 8. Add duplicate transaction keys with changed methods
    for row in duplicate_samples[:KEY_DUPLICATES]:
        modified = row.copy()

        modified[2] = (
            "BANK_TRANSFER"
            if modified[2] != "BANK_TRANSFER"
            else "CARD"
        )

        writer.writerow(modified)

# 9. Write anomaly inventory
report_counts = {
    **anomaly_counts,
    "exact_duplicate_rows": EXACT_DUPLICATES,
    "duplicate_business_keys": KEY_DUPLICATES,
}

with open(REPORT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "anomaly_type",
        "injected_count",
        "percentage"
    ])

    for name, count in report_counts.items():
        writer.writerow([
            name,
            count,
            round(count / TOTAL_ROWS * 100, 4)
        ])

print(f"Generated {TOTAL_ROWS:,} transactions")
print(f"Base records: {BASE_ROWS:,}")
print(f"Exact duplicates: {EXACT_DUPLICATES:,}")
print(f"Key duplicates: {KEY_DUPLICATES:,}")
print(f"Usable source orders: {len(usable_orders):,}")
print(f"CSV file: {OUTPUT_FILE}")
print(f"Dirty-data report: {REPORT_FILE}")
