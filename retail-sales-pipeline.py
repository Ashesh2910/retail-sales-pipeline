"""
Retail Sales Data Pipeline (portable Python version)
"""

from pathlib import Path
from io import StringIO

import random
from datetime import date, timedelta


def generate_sample_dataset(num_rows=1500):
    regions = ["West", "South", "North", "East"]
    categories = {
        "Furniture": ["Chair", "Table", "Sofa"],
        "Technology": ["Laptop", "Phone", "Accessory"],
        "Office Supplies": ["Paper", "Pen", "Storage"],
    }
    lines = ["order_id,customer_id,product_id,region,order_date,quantity,sales,profit,category,sub_category"]
    start = date(2026, 1, 1)
    for i in range(1, num_rows + 1):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])
        sales = random.randint(100, 2000)
        quantity = random.randint(1, 5)
        profit = round(sales * random.uniform(0.08, 0.22), 2)
        order_date = start + timedelta(days=i % 90)
        lines.append(
            f"{1000+i},C{(i%300)+1:03d},P{(i%120)+1:03d},{random.choice(regions)},{order_date},{quantity},{sales},{profit},{category},{sub_category}"
        )
    return "\n".join(lines)


SAMPLE_DATASET = generate_sample_dataset(1500)

OUTPUT = Path("data")
for folder in (OUTPUT / "bronze", OUTPUT / "silver", OUTPUT / "gold"):
    folder.mkdir(parents=True, exist_ok=True)


def run_pipeline():
    try:
        import pandas as pd

        bronze_df = pd.read_csv(StringIO(SAMPLE_DATASET))
        bronze_df["ingestion_ts"] = pd.Timestamp.now()
        bronze_df.to_csv(OUTPUT / "bronze" / "retail_sales.csv", index=False)

        silver_df = bronze_df.drop_duplicates(subset=["order_id"]).copy()
        silver_df["sales"] = pd.to_numeric(silver_df["sales"], errors="coerce").fillna(0)
        silver_df["quantity"] = pd.to_numeric(silver_df["quantity"], errors="coerce").fillna(1)
        silver_df["profit"] = pd.to_numeric(silver_df["profit"], errors="coerce").fillna(0)
        silver_df["order_date"] = pd.to_datetime(silver_df["order_date"], errors="coerce")
        silver_df["revenue"] = (silver_df["sales"] * silver_df["quantity"]).round(2)
        silver_df["customer_segment"] = silver_df["sales"].apply(
            lambda value: "High Value" if value > 1000 else "Medium Value" if value > 500 else "Low Value"
        )
        silver_df.to_csv(OUTPUT / "silver" / "cleaned_sales.csv", index=False)

        fact_sales = silver_df[
            ["order_id", "customer_id", "product_id", "region", "order_date", "quantity", "sales", "profit", "revenue"]
        ].copy()
        fact_sales.to_csv(OUTPUT / "gold" / "fact_sales.csv", index=False)

        kpi_df = silver_df.groupby("region", as_index=False).agg(
            total_sales=("sales", "sum"),
            avg_profit=("profit", "mean"),
            unique_customers=("customer_id", "nunique"),
        )
        kpi_df.to_csv(OUTPUT / "gold" / "kpi_dashboard.csv", index=False)

        assert len(fact_sales) >= 1000
        assert float(kpi_df["total_sales"].sum()) > 0
        print("Pipeline completed successfully with pandas")
        return
    except ModuleNotFoundError:
        pass

    import csv

    rows = list(csv.DictReader(StringIO(SAMPLE_DATASET)))

    with open(OUTPUT / "bronze" / "retail_sales.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    seen = set()
    silver_rows = []
    for row in rows:
        order_id = row.get("order_id")
        if order_id in seen:
            continue
        seen.add(order_id)
        clean_row = dict(row)
        sales = float(clean_row.get("sales") or 0)
        quantity = float(clean_row.get("quantity") or 1)
        clean_row["revenue"] = round(sales * quantity, 2)
        clean_row["customer_segment"] = (
            "High Value" if sales > 1000 else "Medium Value" if sales > 500 else "Low Value"
        )
        silver_rows.append(clean_row)

    with open(OUTPUT / "silver" / "cleaned_sales.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(silver_rows[0].keys()))
        writer.writeheader()
        writer.writerows(silver_rows)

    fact_fields = ["order_id", "customer_id", "product_id", "region", "order_date", "quantity", "sales", "profit", "revenue"]
    with open(OUTPUT / "gold" / "fact_sales.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fact_fields)
        writer.writeheader()
        for row in silver_rows:
            writer.writerow({field: row[field] for field in fact_fields})

    totals = sum(float(row["sales"]) for row in silver_rows)
    assert len(silver_rows) >= 1000
    assert totals > 0
    print("Pipeline completed successfully with csv")


run_pipeline()

README_MD = """
# Retail Sales Data Pipeline with PySpark & Databricks
Portable project version with pandas fallback.
Architecture: Raw CSV -> Bronze -> Silver -> Gold -> KPI Dashboard -> Power BI
"""

ARCHITECTURE_DIAGRAM = """
Raw CSV -> Bronze -> Silver -> Gold -> KPI Dashboard
"""

POWER_BI_KPIS = [
    "Total Sales",
    "Repeat Customer %",
    "Average Basket Size",
    "Regional Growth",
    "Top Product Margin",
    "Customer Churn Risk Proxy",
]
