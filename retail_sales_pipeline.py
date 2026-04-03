"""
Retail Sales Data Pipeline with PySpark & Dimensional Modeling
Supports: Databricks, PySpark, Pandas, and Pure CSV fallback
Implements: Star schema with dimension and fact tables
"""

from pathlib import Path
from io import StringIO
import random
from datetime import date, timedelta


def generate_sample_dataset(num_rows=1500):
    """Generate sample retail sales CSV data"""
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


def run_pipeline_pyspark():
    """PySpark + Dimensional Modeling implementation for Databricks/Spark environments"""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, row_number, to_date, when, broadcast
        from pyspark.sql.window import Window
        import pandas as pd

        spark = SparkSession.builder.appName("RetailSalesPipeline").getOrCreate()
        
        # Bronze Layer
        bronze_df = pd.read_csv(StringIO(SAMPLE_DATASET))
        bronze_spark = spark.createDataFrame(bronze_df)
        bronze_spark.write.mode("overwrite").csv(str(OUTPUT / "bronze" / "retail_sales_spark"), header=True)

        # Silver Layer - Data Cleaning
        silver_df = bronze_spark.dropDuplicates(["order_id"])
        silver_df = silver_df.withColumn("sales", col("sales").cast("double").fillna(0)) \
                             .withColumn("quantity", col("quantity").cast("double").fillna(1)) \
                             .withColumn("profit", col("profit").cast("double").fillna(0)) \
                             .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
                             .withColumn("revenue", (col("sales") * col("quantity")).cast("decimal(10,2)")) \
                             .withColumn("customer_segment", 
                                        when(col("sales") > 1000, "High Value")
                                        .when(col("sales") > 500, "Medium Value")
                                        .otherwise("Low Value"))
        
        silver_df.write.mode("overwrite").csv(str(OUTPUT / "silver" / "cleaned_sales_spark"), header=True)

        # Gold Layer - Dimensional Modeling (Star Schema)
        
        # Dimension: Customers
        dim_customer = silver_df.select(
            col("customer_id"),
            col("customer_segment")
        ).dropDuplicates(["customer_id"])
        dim_customer.write.mode("overwrite").csv(str(OUTPUT / "gold" / "dim_customer"), header=True)

        # Dimension: Products
        dim_product = silver_df.select(
            col("product_id"),
            col("category"),
            col("sub_category")
        ).dropDuplicates(["product_id"])
        dim_product.write.mode("overwrite").csv(str(OUTPUT / "gold" / "dim_product"), header=True)

        # Dimension: Regions
        dim_region = silver_df.select(col("region")).dropDuplicates().withColumn("region_id", row_number().over(Window.orderBy("region")))
        dim_region.write.mode("overwrite").csv(str(OUTPUT / "gold" / "dim_region"), header=True)

        # Dimension: Date
        dim_date = silver_df.select(col("order_date")).dropDuplicates().withColumn("year", col("order_date").substr(1, 4)) \
                                                                         .withColumn("month", col("order_date").substr(6, 2)) \
                                                                         .withColumn("day", col("order_date").substr(9, 2))
        dim_date.write.mode("overwrite").csv(str(OUTPUT / "gold" / "dim_date"), header=True)

        # Fact: Sales
        fact_sales = silver_df.select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("region"),
            col("order_date"),
            col("quantity"),
            col("sales"),
            col("profit"),
            col("revenue")
        )
        fact_sales.write.mode("overwrite").csv(str(OUTPUT / "gold" / "fact_sales_spark"), header=True)

        # KPI Dashboard
        kpi_df = silver_df.groupBy("region").agg(
            {"sales": "sum", "profit": "avg", "customer_id": "countDistinct"}
        ).withColumnRenamed("sum(sales)", "total_sales") \
         .withColumnRenamed("avg(profit)", "avg_profit") \
         .withColumnRenamed("count(DISTINCT customer_id)", "unique_customers")
        
        kpi_df.write.mode("overwrite").csv(str(OUTPUT / "gold" / "kpi_dashboard_spark"), header=True)

        assert fact_sales.count() >= 1000
        assert kpi_df.agg({"total_sales": "sum"}).collect()[0][0] > 0
        print("✓ Pipeline completed successfully with PySpark + Dimensional Modeling")
        spark.stop()
        return True
    except Exception as e:
        print(f"PySpark execution failed: {e}")
        return False


def run_pipeline_pandas():
    """Pandas implementation for local environments"""
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

    # Dimensional Modeling
    dim_customer = silver_df[["customer_id", "customer_segment"]].drop_duplicates()
    dim_customer.to_csv(OUTPUT / "gold" / "dim_customer.csv", index=False)

    dim_product = silver_df[["product_id", "category", "sub_category"]].drop_duplicates()
    dim_product.to_csv(OUTPUT / "gold" / "dim_product.csv", index=False)

    dim_region = silver_df[["region"]].drop_duplicates().reset_index(drop=True)
    dim_region["region_id"] = range(1, len(dim_region) + 1)
    dim_region.to_csv(OUTPUT / "gold" / "dim_region.csv", index=False)

    dim_date = silver_df[["order_date"]].drop_duplicates().sort_values("order_date").reset_index(drop=True)
    dim_date["year"] = dim_date["order_date"].dt.year
    dim_date["month"] = dim_date["order_date"].dt.month
    dim_date["day"] = dim_date["order_date"].dt.day
    dim_date["quarter"] = dim_date["order_date"].dt.quarter
    dim_date.to_csv(OUTPUT / "gold" / "dim_date.csv", index=False)

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
    print("✓ Pipeline completed successfully with Pandas + Dimensional Modeling")


def run_pipeline_csv():
    """Pure CSV implementation (no dependencies)"""
    import csv

    rows = list(csv.DictReader(StringIO(SAMPLE_DATASET)))

    with open(OUTPUT / "bronze" / "retail_sales_csv.csv", "w", newline="", encoding="utf-8") as file:
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

    with open(OUTPUT / "silver" / "cleaned_sales_csv.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(silver_rows[0].keys()))
        writer.writeheader()
        writer.writerows(silver_rows)

    # Dimensional tables
    customers = {}
    products = {}
    regions = set()
    dates = set()

    for row in silver_rows:
        customers[row["customer_id"]] = row["customer_segment"]
        products[row["product_id"]] = (row["category"], row["sub_category"])
        regions.add(row["region"])
        dates.add(row["order_date"])

    with open(OUTPUT / "gold" / "dim_customer_csv.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["customer_id", "customer_segment"])
        writer.writeheader()
        for cid, segment in customers.items():
            writer.writerow({"customer_id": cid, "customer_segment": segment})

    with open(OUTPUT / "gold" / "dim_product_csv.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["product_id", "category", "sub_category"])
        writer.writeheader()
        for pid, (cat, subcat) in products.items():
            writer.writerow({"product_id": pid, "category": cat, "sub_category": subcat})

    with open(OUTPUT / "gold" / "dim_region_csv.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["region_id", "region"])
        writer.writeheader()
        for idx, region in enumerate(sorted(regions), 1):
            writer.writerow({"region_id": idx, "region": region})

    with open(OUTPUT / "gold" / "fact_sales_csv.csv", "w", newline="", encoding="utf-8") as file:
        fact_fields = ["order_id", "customer_id", "product_id", "region", "order_date", "quantity", "sales", "profit", "revenue"]
        writer = csv.DictWriter(file, fieldnames=fact_fields)
        writer.writeheader()
        for row in silver_rows:
            writer.writerow({field: row[field] for field in fact_fields})

    totals = sum(float(row["sales"]) for row in silver_rows)
    assert len(silver_rows) >= 1000
    assert totals > 0
    print("✓ Pipeline completed successfully with CSV + Dimensional Modeling")


def run_pipeline():
    """Main orchestrator - tries PySpark first, falls back to Pandas, then CSV"""
    if run_pipeline_pyspark():
        return
    
    try:
        run_pipeline_pandas()
    except ModuleNotFoundError:
        run_pipeline_csv()


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
