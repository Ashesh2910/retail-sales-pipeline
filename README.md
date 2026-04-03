# 🚀 Retail Sales Data Pipeline | Databricks + PySpark + Unity Catalog

<img width="1536" height="1024" alt="Data Pipeline Architecture" src="https://github.com/user-attachments/assets/129b93dc-1874-46a8-989e-f3677330c096" />

Enterprise-style **Retail Lakehouse Data Pipeline** built using **Databricks, PySpark, Unity Catalog, and Medallion Architecture (Bronze → Silver → Gold)**.

This project simulates how consulting teams build scalable analytics datasets for **regional profitability reporting, KPI dashboards, and BI consumption**.

---

## 📌 Business Problem

Retail organizations often struggle with:

* fragmented raw transaction data
* duplicate orders
* inconsistent sales metrics
* no centralized KPI reporting layer
* poor visibility into regional profitability
* non-reusable datasets for BI teams

This project solves that using a **production-style Databricks lakehouse workflow**.

---

## 🏗️ Architecture

```text
Raw Retail Data
   ↓
Bronze Layer (raw ingestion)
   ↓
Silver Layer (cleaning + transformations)
   ↓
Gold Layer (fact tables + KPI aggregation)
   ↓
Power BI / Dashboard Consumption
```

---

## ⚙️ Tech Stack

* **Databricks**
* **PySpark**
* **Unity Catalog**
* **SQL-style aggregations**
* **Dimensional Modelling**
* **Fact + KPI tables**
* **Medallion Architecture**
* **CSV / Lakehouse datasets**

---

## 📂 Data Layers

```text
data/
 ├── bronze/
 │   └── retail_sales.csv
 ├── silver/
 │   └── cleaned_sales.csv
 └── gold/
     ├── dim_customer.csv
     ├── dim_product.csv
     ├── dim_region.csv
     ├── dim_date.csv
     ├── fact_sales.csv
     └── kpi_dashboard.csv
```

---

## 📊 Databricks Managed Tables

Created in **workspace.default**

* `retail_sales`
* `silver_sales`
* `fact_sales`
* `kpi_dashboard`

---

## 📸 Deployment Proof

### ✅ Unity Catalog Managed Tables

![Unity Catalog Tables](./Screenshot%202026-04-03%20231001.png)

---

### ✅ Silver Layer Transformation Output

Shows cleaned records, typed columns, and normalized transaction structure.

![Silver Layer](./Screenshot%202026-04-03%20231241.png)

---

### ✅ Gold Fact Sales Table

Business-ready fact table with revenue logic and transaction-level metrics.

![Fact Sales](./Screenshot%202026-04-03%20231328.png)

---

### ✅ KPI Dashboard Aggregation

Regional KPI summary for dashboard consumption.

![KPI Dashboard](./Screenshot%202026-04-03%20231412.png)

---

## 📈 Business KPIs Generated

* total regional sales
* average profit
* unique customer count
* revenue per order
* profitability by region
* reusable BI dataset outputs

---

## 💼 Business Impact

This pipeline enables:

* regional sales intelligence
* profitability optimization
* customer segmentation readiness
* Power BI dashboard acceleration
* reusable enterprise analytics datasets
* scalable lakehouse data engineering workflows

---

## 📌 Project Outcomes

* Processed **1500+ retail transactions**
* Created **4 Databricks managed tables**
* Built **Bronze → Silver → Gold lakehouse flow**
* Reduced duplicate order noise using transformation layer
* Enabled **regional KPI dashboards**
* Delivered **Power BI-ready gold datasets**

---

## 👨‍💻 Author

**Ashesh**
Data Analytics | BI | Data Engineering | AI-driven Analytics
