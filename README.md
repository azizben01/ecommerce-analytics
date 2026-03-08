## Advances Database Implementation and Design – Project Guide

This project is an end‑to‑end big‑data pipeline for an e‑commerce platform in Burkina Faso. It covers synthetic data generation, MongoDB + HBase storage, Spark batch analytics, and Python visualizations for the final report.

Everything below assumes your working directory is the project root and mine is:

```bash
cd /home/securityman/adbmsfat
```

---

## 1. Project Structure

- `scripts/`- contains all the files related to the functioning of the project
  - `dataset.py` – generate the synthetic e‑commerce dataset (users, products, categories, sessions, transactions).
  - `loadtomongo.py` – load JSON data into MongoDB collections.
  - `db_analytics.py` – MongoDB aggregation pipelines (product popularity, revenue by category, customer segmentation).
  - `hbase_operations.py` – create and query HBase tables (`user_sessions`, `product_metrics`) for time‑series analytics.
  - `spark_analytics.py` – main Spark batch job (product affinity, CLV by city, device performance, cohort analysis).
  - `integrated_analytics.py` – integrated Spark CLV query combining purchase revenue and engagement metrics.
  - `visualizations.py` – builds the charts used in the report from the JSON data (no Spark).
  - `listdb.py`, `mongo.py` – small helpers for listing/checking MongoDB data.

- `data/`- contains the description of all collections
  - `users.json` – user profiles with `geo_data`, registration dates, etc.
  - `products.json` – product catalog with `category_id`, `subcategory_id`, prices, and stock.
  - `categories.json` – category + subcategory metadata (used for catalog modeling).
  - `sessions.json` – browsing sessions, `device_profile`, `conversion_status`, `cart_contents`, etc.
  - `transactions.json` – (if used) transaction‑level view of purchases.

- `output/`
  - `product_pairs/`, `customer_value/`, `device_performance/`, `cohort_analysis/` – CSV outputs from `spark_analytics.py`.
  - `clv_analysis/` – CSV output from `integrated_analytics.py`.
  - `figures/` – PNG charts created by `visualizations.py` (revenue by category, funnel, device performance, top products).

- `images/`
  - Static diagrams and screenshots used in the written report (schema diagrams, sample outputs, etc.).

- Reports
  - `report.md` – main project report with all sections (MongoDB, HBase, Spark, integration, screenshots).
  - `ADDI-fat-report.md` – assessment answers in exam‑style format (Section A/B answers).

- Environment
  - `venv/` – Python virtual environment (contains PySpark and other dependencies).

---

## 2. Environment Setup

1. **Activate the virtual environment**:

```bash
source venv/bin/activate
```

2. (If needed) install requirements inside the venv:

```bash
pip install pyspark pandas matplotlib seaborn pymongo happybase
```

3. Make sure **MongoDB** and **HBase** are running (for the parts that use them):
- MongoDB: running locally on the default port or as configured in the scripts.
- HBase: running in Docker; enter the shell with:

```bash
docker exec -it hbase hbase shell
```

---

## 3. Data Generation and Loading

### 3.1 Generate the synthetic dataset

If you need to regenerate the JSON files under `data/`:

```bash
source venv/bin/activate
python scripts/dataset.py
```

This populates `data/users.json`, `data/products.json`, `data/categories.json`, `data/sessions.json`, and possibly `data/transactions.json`.

### 3.2 Load data into MongoDB

```bash
source venv/bin/activate
python scripts/loadtomongo.py
```

This creates/populates MongoDB collections:
`users`, `products`, `categories`, `sessions`, `transactions`.

You can inspect them with:

```bash
python scripts/db_analytics.py      # runs sample aggregations
python scripts/listdb.py            # simple listing / checks (if implemented)
```

---

## 4. HBase Time‑Series Component

HBase is used for time‑series style analytics (sessions and product metrics).

1. Ensure HBase is running in Docker and open the shell:

```bash
docker exec -it hbase hbase shell
```

2. Run the Python script that connects to HBase and creates/populates tables:

```bash
source venv/bin/activate
python scripts/hbase_operations.py
```

This script is responsible for:
- Creating the `user_sessions` table (row key like `user_id|timestamp`, column family `cf`).
- Creating the `product_metrics` table (row key `product_id|date`, column families `daily`, `weekly`).
- Inserting or querying example rows as shown in the report.

---

## 5. Spark Batch Analytics

### 5.1 Main Spark analytics (`spark_analytics.py`)

Runs all four Spark analyses and writes CSV outputs under `output/`:

```bash
source venv/bin/activate
python scripts/spark_analytics.py
```

What it does:
- Reads `data/users.json`, `data/products.json`, `data/sessions.json`.
- Converts `cart_contents` (a struct) into line items and builds a `purchases` DataFrame.
- **Analysis 1**: Top product pairs bought together.
- **Analysis 2**: Customer Lifetime Value (revenue) by city and province, with RWF and USD.
- **Analysis 3**: Conversion rate and average session duration by `device_profile`.
- **Analysis 4**: Registration month cohorts and their spending/behavior.
- Saves CSVs into:
  - `output/product_pairs/`
  - `output/customer_value/`
  - `output/device_performance/`
  - `output/cohort_analysis/`

### 5.2 Integrated CLV analytics (`integrated_analytics.py`)

This script focuses on **per‑user CLV** by combining purchase and engagement metrics:

```bash
source venv/bin/activate
python scripts/integrated_analytics.py
```

What it does:
- Reads the same JSON data (users, sessions, products).
- Normalizes `cart_contents` into transaction line items.
- Computes revenue per user and basic engagement (session count, average duration).
- Joins everything into a single CLV view and prints the top customers.
- Writes the result to `output/clv_analysis/` as CSV.

---

## 6. Visualization Scripts

The visualizations are **pure Python + pandas/matplotlib/seaborn** and read directly from `data/`.

Generate all charts used in the report:

```bash
source venv/bin/activate
python scripts/visualizations.py
```

This creates PNGs in `output/figures/`:
- `revenue_by_category.png` – revenue by product category (RWF).
- `conversion_funnel.png` – total sessions → added to cart → converted.
- `device_performance.png` – sessions, conversions, and conversion rate by device type.
- `top_products.png` – top 5 most frequently added‑to‑cart products.

These images are then referenced in `report.md` and can be pasted into the final PDF.

---

## 7. Reports and Assessment Files

- To view or edit the **main write‑up**, open:
  - `report.md` (project report with screenshots and explanations).

- To see the **exam‑style answers**:
  - `ADDI-fat-report.md` (Section A & B answers, tied back to this project).

---

## 8. Typical End‑to‑End Run

If you want to replay the whole pipeline on your machine:

```bash
cd /home/securityman/adbmsfat
source venv/bin/activate

# 1) Generate or regenerate data
python scripts/dataset.py

# 2) Load into MongoDB
python scripts/loadtomongo.py

# 3) (Optional) Run MongoDB + HBase specific scripts
python scripts/db_analytics.py
python scripts/hbase_operations.py

# 4) Spark batch analytics
python scripts/spark_analytics.py
python scripts/integrated_analytics.py

# 5) Visualizations for the report
python scripts/visualizations.py
```

After this, check:
- `output/` for CSVs and figures.
- `report.md` / `ADDI-fat-report.md` for the written explanations and screenshots.

