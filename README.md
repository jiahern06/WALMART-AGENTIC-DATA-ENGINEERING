# 🛒 Walmart Agentic Data Engineering Pipeline

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![AWS S3](https://img.shields.io/badge/AWS%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)

## 📋 Table of Contents
- [Overview](#-overview)
- [Architecture](#-architecture)
- [Technologies](#-technologies)
- [Project Structure](#-project-structure)
- [Data Pipeline Flow](#-data-pipeline-flow)
- [Layer Implementation](#-layer-implementation)
- [Data Model](#-data-model)
- [Orchestration](#-orchestration)
- [Key Features](#-key-features)
- [Setup & Deployment](#-setup--deployment)
- [Use Cases](#-use-cases)
- [Dataset](#-dataset)
- [Roadmap & Known Limitations](#-roadmap--known-limitations)

---

## 🎯 Overview

This project implements an **agentic, production-style data engineering pipeline** for a simulated Walmart retail operation. It combines an operational (OLTP) source system that is queryable by business users through a **SQL chatbot**, with a fully orchestrated **Medallion Architecture** (Bronze → Silver → Gold) built on **Databricks, Delta Lake, and dbt**, driven end-to-end by **Apache Airflow** running in Docker.

It demonstrates production data engineering patterns including:

- ✅ **CDC + file-based ingestion** from an operational database and object storage into a Bronze Delta layer
- ✅ **Two-stage Silver layer**: a technical (staging) layer per source table, and a business layer that assembles a governed **One Big Table (OBT)**
- ✅ **True SCD Type 2 dimensions** via native `dbt snapshot`
- ✅ **Dynamic, config-driven SQL generation** using Jinja to build the OBT from a list of source/column/join configs
- ✅ **Automated data quality testing** (schema tests + custom singular tests) gating every layer
- ✅ **Fully containerized orchestration** — Airflow, Postgres metadata DB, and Redis broker all run via Docker Compose

### Business Problem

A retail operation like Walmart generates constant transactional activity across customers, stores, products, employees, orders, and order line items. This pipeline enables:
- Governed, tested, analytics-ready sales data for BI and ad-hoc analysis
- Historical tracking of how customers, products, stores, and employees change over time (SCD2)
- A conversational/agentic entry point for business users to query operational data directly via chat, without waiting on the analytics pipeline
- A repeatable, containerized orchestration layer that can be scheduled, monitored, and redeployed

---

## 🏗️ Architecture

<img width="1142" height="514" alt="Architecture" src="https://github.com/user-attachments/assets/aacb5289-5c4e-48f0-8f16-c7080e817ed6" />


```text
┌──────────────┐     ┌──────────────┐     ┌──────────────────┐     ┌──────────────────────┐     ┌──────────────────┐
│              │ CDC │              │     │                   │     │                       │     │                   │
│  AGENTIC DB  │────▶│              │     │   SILVER LAYER    │     │     SILVER LAYER      │     │    GOLD LAYER     │
│ (PostgreSQL) │     │ BRONZE LAYER │────▶│    (Technical)     │────▶│      (Business)       │────▶│                   │
│  ⇅ SQL       │     │              │ dbt │                   │ dbt │                       │ dbt │  SCD2 Dimensions  │
│   Chatbot    │     │  Raw Delta   │ run │  6 incremental     │ run │  One-Big-Table (OBT)  │snap-│  + Fact Table      │
├──────────────┤Files│  tables in   │     │  models, 1 per     │     │  joining all 6         │shot │                   │
│   AWS S3     │────▶│  Unity       │     │  source table      │     │  entities + dbt tests  │+run │                   │
│ (file drops) │     │  Catalog     │     │                     │     │                       │     │                   │
└──────────────┘     └──────────────┘     └──────────────────┘     └──────────────────────┘     └──────────────────┘
                             ▲                                                                              │
                             └──────────────────────────────────────────────────────────────────────────────┘
                                          Entire pipeline orchestrated by Apache Airflow (Docker)
```

**How to read the diagram**: business users interact with the operational **Agentic DB** through a SQL chatbot for ad-hoc questions. In parallel, changes to that database (via **CDC**) and file drops to **AWS S3** are incrementally ingested into a **Bronze** Delta layer on Databricks. From there, **dbt** (orchestrated by **Airflow**) builds a technical Silver layer, a business Silver layer (One Big Table with quality checks), and finally a Gold layer of SCD2 dimensions and a fact table.

---

## 🛠️ Technologies

| Category | Technology |
|---|---|
| Data Warehouse / Lakehouse | Databricks, Delta Lake, Unity Catalog |
| Transformation | dbt-core, dbt-databricks |
| Orchestration | Apache Airflow 3.3.1 (CeleryExecutor) |
| Containerization | Docker, Docker Compose |
| Operational Source | PostgreSQL |
| File Landing Zone | AWS S3 |
| Metadata / Broker | PostgreSQL (Airflow metadata DB), Redis (Celery broker) |
| Language | Python, SQL (Jinja-templated) |

---

## 📂 Project Structure

```
WALMART-AGENTIC-DATA-ENGINEERING/
├── airflow_dbt/
│   ├── Dockerfile                     # apache/airflow:3.3.1 + dbt-core + dbt-databricks
│   ├── docker-compose.yaml            # Airflow (CeleryExecutor) + Postgres + Redis
│   ├── requirements.txt               # airflow-operators, apache-airflow, dbt-core, dbt-databricks
│   ├── config/
│   │   └── airflow.cfg
│   ├── dags/
│   │   └── orchestrate.py             # single DAG driving the entire pipeline
│   └── walmart_project/               # dbt project ("walmart_project")
│       ├── dbt_project.yml
│       ├── models/
│       │   ├── source/
│       │   │   └── sources.yml        # declares walmart_db.bronze.* sources
│       │   ├── silver_t/              # technical staging (1 model per source table)
│       │   │   ├── customers_t.sql
│       │   │   ├── employees_t.sql
│       │   │   ├── order_items_t.sql
│       │   │   ├── orders_t.sql
│       │   │   ├── products_t.sql
│       │   │   ├── stores_t.sql
│       │   │   └── properties.yml     # not_null / unique schema tests
│       │   ├── silver_b/
│       │   │   └── obt_b.sql          # dynamically-generated One Big Table
│       │   └── gold/
│       │       ├── ephemeral/         # per-entity ephemeral views over the OBT
│       │       │   ├── eph_customers.sql
│       │       │   ├── eph_employees.sql
│       │       │   ├── eph_orders.sql
│       │       │   ├── eph_products.sql
│       │       │   └── eph_stores.sql
│       │       └── fact/
│       │           └── fact_orders.sql
│       ├── snapshots/                 # dbt snapshot = SCD Type 2 dimensions
│       │   ├── dim_customers.yml
│       │   ├── dim_employees.yml
│       │   ├── dim_orders.yml
│       │   ├── dim_products.yml
│       │   └── dim_stores.yml
│       └── tests/
│           └── test_obt.sql           # singular test: no null FKs in the OBT
└── walmart_dataset/
    ├── ddl/
    │   └── walmart_schema.sql         # source PostgreSQL DDL
    ├── data/                          # synthetic seed CSVs
    │   ├── customers.csv   (2,000 rows)
    │   ├── stores.csv      (25 rows)
    │   ├── products.csv    (500 rows)
    │   ├── employees.csv   (250 rows)
    │   ├── orders.csv      (10,000 rows)
    │   └── order_items.csv (30,021 rows)
    └── load_data.py                   # bulk-loads CSVs into Postgres via COPY
```

---

## 🔄 Data Pipeline Flow

### 1️⃣ **Source → Bronze** (CDC + Files, triggered from Airflow)

**File**: `airflow_dbt/dags/orchestrate.py`

```python
@task
def ingest_cdc():
    ws = WorkspaceClient(host="the host url", token="the token id")
    job_trigger = ws.jobs.run_now(job_id="the job id")

    while True:
        job_run = ws.jobs.get_run(job_trigger.run_id)
        if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED,
                                               RunLifeCycleState.SKIPPED,
                                               RunLifeCycleState.INTERNAL_ERROR]:
            if job_run.state.result_state == RunResultState.SUCCESS:
                break
            raise Exception(f"Job failed with state: {job_run.state.result_state}")
        time.sleep(5)
    return "CDC Ingestion Completed"
```

Airflow doesn't move the CDC/file data itself — it **triggers a Databricks Job** (via the Databricks SDK) and polls it to completion. That job is responsible for incrementally landing CDC changes from the Agentic DB and file drops from S3 into raw Delta tables under `walmart_db.bronze` in Unity Catalog.

---

### 2️⃣ **Bronze → Silver (Technical)** — one incremental dbt model per source table

**Files**: `models/silver_t/*.sql`

```sql
{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

SELECT
    *,
    current_timestamp() AS processed_at
FROM
    {{ source('walmart_databricks', 'customers') }}

{% if is_incremental() %}
    WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
```

The same pattern repeats for `employees_t`, `order_items_t`, `orders_t`, `products_t`, and `stores_t` — each is an incremental model keyed on its natural business key, filtering on `updated_timestamp` so only new/changed Bronze rows are reprocessed.

---

### 3️⃣ **Silver (Technical) → Silver (Business)** — the One Big Table

**File**: `models/silver_b/obt_b.sql`

Rather than hand-writing a long join, the OBT is generated from a **Jinja config list** — each entry describes a source table, its selected/aliased columns, and its join condition:

```sql
{% set configs = [
    { "table": "walmart_db.dbt_schema_silver_t.orders_t", "columns": "o.order_id, ...", "alias": "o" },
    { "table": "walmart_db.dbt_schema_silver_t.customers_t", "columns": "c.customer_id, ...",
      "alias": "c", "join_condition": "o.customer_id = c.customer_id" },
    { "table": "walmart_db.dbt_schema_silver_t.order_items_t", "columns": "oi.order_item_id, ...",
      "alias": "oi", "join_condition": "o.order_id = oi.order_id" },
    { "table": "walmart_db.dbt_schema_silver_t.products_t", "columns": "p.product_id, ...",
      "alias": "p", "join_condition": "oi.product_id = p.product_id" },
    { "table": "walmart_db.dbt_schema_silver_t.employees_t", "columns": "e.employee_id, ...",
      "alias": "e", "join_condition": "o.store_id = e.store_id" },
    { "table": "walmart_db.dbt_schema_silver_t.stores_t", "columns": "s.store_name, ...",
      "alias": "s", "join_condition": "o.store_id = s.store_id" }
] %}

SELECT
    {% for config in configs %}{{ config['columns'] }}{% if not loop.last %},{% endif %}{% endfor %}
FROM
    {% for config in configs %}
        {% if loop.first %}
            {{ config['table'] }} AS {{ config['alias'] }}
        {% else %}
LEFT JOIN {{ config['table'] }} AS {{ config['alias'] }} ON {{ config['join_condition'] }}
        {% endif %}
    {% endfor %}
```

`orders` is the base table (first config, no join condition), and every other entity is `LEFT JOIN`ed in — preserving all orders even if a related customer, product, employee, or store record is missing. This makes the OBT easy to extend: adding a new source is a matter of appending one config block, not rewriting the query.

A **quality check** gates this layer — `tests/test_obt.sql`:

```sql
{{ config(severity='warn') }}

SELECT 1
FROM {{ ref('obt_b') }} AS obt
WHERE obt.order_id IS NULL OR obt.product_id IS NULL OR obt.employee_id IS NULL
   OR obt.store_id IS NULL OR obt.order_item_id IS NULL OR obt.customer_id IS NULL
```

---

### 4️⃣ **Silver (Business) → Gold (Ephemeral views)**

**Files**: `models/gold/ephemeral/*.sql`

```sql
SELECT DISTINCT
    customer_id, customer_first_name, customer_last_name, customer_email,
    customer_phone, customer_city, customer_province, customer_country,
    customer_created_timestamp, customer_updated_timestamp, customer_is_active,
    customer_processed_at,
    CURRENT_TIMESTAMP() AS customer_gold_processed_at
FROM {{ ref('obt_b') }}
```

Each `eph_*` model de-normalizes one entity back out of the OBT via `SELECT DISTINCT`. They're materialized as `ephemeral` (per `dbt_project.yml`), so dbt inlines them as CTEs into whatever references them rather than building a physical table or view.

---

### 5️⃣ **Gold (Ephemeral) → Gold (SCD Type 2 Dimensions)**

**Files**: `snapshots/*.yml`

```yaml
snapshots:
  - name: dim_customers
    relation: ref('eph_customers')
    config:
      schema: gold
      database: walmart_db
      unique_key: customer_id
      strategy: timestamp
      updated_at: customer_updated_timestamp
      dbt_valid_to_current: "to_date('9999-12-31')"
```

This is `dbt snapshot`, not `dbt run` — dbt natively tracks history here, adding `dbt_valid_from` / `dbt_valid_to` / `dbt_scd_id` columns and inserting a new row whenever `customer_updated_timestamp` changes, rather than overwriting in place. The same pattern produces `dim_employees`, `dim_orders`, `dim_products`, and `dim_stores`.

---

### 6️⃣ **Gold (Business) → Gold (Fact)**

**File**: `models/gold/fact/fact_orders.sql`

```sql
SELECT
    order_id, order_item_id, product_id, store_id, employee_id, customer_id,
    total_amount, quantity, unit_price, line_amount
FROM {{ ref('obt_b') }}
```

`fact_orders` is the transactional grain — one row per order line item, carrying the natural keys (`order_id`, `product_id`, `store_id`, `employee_id`, `customer_id`) alongside the measures (`total_amount`, `quantity`, `unit_price`, `line_amount`).

---

## 📊 Data Model

```
                         ┌──────────────────────┐
                         │      fact_orders      │
                         ├──────────────────────┤
                         │ order_id               │
                         │ order_item_id          │
                         │ product_id       (FK)  │
                         │ store_id         (FK)  │
                         │ employee_id      (FK)  │
                         │ customer_id      (FK)  │
                         │ total_amount           │
                         │ quantity               │
                         │ unit_price             │
                         │ line_amount            │
                         └──────────────────────┘
                                    │
        ┌───────────────┬──────────┼──────────┬───────────────┐
        ▼               ▼          ▼          ▼               ▼
┌───────────────┐┌──────────────┐┌──────────────┐┌──────────────┐┌───────────────┐
│ dim_customers ││ dim_products ││  dim_stores  ││dim_employees ││  dim_orders   │
│  (SCD2)       ││  (SCD2)      ││  (SCD2)      ││  (SCD2)      ││  (SCD2)       │
├───────────────┤├──────────────┤├──────────────┤├──────────────┤├───────────────┤
│customer_id    ││product_id    ││store_id      ││employee_id   ││order_id       │
│first_name     ││product_name  ││store_name    ││first_name    ││payment_method │
│last_name      ││category      ││city/province ││job_title     ││order_status   │
│email / phone  ││brand / price ││country       ││salary        ││order_timestamp│
│city/province  ││...           ││...           ││store_id      ││...            │
│country        ││              ││              ││...           ││               │
│...            ││              ││              ││              ││               │
│dbt_valid_from ││dbt_valid_from││dbt_valid_from││dbt_valid_from││dbt_valid_from │
│dbt_valid_to   ││dbt_valid_to  ││dbt_valid_to  ││dbt_valid_to  ││dbt_valid_to   │
└───────────────┘└──────────────┘└──────────────┘└──────────────┘└───────────────┘
```

> **Design note**: unlike a classic Kimball star schema with generated surrogate keys, this model joins `fact_orders` to its dimensions on **natural/business keys** (`customer_id`, `product_id`, `store_id`, `employee_id`). Because the dimensions are SCD2 snapshots, point-in-time-correct joins need to also match on `dbt_valid_from` / `dbt_valid_to` against the fact's event timestamp — see [Roadmap](#-roadmap--known-limitations) for a surrogate-key alternative.

### Table Schemas

#### fact_orders
- **Grain**: one row per order line item
- **Keys**: `order_id`, `order_item_id`, `product_id`, `store_id`, `employee_id`, `customer_id`
- **Measures**: `total_amount`, `quantity`, `unit_price`, `line_amount`

#### dim_customers / dim_employees / dim_products / dim_stores / dim_orders (SCD Type 2)
- Built via `dbt snapshot` on the `timestamp` strategy against each entity's `updated_timestamp`
- Automatically tracked columns: `dbt_scd_id`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`
- `dim_orders` snapshots order-level attributes (status, payment method) separately from the line-item grain captured in `fact_orders`, so order status changes over time remain queryable

---

## 🧩 Orchestration
<img width="1790" height="944" alt="Screenshot 2026-08-27 092726" src="https://github.com/user-attachments/assets/568ec678-3766-4d22-8aa9-a70e0fde71cf" />

**File**: `airflow_dbt/dags/orchestrate.py`

A single Airflow DAG (`orchestrate`) drives every layer in sequence:

```python
ingest_cdc() >> clean_target() >> source_freshness() \
    >> silver_technical >> silver_technical_tests \
    >> silver_business  >> silver_business_tests \
    >> gold_ephermeral  >> gold_dimensions >> gold_facts
```

| Task | Type | What it does |
|---|---|---|
| `ingest_cdc` | `@task` (Databricks SDK) | Triggers & polls the Databricks job that lands CDC + S3 files into Bronze |
| `clean_target` | `@task.bash` | Clears local dbt `target/` and `logs/` before each run |
| `source_freshness` | `@task.bash` | `dbt source freshness` against the Bronze sources |
| `silver_technical` / `_tests` | `BashOperator` | `dbt run --select silver_t` then `dbt test --select silver_t` |
| `silver_business` / `_tests` | `BashOperator` | `dbt run --select silver_b` then `dbt test --select silver_b` |
| `gold_ephermeral` | `BashOperator` | `dbt run --select gold/ephermeral` |
| `gold_dimensions` | `BashOperator` | `dbt snapshot` — builds all 5 SCD2 dimensions |
| `gold_facts` | `BashOperator` | `dbt run --select gold/fact` |

**Runtime**: the whole stack (Airflow webserver/API, scheduler, Celery workers, Postgres metadata DB, Redis broker, and the dbt project) is defined in `docker-compose.yaml`, built from a custom `Dockerfile` that layers `dbt-core` and `dbt-databricks` on top of `apache/airflow:3.3.1`.

```dockerfile
FROM apache/airflow:3.3.1
USER root
RUN apt-get update && apt-get install -y gcc && apt-get clean
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
```

---

## ⚡ Key Features

### 1. **Agentic, chat-driven access to operational data**
Business users can query the operational database directly through a SQL chatbot, independent of the analytics pipeline's refresh cadence.

### 2. **CDC + Incremental File Ingestion into Bronze**
A Databricks job — triggered and polled from Airflow via the Databricks SDK — incrementally lands both CDC changes and S3 file drops into Delta.

### 3. **Config-Driven SQL Generation**
```jinja
{% for config in configs %}
    {% if loop.first %}
        {{ config['table'] }} AS {{ config['alias'] }}
    {% else %}
LEFT JOIN {{ config['table'] }} AS {{ config['alias'] }} ON {{ config['join_condition'] }}
    {% endif %}
{% endfor %}
```
The One Big Table is built from a list of configs, not hand-written joins — adding a new source table is a one-block change.

### 4. **True SCD Type 2 via `dbt snapshot`**
```yaml
strategy: timestamp
updated_at: customer_updated_timestamp
dbt_valid_to_current: "to_date('9999-12-31')"
```
Full history is preserved for customers, employees, products, stores, and orders — not just the latest state.

### 5. **Layered Data Quality Enforcement**
- Schema tests (`not_null`, `unique`) on `products_t.product_id` and `orders_t.order_id`
- A custom singular test (`test_obt.sql`, `severity='warn'`) checking every foreign key in the OBT is populated before Gold is built

### 6. **Fully Containerized, Multi-Service Orchestration**
Airflow (API server, scheduler, Celery workers), Postgres, and Redis all run via a single `docker-compose up`, with the dbt project mounted straight into the Airflow containers.

---

## 🚀 Setup & Deployment

### Prerequisites
- Docker & Docker Compose
- A Databricks workspace with Unity Catalog enabled (catalog `walmart_db`, schema `bronze` populated)
- A PostgreSQL instance for the source "Agentic DB"
- A `~/.dbt/profiles.yml` (or equivalent) configured for the `dbt-databricks` adapter under profile `walmart_project`

### Step 1: Load the Source Database

```bash
# Create the schema
psql -f walmart_dataset/ddl/walmart_schema.sql

# Bulk-load the synthetic seed data
cd walmart_dataset
python load_data.py   # set conn_string first
```

### Step 2: Configure the Databricks CDC Job

Set the real Databricks host, token, and `job_id` in `airflow_dbt/dags/orchestrate.py` (`ingest_cdc` task) — ideally via Airflow Connections/Variables rather than hardcoding.

### Step 3: Bring Up Airflow

```bash
cd airflow_dbt
docker-compose build
docker-compose up airflow-init
docker-compose up -d
```

Airflow UI: `http://localhost:8080`

### Step 4: Trigger the Pipeline

Unpause and trigger the `orchestrate` DAG from the Airflow UI or CLI:

```bash
docker-compose exec airflow-scheduler airflow dags trigger orchestrate
```

This runs, in order: CDC/file ingestion → source freshness → Silver Technical (+tests) → Silver Business/OBT (+tests) → Gold Ephemeral views → Gold SCD2 Dimensions → Gold Fact table.

---

## 💼 Use Cases

### 1. **Revenue by Category and Store**
```sql
SELECT
    p.category,
    s.store_name,
    SUM(f.line_amount) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS order_count
FROM walmart_db.dbt_schema_gold.fact_orders f
JOIN walmart_db.dbt_schema_gold.dim_products p ON f.product_id = p.product_id AND p.dbt_valid_to = to_date('9999-12-31')
JOIN walmart_db.dbt_schema_gold.dim_stores   s ON f.store_id = s.store_id     AND s.dbt_valid_to = to_date('9999-12-31')
GROUP BY p.category, s.store_name
ORDER BY total_revenue DESC;
```

### 2. **Customer Order History (Point-in-Time Correct)**
```sql
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    f.order_id,
    f.total_amount
FROM walmart_db.dbt_schema_gold.fact_orders f
JOIN walmart_db.dbt_schema_gold.dim_orders o
  ON f.order_id = o.order_id
JOIN walmart_db.dbt_schema_gold.dim_customers c
  ON f.customer_id = c.customer_id
 AND o.order_timestamp BETWEEN c.dbt_valid_from AND c.dbt_valid_to;
```

### 3. **Employee Sales Performance by Store**
```sql
SELECT
    e.first_name,
    e.last_name,
    e.job_title,
    st.store_name,
    SUM(f.line_amount) AS revenue_attributed
FROM walmart_db.dbt_schema_gold.fact_orders f
JOIN walmart_db.dbt_schema_gold.dim_employees e ON f.employee_id = e.employee_id AND e.dbt_valid_to = to_date('9999-12-31')
JOIN walmart_db.dbt_schema_gold.dim_stores    st ON f.store_id = st.store_id    AND st.dbt_valid_to = to_date('9999-12-31')
GROUP BY e.first_name, e.last_name, e.job_title, st.store_name
ORDER BY revenue_attributed DESC;
```

### 4. **Order Status Funnel Over Time**
```sql
SELECT
    order_status,
    DATE_TRUNC('month', dbt_valid_from) AS month,
    COUNT(*) AS orders
FROM walmart_db.dbt_schema_gold.dim_orders
GROUP BY order_status, DATE_TRUNC('month', dbt_valid_from)
ORDER BY month, order_status;
```

---

## 📁 Dataset

Synthetic Walmart retail data, seeded into PostgreSQL via `load_data.py`:

| Table | Rows | Key Columns |
|---|---|---|
| `customers` | 2,000 | customer_id, city, province, country |
| `stores` | 25 | store_id, store_name, city, province, country |
| `products` | 500 | product_id, category, brand, price |
| `employees` | 250 | employee_id, store_id, job_title, salary |
| `orders` | 10,000 | order_id, customer_id, store_id, order_status, payment_method |
| `order_items` | 30,021 | order_item_id, order_id, product_id, quantity, unit_price |

---

## 🗺️ Roadmap & Known Limitations

A few items worth calling out for anyone extending this project:

- **Gold selector syntax**: the `gold_ephermeral` and `gold_facts` Airflow tasks currently run `dbt run --select gold/ephermeral` and `dbt run --select gold/fact`. Forward-slash isn't valid dbt graph-selector syntax for multi-level paths (it should be dot notation, e.g. `--select gold.fact`, or `--select path:models/gold/fact`), so both tasks currently log *"The selection criterion does not match any enabled nodes"* and complete as no-ops, while `silver_t`, `silver_b`, and the `dbt snapshot` dimensions build successfully.
- **Task naming**: `gold_ephermeral` is a typo of `ephemeral` (the actual folder name), which compounds the selector mismatch above.
- **Surrogate keys**: `fact_orders` joins to dimensions on natural keys rather than dimension surrogate keys, which pushes the burden of point-in-time-correct joins onto every downstream query (see the `dbt_valid_from`/`dbt_valid_to` join pattern in [Use Cases](#-use-cases)). A surrogate-key lookup step would simplify consumption.
- **Secrets management**: the Databricks `host`, `token`, and `job_id` are hardcoded placeholders in `orchestrate.py`. These should move to Airflow Connections/Variables or a secrets backend before any real deployment.
- **CI/CD**: there's no automated pipeline currently running `dbt test`/`dbt build` on pull requests — a natural next step alongside the existing Airflow-triggered tests.
