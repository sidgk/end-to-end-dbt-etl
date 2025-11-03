# 🚆 Omio Analytics Engineer Case Study
**Author:** Siddu Kattimani  
**Tools:** Python, Airflow, dbt (BigQuery), SQL, Git, Miro, dbdaigram.io, ChatGpt(for documentation)  
**Purpose:** End-to-end design and implementation of a modern data pipeline and analytics model for a multimodal travel platform.

## 🧭 Overview
This project simulates a **production-grade data warehouse** for a travel company (like Omio), starting from **raw JSON exports** and ending in **clean, analytics-ready fact and dimension tables** modeled in a **star schema**.

The pipeline is fully automated through **Airflow DAGs**, transformations are defined in **dbt**, and **data quality** is enforced at multiple layers with both technical and business tests.

# Data Architecture

<img width="1654" height="553" alt="image" src="https://github.com/user-attachments/assets/bcd6bb26-235b-4abb-99cb-912d768f5782" />


## 🏗️ Architecture Summary

## ⚙️ 1. Data Extraction & Loading

### 🧩 Extraction
- **Source:** JSON exports representing booking, passenger, and ticket data.
- **Process:**  
  A Python task (`ingest_raw_export.py`) parses the JSON, flattens nested attributes, and converts it into structured **DataFrames**.
- **Output:** Saved as `.csv` or `.parquet` in an S3 bucket for downstream processing.
- **Scheduling**: Daily @ **2:00 AM** via Airflow.

### 🚀 Loading
- **Task:** `load_to_bq_stage`  
- **Action:** Uploads the transformed files to **BigQuery** into the `omio_stage` dataset as staging tables (e.g., `stage_bookings`, `stage_passengers`, etc.).
- **Scheduling:** Daily @ **3:00 AM** via Airflow.

---

## 🧱 2. Data Modeling with dbt

### 🌟 Star Schema Design

<img width="1488" height="791" alt="image" src="https://github.com/user-attachments/assets/c6779f64-38f3-44ea-8028-a3d76ed45437" />

### 🔹 Staging Models (`models/staging`)
- Models prefixed with `raw_` (e.g., `raw_bookings.sql`, `raw_segments.sql`).
- Purpose:
  - Perform typecasting and light cleaning.
  - Deduplicate based on latest update timestamps.
  - Enforce column-level quality tests (uniqueness, not-null, relationships).
- Materialization: **View**
- Output dataset: `omio_raw`

### 🔸 Core Models (`models/core`)
- Fact and dimension models organized under `dim_` and `fact_` prefixes.
- **Key Models:**
  - `fact_booking.sql`: Central transaction table containing booking-level metrics.
  - `dim_tickets`, `dim_segments`, `dim_passengers`, `dim_date`: Provide descriptive context.
- Materialization:
  - Dimensions: **Incremental**
  - Fact: Can be materialised as **Table or Incremental**, depending upon the use case. In this case study I recommend using incremental approach with the **pre_hook** .

**This is how the Lineage Graph looks for fact tables:**
**fact_booking**
<img width="1476" height="803" alt="image" src="https://github.com/user-attachments/assets/1dd29bc7-d3d1-45d7-83df-3c8169cccb5d" />

**Data Mart:*
<img width="1465" height="775" alt="image" src="https://github.com/user-attachments/assets/d638ac3f-9d12-4351-ab82-9cfe7679c9cf" />


### 🔹 Intermediate Models (`models/intermediate`)
- Prefixed as `bridge_`, or `int_ e.g., `bridge_ticket_passenger.sql`
- Handle intermediate transformations, joins, or data enrichment between staging and core models.


### 💾 3 Snapshots (Slowly Changing Dimensions)
**Snapshots Implemented**

- `booking_snapshot`
- `passenger_snapshot`
- `currency_snapshot`

**Lineage Graph of booking_snapshot**
<img width="1465" height="781" alt="image" src="https://github.com/user-attachments/assets/58c7edfe-35c2-4280-8a85-166cbd7d2e0a" />

**Purpose**

- Track historical changes in key entities:

- Booking status transitions (Pending → Confirmed → Cancelled)

- Price changes over time

- Currency exchange rate shifts

**Example Use-Cases**

- “When did this booking’s price change?”

- “What was the offer partner when the user first booked?”

- “How has the EUR/USD rate fluctuated since October?”

Snapshots are defined using dbt’s native snapshot feature with the updated_at field as the change detector.

### 💶 4. Currency Standardization
**Macro: `convert_to_eur`**

A dbt macro used to convert all currencies to a single company standard (EUR) using the latest available rate from the currency_snapshot.

This ensures all financial KPIs are comparable and consistent across regions.

### 📊 5. Data Marts (Analytics Layer)
**`fact_bookings_monthly.sql`**

- Aggregates booking, ticket, and passenger activity at a monthly level.

- Metrics included:

    - Total bookings
    - Total tickets sold
    - Total passengers
    - Total & average booking value
- Used by stakeholders for trend and performance dashboards.

**FYI: I have not added surrogate key to any models in this case study, I just relied on primary keys, I strongly see the usecase to add them, example in intermidate/bridge models** 

### ✅ 6. Data Quality Testing
**Technical Tests (schema.yml)**

- Uniqueness: Ensure primary keys are unique (booking_id, ticket_id, etc.)
- Not Null: Core identifiers and key metrics must not be null.
- Relationships: Validate joins between fact and dimension tables.
- Accepted Values: Restrict booking_status to valid states (pending, confirmed, cancelled).

**Business Logic Tests (custom SQL)**

- test_bookings_have_passengers.sql → Every booking must have ≥ 1 passenger.
- test_bookings_have_tickets.sql → Every booking must have ≥ 1 ticket.
- test_ticket_volume_drop.sql → Alerts if today’s ticket volume drops > 50% below 7-day average.
- test_booking_price_spike.sql → Detects unusual booking price spikes compared to historical mean.

**When to Test**

- **Pre-Transform:** Technical validations to ensure data integrity before loading to fact models.
- **Post-Transform:** Business logic tests validating metrics and model outputs.

**Handling Seasonality & External Anomalies**

Seasonality anomalies (e.g., strikes, wars, weather events) are difficult to detect automatically.
A practical approach:

- Flag deviations using historical rolling averages.
- Combine alerts with external event feeds or anomaly review dashboards.
- Focus on identifying unexpected behavior rather than all anomalies.

### 🪄 7. Pipeline Orchestration (Airflow)
**DAGs Overview**

| DAG Name               | Purpose                                    | Schedule    | Dependencies      |
| ---------------------- | ------------------------------------------ | ----------- | ----------------- |
| `etl_ingest_load_dag`  | Extract JSON → load to BigQuery stage      | 2 AM – 3 AM | None              |
| `dbt_stage_models_dag` | Run staging (`raw_`) models                | 3 AM – 4 AM | After ingestion   |
| `dbt_snapshot_dag`     | Run dbt snapshots                          | 5 AM        | After raw models  |
| `dbt_core_models_dag`  | Run core (`dim_`, `fact_`) models          | 4 AM – 5 AM | After staging     |
| `dbt_tests_dag`        | Run dbt & custom tests (Data Health Check) | 6:30 AM     | After core models |

### 📖 8. Documentation & Lineage

- Auto-generated dbt docs (dbt docs generate) describe every model, column, and relationship.
- Exported static site (index.html) in /target folder for easy sharing.
- Full lineage graph shows upstream and downstream dependencies.

### 📈 9. KPIs Monitored

| KPI                                | Description                                 |
| ---------------------------------- | ------------------------------------------- |
| **Total Bookings**                 | Total number of unique booking transactions |
| **Gross Booking Value (GBV)**      | Sum of all booking values before discounts  |
| **Ticket Revenue**                 | Sum of ticket sales per booking             |
| **Average Passengers per Booking** | Mean number of passengers per booking       |
| **Cancellation Rate**              | % of cancelled bookings                     |
| **Conversion Rate**                | Ratio of completed to attempted bookings    |

### 🧩 10. Project Highlights

- **End-to-end modular design**: Easy to extend for new data sources or KPIs.
- **Best practices followed**: Raw → Core → Marts separation, naming conventions, and incremental models.
- **Automated testing**: Ensures data reliability and stakeholder trust.
- **Snapshot tracking**: Enables historical auditability.
- **Documentation**: Clear lineage and business logic transparency.

**`"A reliable data foundation is the backbone of every decision — this project demonstrates how raw data evolves into trusted business insights.”`** 
**`"Whatever you can't measure, you can't manage`"**
