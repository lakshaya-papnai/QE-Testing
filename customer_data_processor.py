# 1. Overview

**Purpose:** Run and validate the Customer Demographics ETL in Databricks.

**Flow:**
`Bronze → Silver → Gold`

* **Bronze:** Creates two customer datasets with explicit schemas.
* **Silver:** Removes records where `age < 0`.
* **Gold:** Combines the cleaned datasets.
* **Logging:** Tracks initialization, processing stages, success, and failures.

**Prerequisites**

* Databricks cluster with PySpark.
* Valid `spark` session.
* Databricks `display()` support.

# 2. Execution & Validation

Run the notebook/script in Databricks and verify these logs:

```text
Initializing Customer Demographics ETL Job...
Defining explicit schemas...
Extracting data into Bronze layer...
Filtering noisy data for Silver layer...
Integrating Silver tables into Gold layer...
Pipeline completed successfully.
```

Expected Gold output:

| id | name    | age |
| -: | ------- | --: |
|  1 | Alice   |  25 |
|  3 | Charlie |  30 |
|  4 | Dave    |  22 |
|  6 | Eve     |  28 |

`TestUser` and `BotAccount` are removed because their ages are negative.

**Important:** The supplied script uses different column orders in the two DataFrames. Use `unionByName()` after selecting a consistent column order:

```python
df1_silver = df1_bronze.filter("age >= 0").select("id", "name", "age")
df2_silver = df2_bronze.filter("age >= 0").select("id", "name", "age")

df_gold = df1_silver.unionByName(df2_silver)
```

# 3. Troubleshooting & Operations

**If the job fails:**

1. Check the Databricks cell output and `CustomerDemographicsETL` logs.
2. If the failure occurs at the Gold step, verify that both DataFrames have the same column names and types.
3. Confirm `unionByName()` is being used.
4. Re-run the notebook after correcting the issue.

**Success criteria**

* No unhandled exception.
* Four valid customer records in Gold.
* Negative-age records are excluded.
* Final success log is generated.

**Operational note:** The current script uses hard-coded sample data. For production, replace these inputs with persistent Bronze sources and add data-quality, row-count, and monitoring checks.
