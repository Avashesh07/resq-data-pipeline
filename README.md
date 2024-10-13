# ResQ Data Pipeline Project 

## Project Description 
This project implements a data pipeline for ResQ Club to create a presentation table that combines data from three tables: `orders`, `providers`, and `users`. The goal is to provide analysts with a single table from which they can run queries using only `SELECT` statements, without needing to perform joins or complex transformations.
The pipeline involves:

- Extracting data from a SQLite database.

- Loading the data into Google BigQuery.

- Transforming the data to create a presentation table in BigQuery.

- Providing SQL queries to answer specific business questions.

You can access the following deliverables:
 
- [ResQDashboard.pdf](https://github.com/Avashesh07/resq-data-pipeline/blob/main/ResQDashboard.pdf) : An overview of the key metrics and visualizations for the project.
 
- [resqDataAnalysis.ipynb](https://github.com/Avashesh07/resq-data-pipeline/blob/main/resqDataAnalysis.ipynb) : Jupyter Notebook for Customer Lifetime Value (CLV) analysis and more.

## Table of Contents 
 
- Project Description
 
- Prerequisites
 
- Project Structure
 
- Setup Instructions
  - 1. Clone the Repository
 
  - 2. Navigate to the Project Directory
 
  - 3. Install Required Python Packages
 
  - 4. Set Up Google Cloud Credentials
 
  - 5. Place the SQLite Database File
 
- Running the Pipeline
  - Step 1: Extract Data and Load into BigQuery
 
  - Step 2: Create the Presentation Table in BigQuery
 
  - Step 3: Run Analyst Queries
 
- Task 2: Customer Lifetime Value (CLV) Analysis
 
- Challenges and Solutions

---


## Prerequisites

Before running the project, ensure you have the following installed and set up:

- **Python 3.7 or higher**
- **Google Cloud SDK** with `gcloud` command-line tool
- **Google Cloud account** with access to BigQuery
- **Git** (for version control)
- **SQLite** (for accessing the database file)
- **Pip** (Python package installer)

## Project Structure
```
resq-data-pipeline/
  ├── README.md
  ├── .gitignore
  ├── requirements.txt
  ├── extract_and_load.py
  ├── sql_queries/
    ├── create_presentation_table.sql
    ├── top_10_partners_by_sales.sql
    ├── favorite_partner_segments.sql
    └── m1_retention_cohort_analysis.sql
```


- **`README.md`**: Documentation and instructions.
- **`.gitignore`**: Specifies files and directories to be ignored by Git.
- **`requirements.txt`**: Lists the Python package dependencies.
- **`extract_and_load.py`**: Python script to extract data from the SQLite database and load it into BigQuery.
- **`sql_queries/`**: Directory containing SQL scripts for creating the presentation table and answering the analyst's questions.

## Setup Instructions

### 1. Clone the Repository
`
git clone https://github.com/Avashesh07/resq-data-pipeline.git
`
### 2. Navigate to the Project Directory
`
cd resq-data-pipeline
`
### 3. Install Required Python Packages
`
pip install -r requirements.txt
`
### 4. Set Up Google Cloud Credentials

Ensure you have the Google Cloud SDK installed and authenticated.

-   **Install the Google Cloud SDK**

-   **Authenticate with Google Cloud**:


    `gcloud auth login
    gcloud auth application-default login`

-   **Set Your Google Cloud Project**:

    Replace `your_project_id` with your actual Google Cloud project ID.


    `gcloud config set project your_project_id`

### 5. Place the SQLite Database File

**Note**: The SQLite database file (`mock_resq.db`) is not included in this repository due to data privacy considerations. Please ensure you have the `.db` file and place it in the root directory of the project.


`resq-data-pipeline/
├── mock_resq.db`

Running the Pipeline
--------------------

### Step 1: Extract Data and Load into BigQuery

Run the `extract_and_load.py` script to extract data from the SQLite database and load it into BigQuery.


`python extract_and_load.py`

**Script Overview**:

-   Connects to the SQLite database (`mock_resq.db`).
-   Extracts data from the `orders`, `providers`, and `users` tables.
-   Converts column names to lowercase for consistency.
-   Casts columns to appropriate data types.
-   Loads the data into BigQuery in a dataset named `resq_data`.

### Step 2: Create the Presentation Table in BigQuery

Execute the SQL script to create the presentation table.

1.  Open the BigQuery Console.
2.  Click on **Compose New Query**.
3.  Copy and paste the contents of `sql_queries/create_presentation_table.sql` into the query editor.
4.  Replace `your_project_id` with your actual project ID.
5.  Run the query.

**`create_presentation_table.sql`**:

```
CREATE OR REPLACE TABLE `your_project_id.resq_data.presentation_table` AS
SELECT
  CAST(o.id AS INT64) AS order_id,
  CAST(o.createdat AS TIMESTAMP) AS order_created_at,
  CAST(o.userid AS INT64) AS user_id,
  CAST(o.quantity AS INT64) AS quantity,
  CAST(o.refunded AS BOOL) AS refunded,
  o.currency AS currency,
  CAST(o.sales AS FLOAT64) AS sales,
  CAST(o.providerid AS INT64) AS provider_id,
  p.defaultoffertype AS default_offer_type,
  p.country AS provider_country,
  CAST(p.registereddate AS TIMESTAMP) AS provider_registered_date,
  u.country AS user_country,
  CAST(u.registereddate AS TIMESTAMP) AS user_registered_date
FROM `your_project_id.resq_data.orders` o
LEFT JOIN `your_project_id.resq_data.providers` p ON o.providerid = p.id
LEFT JOIN `your_project_id.resq_data.users` u ON o.userid = u.id;
```

### Step 3: Run Analyst Queries

You can now run queries on the `presentation_table` to answer specific business questions.

#### 3.1. Top 10 Partners by Sales

**File**: `sql_queries/top_10_partners_by_sales.sql`


```
SELECT
  provider_id,
  SUM(sales) AS total_sales
FROM `your_project_id.resq_data.presentation_table`
GROUP BY provider_id
ORDER BY total_sales DESC
LIMIT 10;
```

#### 3.2. Customers' Favorite Partner Segments

**File**: `sql_queries/favorite_partner_segments.sql`


```
SELECT
  default_offer_type AS partner_segment,
  COUNT(DISTINCT user_id) AS customer_count
FROM `your_project_id.resq_data.presentation_table`
GROUP BY partner_segment
ORDER BY customer_count DESC;
```

#### 3.3. M1 Retention for Customer Cohorts

**File**: `sql_queries/m1_retention_cohort_analysis.sql`


```
WITH first_orders AS (
  SELECT
    user_id,
    MIN(DATE(order_created_at)) AS first_order_date
  FROM `your_project_id.resq_data.presentation_table`
  GROUP BY user_id
),
cohort_data AS (
  SELECT
    fo.user_id,
    DATE_TRUNC(fo.first_order_date, MONTH) AS cohort_month,
    DATE_TRUNC(DATE(pt.order_created_at), MONTH) AS order_month
  FROM `your_project_id.resq_data.presentation_table` pt
  JOIN first_orders fo ON pt.user_id = fo.user_id
)
SELECT
  cohort_month,
  COUNT(DISTINCT user_id) AS cohort_size,
  SUM(CASE WHEN order_month = DATE_ADD(cohort_month, INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS retained_users,
  SAFE_DIVIDE(
    SUM(CASE WHEN order_month = DATE_ADD(cohort_month, INTERVAL 1 MONTH) THEN 1 ELSE 0 END),
    COUNT(DISTINCT user_id)
  ) AS m1_retention_rate
FROM cohort_data
GROUP BY cohort_month
ORDER BY cohort_month;
```

**Instructions**:

-   Replace `your_project_id` with your actual project ID in the SQL scripts.
-   Run each query in the BigQuery Console to obtain the results.

Approach and Explanation
------------------------

### Data Extraction and Loading

-   **Python Script (`extract_and_load.py`)**:
    -   Utilizes the `sqlite3` library to connect to the SQLite database.
    -   Reads data from the `orders`, `providers`, and `users` tables into Pandas DataFrames.
    -   Converts column names to lowercase to maintain consistency in BigQuery.
    -   Casts columns to appropriate data types to match BigQuery schemas.
    -   Uses the `google-cloud-bigquery` library to load DataFrames into BigQuery tables.
    -   Specifies schemas explicitly to ensure data types are correctly assigned.

### Data Transformation

-   **Creating the Presentation Table**:
    -   The SQL script `create_presentation_table.sql` combines data from the three tables.
    -   Performs `LEFT JOIN`s to bring in data from `providers` and `users` based on `provider_id` and `user_id`.
    -   Casts columns to the correct data types.
    -   The resulting `presentation_table` contains all necessary fields for analysis.

### Analyst Queries

-   Provided SQL scripts to answer specific business questions, ensuring the analyst can run them directly on the `presentation_table` without needing to perform joins or additional transformations.


Additional Notes
----------------

-   **Data Privacy**:

    -   The SQLite database file (`mock_resq.db`) is not included in the repository to maintain data privacy.
    -   Instructions are provided on where to place the database file.
-   **Assumptions**:

    -   It is assumed that the user has the necessary permissions to create datasets and tables in BigQuery.
    -   The `presentation_table` is created in the same project and dataset as the source tables.
-   **Potential Improvements**:

    -   Implement error handling and logging in the Python script for better debugging.
    -   Automate the data pipeline using cloud services like Cloud Functions or Cloud Composer for scalability.

## Task 2: Customer Lifetime Value (CLV) Analysis 
In this task, you need to calculate the **Customer Lifetime Value (CLV)**  using the presentation table created in the previous step. The analysis should include factors such as: 
- **Average Order Value (AOV)** : Total sales divided by the number of orders.
 
- **Purchase Frequency** : Total orders divided by the number of customers.
 
- **Customer Lifespan** : The time between a customer's first and last purchase.

### Calculating CLV 

The formula used for calculating CLV is:


```plaintext
CLV = AOV × Purchase Frequency × Customer Lifespan
```
You can run this analysis in **Jupyter Notebook**  or through Python. Assuming the `df` is the presentation table.
### Step-by-Step CLV Calculation: 
 
1. **Average Order Value (AOV)** :

```python
# Calculate Total Revenue and Total Orders
total_revenue = df['sales'].sum()
total_orders = df['order_id'].nunique()
# Calculate Average Order Value
aov = total_revenue / total_orders
```
 
2. **Purchase Frequency** :

```python
# Calculate Total Customers
total_customers = df['user_id'].nunique()

# Calculate Purchase Frequency
purchase_frequency = total_orders / total_customers
```
 
3. **Customer Lifespan** :

```python
# Average Customer Lifespan
customer_lifespans = df.groupby('user_id')['order_created_at'].agg(['min', 'max'])
customer_lifespans['lifespan_days'] = (customer_lifespans['max'] - customer_lifespans['min']).dt.days
customer_lifespans['lifespan_days'] = customer_lifespans['lifespan_days'].replace(0, 1)  # Handle single purchases
average_lifespan_days = customer_lifespans['lifespan_days'].mean()
average_lifespan_years = average_lifespan_days / 365
```
 
4. **Final CLV Calculation** :
You can multiply the above values to get the final CLV estimate.

---


## Challenges and Solutions 

### Data Extraction and Loading 
 
- Utilizes the `sqlite3` library to connect to the SQLite database and load the data into BigQuery using the `google-cloud-bigquery` library.

### Data Transformation 

- Combines data from the three tables and casts columns to appropriate data types.

### CLV Calculation and Dashboard 
 
- Provides a clear approach to calculating CLV.


---




