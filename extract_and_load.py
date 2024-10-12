import sqlite3
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Connect to SQLite database
conn = sqlite3.connect('mock_resq.db')

# Initialize BigQuery client
client = bigquery.Client()

# Define the BigQuery dataset and project
dataset_id = "{}.{}".format(client.project, "resq_data")

# Create a Dataset object
dataset = bigquery.Dataset(dataset_id)

# Ensure the dataset exists
try:
    client.get_dataset(dataset_id)  # Make an API request.
    print(f"Dataset {dataset_id} already exists.")
except NotFound:
    dataset = client.create_dataset(dataset)  # Make an API request.
    print(f"Created dataset {dataset_id}")

# Define schemas for each table with lowercase field names
orders_schema = [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("createdat", "TIMESTAMP"),
    bigquery.SchemaField("userid", "INTEGER"),
    bigquery.SchemaField("quantity", "INTEGER"),
    bigquery.SchemaField("refunded", "BOOLEAN"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("sales", "FLOAT"),
    bigquery.SchemaField("providerid", "INTEGER"),
]

providers_schema = [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("defaultoffertype", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("registereddate", "TIMESTAMP"),
]

users_schema = [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("registereddate", "TIMESTAMP"),
]

# Tables to extract with their schemas
tables = [
    ('orders', orders_schema),
    ('providers', providers_schema),
    ('users', users_schema),
]

for table_name, schema in tables:
    # Read table from SQLite
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

    # Convert column names to lowercase (BigQuery standard)
    df.columns = [col.lower() for col in df.columns]

    # Correct data types in DataFrame
    for field in schema:
        column = field.name  # Field names are already lowercase
        data_type = field.field_type
        if column in df.columns:
            if data_type == "INTEGER":
                df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
            elif data_type == "FLOAT":
                df[column] = pd.to_numeric(df[column], errors='coerce')
            elif data_type == "BOOLEAN":
                df[column] = df[column].astype('bool')
            elif data_type == "TIMESTAMP":
                df[column] = pd.to_datetime(df[column], errors='coerce')
            # Strings don't need conversion

    # Load DataFrame to BigQuery
    table_id = "{}.{}.{}".format(client.project, "resq_data", table_name)
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Loaded {len(df)} rows into {table_id}.")

conn.close()
