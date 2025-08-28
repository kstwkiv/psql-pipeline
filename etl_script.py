import psycopg2
import os
import pandas as pd

def extract_from_postgresql(query, connection_params):
    conn = psycopg2.connect(**connection_params)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def transform_data(df):
    # Example transformation: drop rows with nulls, convert date/time columns, lowercase columns
    df = df.dropna()
    for col in df.columns:
        if 'date' in col or 'time' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    df.columns = [c.lower() for c in df.columns]
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)
    print(f"Loaded data to {output_path}")

if __name__ == "__main__":
    # Safely get PG_PORT environment variable, defaulting to 5432 if empty or invalid
    port_env = os.environ.get('PG_PORT')
    port = int(port_env) if port_env and port_env.isdigit() else 5432

    connection_params = {
        'host': os.environ.get('PG_HOST', 'localhost'),
        'database': os.environ.get('PG_DATABASE', 'postgres'),
        'user': os.environ.get('PG_USERNAME', 'postgres'),
        'password': os.environ.get('PG_PASSWORD', ''),
        'port': port
    }

    tables = ['categories', 'customers', 'orders', 'products']
    for table in tables:
        print(f"Processing table: {table}")
        query = f"SELECT * FROM {table};"
        df_extracted = extract_from_postgresql(query, connection_params)
        df_transformed = transform_data(df_extracted)
        load_to_csv(df_transformed, f"{table}_output.csv")

    print("ETL process completed. Data exported to CSV files.")
