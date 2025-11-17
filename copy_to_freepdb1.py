"""
Copy graph data from FREE database to FREEPDB1
This allows Graph Studio (connected to FREEPDB1) to see the data
"""

import oracledb
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

# Connection details
ORACLE_HOST = "localhost"
ORACLE_PORT = 1521
ORACLE_USER = "graphuser"
ORACLE_PASSWORD = os.getenv("GRAPH_PASSWORD", "Welcome1")

def connect_to_db(service_name):
    """Connect to Oracle database"""
    dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=service_name)
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)

def copy_table(source_conn, dest_conn, table_name):
    """Copy a table from source to destination"""
    print(f"Copying {table_name}...")

    # Read from FREE
    df = pd.read_sql(f"SELECT * FROM {table_name}", source_conn)
    print(f"  - Read {len(df)} rows from FREE")

    # Drop table if exists in FREEPDB1
    try:
        cursor = dest_conn.cursor()
        cursor.execute(f"DROP TABLE {table_name}")
        print(f"  - Dropped existing table")
    except:
        pass

    # Get column definitions from source
    cursor = source_conn.cursor()
    cursor.execute(f"""
        SELECT column_name, data_type, data_length, data_precision, nullable
        FROM user_tab_columns
        WHERE table_name = UPPER('{table_name}')
        ORDER BY column_id
    """)

    columns = cursor.fetchall()

    # Build CREATE TABLE statement
    col_defs = []
    for col in columns:
        col_name, data_type, data_length, data_precision, nullable = col

        if data_type == 'VARCHAR2':
            col_def = f"{col_name} VARCHAR2({data_length})"
        elif data_type == 'NUMBER':
            if data_precision:
                col_def = f"{col_name} NUMBER({data_precision})"
            else:
                col_def = f"{col_name} NUMBER"
        else:
            col_def = f"{col_name} {data_type}"

        if nullable == 'N':
            col_def += " NOT NULL"

        col_defs.append(col_def)

    create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"

    # Create table in FREEPDB1
    cursor = dest_conn.cursor()
    cursor.execute(create_sql)
    print(f"  - Created table structure")

    # Insert data
    if len(df) > 0:
        # Prepare INSERT statement
        cols = ', '.join(df.columns)
        placeholders = ', '.join([f":{i+1}" for i in range(len(df.columns))])
        insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

        # Convert DataFrame to list of tuples
        data = [tuple(row) for row in df.values]

        # Batch insert
        cursor.executemany(insert_sql, data)
        dest_conn.commit()
        print(f"  - Inserted {len(df)} rows into FREEPDB1")

    print(f"  [OK] {table_name} copied successfully\n")

if __name__ == "__main__":
    print("="*60)
    print("Copying Graph Data from FREE to FREEPDB1")
    print("="*60)

    # Connect to both databases
    print("\nConnecting to databases...")
    free_conn = connect_to_db("FREE")
    freepdb1_conn = connect_to_db("FREEPDB1")
    print("[OK] Connected to both databases\n")

    # List of tables to copy
    tables = [
        'entities',
        'countries',
        'months',
        'inspections',
        'target_proxies',
        'country_months',
        'shipped_in_edges',
        'is_from_edges',
        'has_weather_edges',
        'has_inspection_result_edges'
    ]

    # Copy each table
    for table in tables:
        try:
            copy_table(free_conn, freepdb1_conn, table)
        except Exception as e:
            print(f"  [ERROR] Error copying {table}: {e}\n")

    # Verify
    print("="*60)
    print("Verification")
    print("="*60)

    cursor = freepdb1_conn.cursor()
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table:30s} {count:5d} rows")

    # Close connections
    free_conn.close()
    freepdb1_conn.close()

    print("\n" + "="*60)
    print("[OK] Copy Complete!")
    print("="*60)
    print("\nYou can now use Graph Studio UI with FREEPDB1 database")
