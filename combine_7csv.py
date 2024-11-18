import psycopg2
import csv
import os
import time
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Database connection parameters
DB_NAME = "austin_cigna_oap_unfiltered_"  # Change this for different databases, use lowercase
DB_USER = "postgres"
DB_PASSWORD = "harshu02"  # Change this to your password
DB_HOST = "localhost"
DB_PORT = "5432"

# Path to CSV files
CSV_PATH = "/Users/harshupande/Library/CloudStorage/GoogleDrive-hpande@slu.edu/My Drive/price_transparency_files/austin_mrfs/austin_cigna_oap/unfiltered"  # Change this to your CSV files directory
EXPORT_PATH = "/Users/harshupande/Library/CloudStorage/GoogleDrive-hpande@slu.edu/My Drive/price_transparency_files/austin_mrfs/austin_cigna_oap/unfiltered"  # Change this to your export directory

# Final table name
FINAL_TABLE_NAME = "austin_cigna_oap_prices_combined"  # Change this to your desired final table name

def log_progress(message):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}")

def create_database():
    conn = None
    try:
        # Connect to the default 'postgres' database to create a new database
        conn = psycopg2.connect(
            dbname="postgres",
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if the database already exists
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {DB_NAME}")
            log_progress(f"Database '{DB_NAME}' created successfully")
        else:
            log_progress(f"Database '{DB_NAME}' already exists")
    
    except (Exception, psycopg2.Error) as error:
        log_progress(f"Error while creating database: {error}")
    
    finally:
        if conn:
            cursor.close()
            conn.close()

def execute_query(cursor, query, commit=False):
    log_progress(f"Executing: {query[:50]}...")  # Log first 50 characters of the query
    cursor.execute(query)
    if commit:
        cursor.connection.commit()
    log_progress("Query completed.")

def create_tables(cursor):
    table_creation_queries = [
        f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}code (
            id TEXT PRIMARY KEY,
            billing_code_type_version TEXT,
            billing_code TEXT,
            billing_code_type TEXT
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}tin (
            id TEXT PRIMARY KEY,
            tin_type TEXT,
            tin_value TEXT
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}rate (
            id TEXT PRIMARY KEY,
            code_id TEXT,
            rate_metadata_id TEXT,
            negotiated_rate NUMERIC
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}rate_metadata (
            id TEXT PRIMARY KEY,
            billing_class TEXT,
            negotiated_type TEXT,
            service_code TEXT,
            expiration_date DATE,
            additional_information TEXT,
            billing_code_modifier TEXT
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}file (
            id TEXT PRIMARY KEY,
            filename TEXT,
            reporting_entity_name TEXT,
            reporting_entity_type TEXT,
            plan_name TEXT,
            plan_id_type TEXT,
            plan_id TEXT,
            plan_market_type TEXT,
            last_updated_on DATE,
            version TEXT,
            url TEXT
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}npi_tin (
            npi TEXT,
            tin_id TEXT
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}tin_rate_file (
            tin_id TEXT,
            rate_id TEXT,
            file_id TEXT
        );
        """
    ]

    for query in table_creation_queries:
        execute_query(cursor, query)

def import_csv(cursor, file_path, table_name, columns):
    with open(file_path, 'r', encoding='utf-8') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            placeholders = ','.join(['%s'] * len(columns))
            columns_str = ','.join(columns)
            sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            cursor.execute(sql, row)
    cursor.connection.commit()
    log_progress(f"Imported data into {table_name}")

def import_data(cursor):
    import_csv(cursor, os.path.join(CSV_PATH, 'file.csv'), f"{DB_NAME}file", ['id', 'filename', 'reporting_entity_name', 'reporting_entity_type', 'plan_name', 'plan_id_type', 'plan_id', 'plan_market_type', 'last_updated_on', 'version', 'url'])
    import_csv(cursor, os.path.join(CSV_PATH, 'code.csv'), f"{DB_NAME}code", ['id', 'billing_code_type_version', 'billing_code', 'billing_code_type'])
    import_csv(cursor, os.path.join(CSV_PATH, 'npi_tin.csv'), f"{DB_NAME}npi_tin", ['npi', 'tin_id'])
    import_csv(cursor, os.path.join(CSV_PATH, 'tin.csv'), f"{DB_NAME}tin", ['id', 'tin_type', 'tin_value'])
    import_csv(cursor, os.path.join(CSV_PATH, 'tin_rate_file.csv'), f"{DB_NAME}tin_rate_file", ['tin_id', 'rate_id', 'file_id'])
    import_csv(cursor, os.path.join(CSV_PATH, 'rate.csv'), f"{DB_NAME}rate", ['id', 'code_id', 'rate_metadata_id', 'negotiated_rate'])
    import_csv(cursor, os.path.join(CSV_PATH, 'rate_metadata.csv'), f"{DB_NAME}rate_metadata", ['id', 'billing_class', 'negotiated_type', 'service_code', 'expiration_date', 'additional_information', 'billing_code_modifier'])

def create_indexes(cursor):
    index_queries = [
        f"CREATE INDEX IF NOT EXISTS idx_tin_rate_file_rate_id ON {DB_NAME}tin_rate_file(rate_id);",
        f"CREATE INDEX IF NOT EXISTS idx_tin_rate_file_tin_id ON {DB_NAME}tin_rate_file(tin_id);",
        f"CREATE INDEX IF NOT EXISTS idx_rate_id ON {DB_NAME}rate(id);",
        f"CREATE INDEX IF NOT EXISTS idx_rate_code_id ON {DB_NAME}rate(code_id);",
        f"CREATE INDEX IF NOT EXISTS idx_tin_id ON {DB_NAME}tin(id);",
        f"CREATE INDEX IF NOT EXISTS idx_npi_tin_tin_id ON {DB_NAME}npi_tin(tin_id);",
        f"CREATE INDEX IF NOT EXISTS idx_npi_tin_npi ON {DB_NAME}npi_tin(npi);",
        f"CREATE INDEX IF NOT EXISTS idx_code_id ON {DB_NAME}code(id);"
    ]

    for query in index_queries:
        execute_query(cursor, query)

def perform_join(cursor):
    # Drop existing table if it exists
    execute_query(cursor, f"DROP TABLE IF EXISTS {DB_NAME}{FINAL_TABLE_NAME};")

    # Perform the join operation with explicit table names and aliases
    join_query = f"""
    CREATE TABLE {DB_NAME}{FINAL_TABLE_NAME} AS
    SELECT DISTINCT
        t.tin_type,
        t.tin_value,
        rm.billing_class,
        rm.negotiated_type,
        rm.service_code,
        rm.billing_code_modifier,
        r.negotiated_rate,
        nt.npi,
        f.reporting_entity_name,
        f.reporting_entity_type,
        f.plan_name,
        f.plan_id_type,
        f.plan_id,
        f.plan_market_type,
        f.last_updated_on,
        f.version,
        f.url,
        c.billing_code
    FROM 
        {DB_NAME}tin_rate_file trf
    JOIN 
        {DB_NAME}rate r ON r.id = trf.rate_id
    JOIN 
        {DB_NAME}tin t ON t.id = trf.tin_id
    JOIN 
        {DB_NAME}npi_tin nt ON nt.tin_id = t.id
    JOIN 
        {DB_NAME}code c ON c.id = r.code_id
    JOIN 
        {DB_NAME}file f ON f.id = trf.file_id
    JOIN 
        {DB_NAME}rate_metadata rm ON rm.id = r.rate_metadata_id;
    """

    execute_query(cursor, join_query)


def export_table(cursor, table_name, export_path):
    log_progress(f"Exporting {table_name} to CSV...")
    export_query = f"COPY {table_name} TO STDOUT WITH CSV HEADER"
    with open(os.path.join(export_path, f"{table_name}.csv"), 'w', newline='') as f:
        cursor.copy_expert(export_query, f)
    log_progress(f"Export of {table_name} completed.")

def main():
    # Create the database
    create_database()

    conn = None
    try:
        log_progress("Connecting to the database...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.set_session(autocommit=True)  # Set autocommit mode
        cursor = conn.cursor()
        log_progress("Connected successfully.")

        # Create tables
        log_progress("Creating tables...")
        create_tables(cursor)

        # Import data
        log_progress("Importing data...")
        import_data(cursor)

        # Create indexes
        log_progress("Creating indexes...")
        create_indexes(cursor)

        # Perform join operation
        log_progress("Performing join operation...")
        perform_join(cursor)

        # Count rows in the new table
        log_progress("Counting rows in the new table...")
        cursor.execute(f"SELECT COUNT(*) FROM {DB_NAME}{FINAL_TABLE_NAME};")
        row_count = cursor.fetchone()[0]
        log_progress(f"Total rows in {DB_NAME}{FINAL_TABLE_NAME}: {row_count}")

        # Export the final table
        export_table(cursor, f"{DB_NAME}{FINAL_TABLE_NAME}", EXPORT_PATH)

        log_progress("All operations completed successfully.")

    except psycopg2.OperationalError as e:
        log_progress(f"Unable to connect to the database: {e}")
        log_progress("Please check your database credentials and ensure the database server is running.")
    except Exception as e:
        log_progress(f"An error occurred: {str(e)}")
    finally:
        if conn:
            conn.close()
            log_progress("Database connection closed.")

if __name__ == "__main__":
    main()