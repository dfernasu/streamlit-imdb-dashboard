# ///////////////////////////////////////////////////////////////////////
#
#                           LOAD INTO SNOW
#   This script is executed apart from the app, and it is used to upload
#   the .csv files in the datasets folder into the Snowflake database.
#
# ///////////////////////////////////////////////////////////////////////

import os
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.errors import Error
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import logging as log

load_dotenv()

# -----------------------------------------------------------------------
#                          GLOBAL PARAMETERS
# -----------------------------------------------------------------------

PROYECT_FOLDER = os.getenv("PROYECT_FOLDER")
DATASETS_FOLDER = f"{PROYECT_FOLDER}/datasets/"
SQL_SCRIPT_PATH = f"{PROYECT_FOLDER}/scripts/schema_creation_snow.sql"

DATABASE = os.getenv("SNOW_DATABASE")
SCHEMA = os.getenv("SNOW_SCHEMA")
WAREHOUSE = os.getenv("SNOW_WAREHOUSE")

conn = None
cursor = None

datasets_names = ['dim_genres', 'dim_actors', 'dim_directors', 'dim_years', 'bridge_genres', 'bridge_actors', 'fact_table']

# -----------------------------------------------------------------------
#                        CONNECTION FUNCTIONS
# -----------------------------------------------------------------------

def raise_unknown_error(unknown_error: Exception):
    print(f"[ERROR] An unknown error has occurred:\n\t- Msg: {unknown_error}")
    raise unknown_error

def raise_snowflake_error(snowflake_error: Exception):
    print(f"[ERROR] An error has occurred in snowflake:\n\t- Msg: {snowflake_error.msg}\n\t- Query with id {snowflake_error.sfqid} executed: {snowflake_error.query}\n\t- Error Code: {snowflake_error.errno}")
    raise snowflake_error

def get_snow_config():
    config = {}

    try:
        config["autocommit"] = bool(os.getenv("SNOW_AUTOCOMMIT"))
        config["account"] = str(os.getenv("SNOW_ACCOUNT"))
        config["user"] = str(os.getenv("SNOW_USER"))
        config["password"] = str(os.getenv("SNOW_PASSWORD"))
        config["database"] = str(os.getenv("SNOW_DATABASE"))
        config["schema"] = str(os.getenv("SNOW_SCHEMA"))
        config["warehouse"] = str(os.getenv("SNOW_WAREHOUSE"))

    except KeyError as error:
        print(f"[ERROR] An getenvment Variable was not configured:\n\t- Msg: {error}")
        return None
    
    return config

def create_snow_connection():
    try:
        print("\n----------- Creating a connection to the Snowflake Account -----------")

        config = get_snow_config()
        conn = snowflake.connector.connect(**config)

        cursor = conn.cursor()
        cursor.execute(f'USE DATABASE {conn.database};')
        cursor.execute(f'USE WAREHOUSE {conn.warehouse};')
        cursor.execute(f'USE SCHEMA {conn.database}.{conn.schema};')
        cursor.close()
        
    except snowflake.connector.errors as snowflake_error:
        close_snow_connection(conn)
        raise_snowflake_error(snowflake_error)

    except Exception as unknown_error:
        close_snow_connection(conn)
        raise_unknown_error(unknown_error)

    else:
        print(f"[SUCCESS] Connected to the account: {conn.account} with user {conn.user}, using the database {conn.database} and the warehouse {conn.warehouse}")
        return conn
    
    
def close_snow_connection(conn):
    if(conn is not None):
        print("\n----------- Closing the connection with the Snowflake Account -----------")
        conn.close()
        print("[SUCCESS] Connection to Snowflake closed successfully.")
    else:
        print("[ERROR] Not a valid connection to close.")

# -----------------------------------------------------------------------
#                           DATA FUNCTIONS
# -----------------------------------------------------------------------

def schema_creation():

    log.info(f"\n----------- Creation of the schema {SCHEMA} -----------")

    with open(SQL_SCRIPT_PATH, 'r', encoding='utf-8') as file:
        for cursor in conn.execute_stream(file):
            for result in cursor:
                log.info(f"\t - {result}")
        file.close()

    log.info(f"[SUCCESS] Schema {SCHEMA} created.")


def load_dataset(name: str):
    complete_path = DATASETS_FOLDER + name + '.csv'
    dataset = pd.read_csv(complete_path,sep=',', header=0)
    print(f"\t - Loaded: {complete_path}")

    return dataset


def load_table_from_dataset(conn: SnowflakeConnection, table_name: str, dataset: pd.DataFrame):
    write_pandas(conn=conn, database=DATABASE, schema=SCHEMA, table_name=table_name.upper(), auto_create_table=False, df=dataset)

    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    num_rows_inserted = int(cursor.fetchone()[0])

    assert(num_rows_inserted == len(dataset))
    
    print(f"\t - The table: {table_name} was loaded with {num_rows_inserted} rows.")


# -----------------------------------------------------------------------
#                                MAIN
# -----------------------------------------------------------------------

try:
    conn = create_snow_connection()
    
    if(conn is not None):
        
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {DATABASE};")
        cursor.execute(f"USE SCHEMA {SCHEMA};")
        
        schema_creation()

        log.info(f"\n----------- Loading datasets from {DATASETS_FOLDER} -----------")

        datasets = []
        for dataset_name in datasets_names:

            df = load_dataset(dataset_name)
            load_table_from_dataset(conn, dataset_name, df)

        conn.commit()

except snowflake.connector.errors.Error as snowflake_error:
    conn.rollback()
    raise_snowflake_error(snowflake_error)

except Exception as unknown_error:
    conn.rollback()
    raise_unknown_error(unknown_error)

finally:
    close_snow_connection(conn)