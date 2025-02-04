# ///////////////////////////////////////////////////////////////////////
#
#                           LOAD INTO SNOW
#   This script is executed apart from the app, and it is used to upload
#   the .csv files in the datasets folder into the Snowflake database.
#
# ///////////////////////////////////////////////////////////////////////

from utilities_db_connections import *
from snowflake.connector.errors import Error
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import logging as log

# -----------------------------------------------------------------------
#                          GLOBAL PARAMETERS
# -----------------------------------------------------------------------

PROYECT_FOLDER = "C:/Users/dfernasu/OneDrive - NTT DATA EMEAL/Documentos/TareasSnowflake/ProyectoSnowStreamlit"
DATASETS_FOLDER = f"{PROYECT_FOLDER}/datasets/"
SQL_SCRIPT_PATH = f"{PROYECT_FOLDER}/scripts/schema_creation_snow.sql"

DATABASE = 'PRACTICE_DATASETS'
SCHEMA = 'IMDB_DWH'
WAREHOUSE = 'COMPUTE_WH'

conn = None
cursor = None

datasets_names = ['dim_genres', 'dim_actors', 'dim_directors', 'dim_years', 'bridge_genres', 'bridge_actors', 'fact_table']

# -----------------------------------------------------------------------
#                              FUNCTIONS
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
    log.info(f"\t - Loaded: {complete_path}")

    return dataset


def load_table_from_dataset(conn: SnowflakeConnection, table_name: str, dataset: pd.DataFrame):
    write_pandas(conn=conn, database=DATABASE, schema=SCHEMA, table_name=table_name.upper(), auto_create_table=False, df=dataset)

    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    num_rows_inserted = int(cursor.fetchone()[0])

    assert(num_rows_inserted == len(dataset))
    
    log.info(f"\t - The table: {table_name} was loaded with {num_rows_inserted} rows.")


# -----------------------------------------------------------------------
#                                MAIN
# -----------------------------------------------------------------------

try:
    conn = create_snow_connection(DATABASE, WAREHOUSE)
    
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