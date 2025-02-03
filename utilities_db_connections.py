# ///////////////////////////////////////////////////////////////////////
#
#                       UTILITIES CONNECTIONS
#   Functions to create and close connections to a snowflake account, and
#   manage the access to the local database.
#
# ///////////////////////////////////////////////////////////////////////

from dotenv import load_dotenv
import os
import psycopg2 as psy
import snowflake.connector
from utilities_navigation import get_login_state, get_credentials
import logging as log
from global_parameters import LOGGER_DB_CONNECTIONS_KEY

logger_db_conn = log.getLogger(LOGGER_DB_CONNECTIONS_KEY)
load_dotenv()

# -----------------------------------------------------------------------
#                              EXCEPTIONS
# -----------------------------------------------------------------------

def raise_unknown_error(unknown_error: Exception):
    logger_db_conn.error("[ERROR] An unknown error has occurred:\n\t- Msg: {unknown_error}")
    raise unknown_error

def raise_login_error():
    login_exception = Exception('The user is not logged into the snowflake account.')
    raise login_exception

def raise_snowflake_error(snowflake_error: snowflake.connector.errors.Error):
    logger_db_conn.error(f"[ERROR] An error has occurred in snowflake:\n\t- Msg: {snowflake_error.msg}\n\t- Query with id {snowflake_error.sfqid} executed: {snowflake_error.query}\n\t- Error Code: {snowflake_error.errno}")
    raise snowflake_error

def raise_psql_error(psql_error):
    logger_db_conn.error(f"[ERROR] An error has occurred in Postgresql:\n\t- Msg: {psql_error.pgerror}\n\t- Error Code: {psql_error.pgcode}")
    raise psql_error

def raise_missing_env_variable(key_error: KeyError):
    logger_db_conn.error(f"[ERROR] An getenvment Variable was not configured:\n\t- Msg: {key_error}")
    raise key_error

# -----------------------------------------------------------------------
#                           SNOWFLAKE FUNCTIONS
# -----------------------------------------------------------------------

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
        raise_missing_env_variable(error)
        return None
    
    return config

def validate_credentials(username, password):
    try:

        config = get_snow_config()
        config['user'] = username
        config['password'] = password

        conn = snowflake.connector.connect(**config)

        cursor = conn.cursor()
        cursor.execute(f'USE DATABASE {conn.database};')
        cursor.execute(f'USE WAREHOUSE {conn.warehouse};')
        cursor.execute(f'USE SCHEMA {conn.database}.{conn.schema};')
                
        close_snow_connection(conn)
        return True
    
    except snowflake.connector.errors.DatabaseError:
        return False
    except Exception as unknown_error:
        raise_unknown_error(unknown_error)
        return False
    
def create_snow_connection():
    try:
        logger_db_conn.info("\n----------- Creating a connection to the Snowflake Account -----------")

        if get_login_state():
            
            config = get_snow_config()
            config['user'], config['password'] = get_credentials()
            conn = snowflake.connector.connect(**config)

            cursor = conn.cursor()
            cursor.execute(f'USE DATABASE {conn.database};')
            cursor.execute(f'USE WAREHOUSE {conn.warehouse};')
            cursor.execute(f'USE SCHEMA {conn.database}.{conn.schema};')
        else:
            raise_login_error()
        
    except snowflake.connector.errors as snowflake_error:
        close_snow_connection(conn)
        raise_snowflake_error(snowflake_error)
    
    except Exception as unknown_error:
        close_snow_connection(conn)
        raise_unknown_error(unknown_error)

    else:
        logger_db_conn.info(f"[SUCCESS] Connected to the account: {conn.account} with user {conn.user}, using the database {conn.database} and the warehouse {conn.warehouse}")
        return conn
    
def close_snow_connection(conn):
    if(conn is not None):
        logger_db_conn.info("\n----------- Closing the connection with the Snowflake Account -----------")
        conn.close()
        logger_db_conn.info("[SUCCESS] Connection to Snowflake closed successfully.")
    else:
        logger_db_conn.error("[ERROR] Not a valid connection to close.")

# -----------------------------------------------------------------------
#                           POSTGRESQL FUNCTIONS
# -----------------------------------------------------------------------

def get_psql_config():
    config = {}

    try:
        config["user"] = str(os.getenv("POSTGRES_USER"))
        config["password"] = str(os.getenv("POSTGRES_PASSWORD"))
        config["dbname"] = str(os.getenv("POSTGRES_DATABASE"))
        config["host"] = str(os.getenv("POSTGRES_HOST"))
        config["port"] = int(os.getenv("POSTGRES_PORT"))

    except KeyError as error:
        raise_missing_env_variable(error)
        return None
    
    return config

def create_psql_connection():
    try:
        logger_db_conn.info("\n----------- Creating a connection to the Local Postgresql -----------")

        if get_login_state():
            
            config = get_psql_config()
            conn = psy.connect(**config)

            cursor = conn.cursor()
            cursor.execute('SELECT current_database();')
            connected_database = cursor.fetchall()[0][0]

            assert(connected_database == str(config['dbname']))
        else:
            raise_login_error()
    
    except (psy.DatabaseError, psy.OperationalError, psy.DataError, psy.IntegrityError, psy.InternalError, psy.ProgrammingError) as psql_error:
        raise_psql_error(psql_error)

    except Exception as unknown_error:
        raise_unknown_error(unknown_error)

    else:
        logger_db_conn.info(f"[SUCCESS] Connected to the Local PostgreSQL with user {config['user']} and the database {connected_database}")
        return conn
    
def close_psql_connection(conn):
    if(conn is not None):
        logger_db_conn.info("\n----------- Closing the connection with the Local PostgreSQL -----------")
        conn.close()
        logger_db_conn.info("[SUCCESS] Connection to PostgreSQL closed successfully.")
    else:
        logger_db_conn.error("[ERROR] Not a valid connection to close.")
