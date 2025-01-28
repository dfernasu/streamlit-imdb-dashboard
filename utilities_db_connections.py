# ///////////////////////////////////////////////////////////////////////
#
#                       UTILITIES CONNECTIONS
#   Functions to create and close connections to a snowflake account, and
#   manage the access to the local database.
#
# ///////////////////////////////////////////////////////////////////////

from configparser import ConfigParser
import psycopg2
import snowflake.connector
from utilities_navigation import get_login_state, get_credentials

# -----------------------------------------------------------------------
#                          GLOBAL PARAMETERS
# -----------------------------------------------------------------------

DB_CONF_FILE = "./database.ini"
SNOW_SECTION = "snowflake"
PSQL_SECTION = "postgresql-connection"
PSQL_DATA = "postgresql-data"

# -----------------------------------------------------------------------
#                              EXCEPTIONS
# -----------------------------------------------------------------------

def raise_unknown_error(unknown_error: Exception):
    print("[ERROR] An unknown error has occurred:")
    print(f"\t Msg: {unknown_error}")
    raise unknown_error

def raise_login_error():
    login_exception = Exception('The user is not logged into the snowflake account.')
    print(f"\t Msg: {login_exception}")
    raise login_exception

def raise_snowflake_error(snowflake_error: snowflake.connector.errors.Error):
    print("[ERROR] An error has occurred in snowflake:")
    print(f"\t Msg: {snowflake_error.msg}")
    print(f"\t Query with id {snowflake_error.sfqid} executed: {snowflake_error.query}")
    print(f"\t Error Code: {snowflake_error.errno}")
    raise snowflake_error

def raise_psql_error(psql_error):
    print("[ERROR] An error has occurred in Postgresql:")
    print(f"\t Msg: {psql_error.pgerror}")
    print(f"\t Error Code: {psql_error.pgcode}")
    raise psql_error

# -----------------------------------------------------------------------
#                           GENERAL FUNCTIONS
# -----------------------------------------------------------------------

def get_config_data(filename=DB_CONF_FILE, section=SNOW_SECTION):
    parser = ConfigParser()

    with open(filename, 'r', encoding='utf-8', errors='ignore') as file:
        parser.read_file(file)
    
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return config

def validate_credentials(username, password):
    try:

        config = get_config_data()
        config['user'] = username
        config['password'] = password

        conn = snowflake.connector.connect(**config)

        cursor = conn.cursor()
        cursor.execute(f'USE DATABASE {conn.database};')
        cursor.execute(f'USE WAREHOUSE {conn.warehouse};')
        cursor.execute(f'USE SCHEMA {conn.database}.{conn.schema};')
                
        close_snow_connection(conn)
        return True
    
    except Exception as unknown_error:
        raise_unknown_error(unknown_error)
        return False

# -----------------------------------------------------------------------
#                           SNOWFLAKE FUNCTIONS
# -----------------------------------------------------------------------
    
def create_snow_connection():
    try:
        print("\n----------- Creating a connection to the Snowflake Account -----------")

        if get_login_state():
            
            config = get_config_data()
            config['user'], config['password'] = get_credentials()
            conn = snowflake.connector.connect(**config)

            cursor = conn.cursor()
            cursor.execute(f'USE DATABASE {conn.database};')
            cursor.execute(f'USE WAREHOUSE {conn.warehouse};')
            cursor.execute(f'USE SCHEMA {conn.database}.{conn.schema};')
        else:
            raise_login_error()
        
    except snowflake.connector.errors.Error as snowflake_error:
        close_snow_connection(conn)
        raise_unknown_error(snowflake_error)
    
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
#                           POSTGRESQL FUNCTIONS
# -----------------------------------------------------------------------

def dict_to_conn_str(config):

    user = str(config['user'])
    password = str(config['password'])
    host = str(config['host'])
    port = int(config['port'])
    dbname = str(config['dbname'])

    conn_str = f"host='{host}' port={port} user='{user}' password='{password}' dbname='{dbname}'"
    return conn_str.encode('utf-8', 'ignore').decode('utf-8', 'strict')

def create_psql_connection():
    try:
        print("\n----------- Creating a connection to the Local Postgresql -----------")

        if get_login_state():
            
            config = get_config_data(section=PSQL_SECTION)
            
            conn = psycopg2.connect(dict_to_conn_str(config))

            cursor = conn.cursor()
            cursor.execute('SELECT current_database();')
            connected_database = cursor.fetchall()[0][0]

            assert(connected_database == str(config['dbname']))
        else:
            raise_login_error()
    
    except (psycopg2.DatabaseError, psycopg2.OperationalError, psycopg2.DataError, psycopg2.IntegrityError, psycopg2.InternalError, psycopg2.ProgrammingError) as psql_error:
        raise_psql_error(psql_error)

    except Exception as unknown_error:
        raise_unknown_error(unknown_error)

    else:
        print(f"[SUCCESS] Connected to the Local PostgreSQL with user {config['user']} and the database {connected_database}")
        return conn
    
def close_psql_connection(conn):
    if(conn is not None):
        print("\n----------- Closing the connection with the Local PostgreSQL -----------")
        conn.close()
        print("[SUCCESS] Connection to PostgreSQL closed successfully.")
    else:
        print("[ERROR] Not a valid connection to close.")
    
conn = create_psql_connection()
close_psql_connection(conn)

