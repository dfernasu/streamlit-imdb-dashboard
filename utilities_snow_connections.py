# ///////////////////////////////////////////////////////////////////////
#
#                       UTILITIES SNOW CONNECTION
#   Functions to create and close connections to a snowflake account.
#
# ///////////////////////////////////////////////////////////////////////

import snowflake.connector
from utilities_navigation import get_login_state, get_credentials

# -----------------------------------------------------------------------
#                          GLOBAL PARAMETERS
# -----------------------------------------------------------------------

DATABASE = 'PRACTICE_DATASETS'
SCHEMA = 'IMDB_DWH'
WAREHOUSE = 'IMDB_WH'
ACCOUNT = 'MLITLJS-TF67445'

# -----------------------------------------------------------------------
#                              FUNCTIONS
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


def validate_credentials(username, password):
    try:
        conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=ACCOUNT,
            database=DATABASE,
            schema=SCHEMA,
            warehouse=WAREHOUSE
        )

        cursor = conn.cursor()
        cursor.execute(f'USE DATABASE {conn.database};')
        cursor.execute(f'USE WAREHOUSE {conn.warehouse};')
        cursor.execute(f'USE SCHEMA {conn.database}.{conn.schema};')
                
        close_connection(conn)
        return True
    
    except Exception as unknown_error:
        print(f'[ERROR] {unknown_error}')
        return False
    
    
def create_connection():
    try:
        print("\n----------- Creating a connection to the Snowflake Account -----------")

        if get_login_state():
            username, password = get_credentials()

            #TODO: Remove this line after tests
            print(f'[INFO] Used credentials for login: {username} - {password}')

            conn = snowflake.connector.connect(
            autocommit=False,
            user=username,
            password=password,
            account=ACCOUNT,
            database=DATABASE,
            schema=SCHEMA,
            warehouse=WAREHOUSE
            )

            cursor = conn.cursor()
            cursor.execute(f'USE DATABASE {conn.database};')
            cursor.execute(f'USE WAREHOUSE {conn.warehouse};')
            cursor.execute(f'USE SCHEMA {conn.database}.{conn.schema};')
        else:
            raise_login_error()
        
    except snowflake.connector.errors.Error as snowflake_error:
        close_connection(conn)
        raise_unknown_error(snowflake_error)
    
    except Exception as unknown_error:
        close_connection(conn)
        raise_unknown_error(unknown_error)

    else:
        print(f"[SUCCESS] Connected to the account: {conn.account} with user {conn.user}, using the database {conn.database} and the warehouse {conn.warehouse}")
        return conn
    
    
def close_connection(conn):
    if(conn is not None):
        print("\n----------- Closing the connection with the Snowflake Account -----------")
        conn.close()
        print("[SUCCESS] Connection to Snowflake closed successfully.")
    else:
        print("[ERROR] Not a valid connection to close.")

