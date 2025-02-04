# ///////////////////////////////////////////////////////////////////////
#
#                       TEST DB CONNECTIONS
#   Python script used for testing the functions used to connect to
#   the snowflake account and the local Postgres database.
#
# ///////////////////////////////////////////////////////////////////////

import unittest
import os
from dotenv import load_dotenv
from unittest.mock import patch
from utilities_db_connections import validate_credentials, create_snow_connection, create_psql_connection, close_snow_connection, close_psql_connection

load_dotenv()

class TestDBConnections(unittest.TestCase):

    def setUp(self):
        self.validate_credentials = None
        self.snow_conn = None
        self.psql_conn = None

    def test_validate_credentials_success(self):
        user = str(os.getenv('SNOW_USER_TESTING'))
        password = str(os.getenv('SNOW_PASSWORD_TESTING'))

        self.validate_credentials = validate_credentials(user, password)
        self.assertTrue(self.validate_credentials, "Validate credentials should be True")

    def test_validate_credentials_failure(self):
        user = 'invalid_user'
        password = 'invalid_password'

        self.validate_credentials = validate_credentials(user, password)
        self.assertFalse(self.validate_credentials, "Validate credentials should be False")

    @patch('utilities_db_connections.get_login_state', return_value=True)
    @patch('utilities_db_connections.get_credentials', return_value=(str(os.getenv('SNOW_USER_TESTING')), str(os.getenv('SNOW_PASSWORD_TESTING'))))
    def test_create_snow_connection_success(self, mock_get_login_state, mock_get_credentials):
        self.snow_conn = create_snow_connection()
        self.assertIsNotNone(self.snow_conn, "Snowflake connection should not be None")

    @patch('utilities_db_connections.get_login_state', return_value=True)
    @patch('utilities_db_connections.get_credentials', return_value=('invalid_user', 'invalid_password'))
    def test_snow_connection_failure(self, mock_get_login_state, mock_get_credentials):
        with self.assertRaises(Exception):
            create_snow_connection()

    @patch('utilities_db_connections.get_login_state', return_value=True)
    def test_create_psql_connection(self, mock_get_login_state):

        self.psql_conn = create_psql_connection()
        self.assertIsNotNone(self.psql_conn, "PostgreSQL connection should not be None")

    @patch('utilities_db_connections.get_login_state', return_value=False)
    @patch('utilities_db_connections.get_credentials', return_value=(str(os.getenv('SNOW_USER_TESTING')), str(os.getenv('SNOW_PASSWORD_TESTING'))))
    def test_create_snow_connection_no_login(self, mock_get_login_state, mock_get_credentials):
        with self.assertRaises(Exception):
            create_snow_connection()

    @patch('utilities_db_connections.get_login_state', return_value=False)
    def test_create_psql_connection_no_login(self, mock_get_login_state):
        with self.assertRaises(Exception):
            create_psql_connection()

    def tearDown(self):
        if self.snow_conn:
            close_snow_connection(self.snow_conn)
        if self.psql_conn:
            close_psql_connection(self.psql_conn)

if __name__ == '__main__':
    unittest.main()