/* ///////////////////////////////////////////////////////////////////////////
                            SNOWFLAKE CONFIGURATION

    This script is designed to configure the Snowflake Account with all the 
    necessary components and restrictions for the project.

    Note 1: This script is intended to be executed section by section, 
            rather than all at once.

    Note 2: The account must be Enterprise Edition or higher. If this is 
            not possible, the Access Policy section can be skipped.
    
/////////////////////////////////////////////////////////////////////////// */

/* -----------------------------------------------------------------------------
                    DATABASE, SCHEMA AND WAREHOUSE CREATION
-----------------------------------------------------------------------------*/

USE ROLE sysadmin;

CREATE OR REPLACE DATABASE PRACTICE_DATASETS;
CREATE OR REPLACE SCHEMA PRACTICE_DATASETS.IMDB_DWH;

CREATE OR REPLACE WAREHOUSE IMDB_WH WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

/* ----------------------------------------------------------------------------
                    ROLE CREATION: USERIMDB
    
    This role is intended to restrict privileges for new users except 
    the sysadmin (the owner of the database).
-----------------------------------------------------------------------------*/

USE ROLE useradmin;

CREATE OR REPLACE ROLE USERIMDB WITH
    COMMENT = 'Role given to external users to access the IMDB Schema';

-- Grant access to the resources

USE ROLE sysadmin; -- The Owner of the resources

USE DATABASE PRACTICE_DATASETS;
USE SCHEMA PRACTICE_DATASETS.IMDB_DWH;

GRANT USAGE ON DATABASE PRACTICE_DATASETS TO ROLE USERIMDB;
GRANT USAGE ON SCHEMA IMDB_DWH TO ROLE USERIMDB;

GRANT SELECT ON ALL TABLES IN SCHEMA IMDB_DWH TO ROLE USERIMDB;

GRANT USAGE ON WAREHOUSE IMDB_WH TO ROLE USERIMDB;

/* ----------------------------------------------------------------------------
                            IMDB USER CREATION
    
    This two users represent the new users of the application and they 
    belong to the role USERIMDB.

    These users only have usage and select privileges with the schema IMDB_DWH, 
    and can only use the warehouse IMDB_WH, wich also has a Control Monitor 
    administrated by the ACCOUNTADMIN.
-----------------------------------------------------------------------------*/

USE ROLE useradmin;

CREATE OR REPLACE USER IMDB_USER_1 WITH
    LOGIN_NAME = 'IMDB_USER_1'
    PASSWORD = '@OtctL(H$r._X_43~86£X5Ae{1'
    EMAIL = 'dfernasu1_user1@imdb.snow.com'
    COMMENT = 'External user that can only access the IMDB schema and use the action SELECT'
    DEFAULT_ROLE = USERIMDB
    DEFAULT_WAREHOUSE = IMDB_WH
    DEFAULT_NAMESPACE = 'PRACTICE_DATASETS.IMDB_DWH';
    
CREATE OR REPLACE USER IMDB_USER_2 WITH
    LOGIN_NAME = 'IMDB_USER_2'
    PASSWORD = '@OtctL(H$r._X_43~86£X5Ae{2'
    EMAIL = 'dfernasu1_user2@imdb.snow.com'
    COMMENT = 'External user that can only access the IMDB schema and use the action SELECT'
    DEFAULT_ROLE = USERIMDB
    DEFAULT_WAREHOUSE = IMDB_WH
    DEFAULT_NAMESPACE = 'PRACTICE_DATASETS.IMDB_DWH';

GRANT ROLE USERIMDB TO USER IMDB_USER_1;
GRANT ROLE USERIMDB TO USER IMDB_USER_2;

/* ----------------------------------------------------------------------------
                            TEST USER CREATION
    
    Addition of a test user, for testing conexions to the account, that has 
    no rights other than usage (not selects).
-----------------------------------------------------------------------------*/

-- User and role creation
USE ROLE useradmin;

CREATE OR REPLACE ROLE TEST_USERIMDB WITH
    COMMENT = 'Role used for testing conexions';

CREATE OR REPLACE USER TEST_USER WITH
    LOGIN_NAME = 'TEST_USER'
    PASSWORD = 'test_userJf^746'
    EMAIL = 'test_user@imdb.snow.com'
    COMMENT = 'User used for testing conexions'
    DEFAULT_ROLE = TEST_USERIMDB
    DEFAULT_WAREHOUSE = IMDB_WH
    DEFAULT_NAMESPACE = 'PRACTICE_DATASETS.IMDB_DWH';

GRANT ROLE TEST_USERIMDB TO USER TEST_USER;

-- Grant permissions to the test user

USE ROLE sysadmin;

GRANT USAGE ON WAREHOUSE IMDB_WH TO ROLE TEST_USERIMDB;

USE DATABASE PRACTICE_DATASETS;
USE SCHEMA PRACTICE_DATASETS.IMDB_DWH;

GRANT USAGE ON DATABASE PRACTICE_DATASETS TO ROLE TEST_USERIMDB;
GRANT USAGE ON SCHEMA IMDB_DWH TO ROLE TEST_USERIMDB;

/* ----------------------------------------------------------------------------
                        RESOURCE MONITOR CREATION
    
    This monitor establishes certain restrictions on the usage of the IMDB_WH 
    for IMDB users.
-----------------------------------------------------------------------------*/

USE ROLE accountadmin;

CREATE OR REPLACE RESOURCE MONITOR IMDB_USER_MONITOR WITH
    CREDIT_QUOTA = 20
    FREQUENCY = 'DAILY'
    START_TIMESTAMP = 'IMMEDIATELY'
    NOTIFY_USERS = ('DFERNASU1')
    TRIGGERS
         ON 90 PERCENT DO SUSPEND
         ON 95 PERCENT DO SUSPEND_IMMEDIATE
         ON 70 PERCENT DO NOTIFY
         ON 80 PERCENT DO NOTIFY;


ALTER WAREHOUSE IMDB_WH SET RESOURCE_MONITOR = IMDB_USER_MONITOR;

SHOW RESOURCE MONITORS;

/* ----------------------------------------------------------------------------
                        ACCESS POLICY CREATION
    
    This Access Policy restricts certain years of the database for IMDB users,
    allowing only the sysadmin to view all years of the dataset.

    NOTE: Before executing this section, ensure that the schema has all tables
          created. This can be achieved by running the load_into_snow.py script.

    IMPORTANT: Enterprise Edition required
-----------------------------------------------------------------------------*/

USE ROLE sysadmin;

USE DATABASE PRACTICE_DATASETS;
USE SCHEMA PRACTICE_DATASETS.IMDB_DWH;

CREATE OR REPLACE ROW ACCESS POLICY IMDB_DWH.year_access_policy 
AS (data_row NUMBER)
RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() = 'SYSADMIN' THEN TRUE
        WHEN data_row >= 2010 THEN TRUE
        ELSE FALSE
    END;

CREATE OR REPLACE ROW ACCESS POLICY IMDB_DWH.year_id_access_policy 
AS (data_row NUMBER)
RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() = 'SYSADMIN' THEN TRUE
        WHEN EXISTS (
            SELECT 1 
            FROM IMDB_DWH.DIM_YEARS 
            WHERE DIM_YEARS.year_id = data_row 
            AND DIM_YEARS.year >= 2010
        ) THEN TRUE
        ELSE FALSE
    END;

ALTER TABLE IMDB_DWH.DIM_YEARS
ADD ROW ACCESS POLICY IMDB_DWH.year_access_policy 
ON ("YEAR");

ALTER TABLE IMDB_DWH.FACT_TABLE
ADD ROW ACCESS POLICY IMDB_DWH.year_id_access_policy 
ON ("YEAR_ID");

ALTER TABLE IMDB_DWH.DIM_YEARS
DROP ROW ACCESS POLICY IMDB_DWH.year_access_policy;

ALTER TABLE IMDB_DWH.FACT_TABLE
DROP ROW ACCESS POLICY IMDB_DWH.year_id_access_policy;