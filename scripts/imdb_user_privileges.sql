/* -----------------------------------------------------------------------------
    This script is used to create and configure the privileges of two new users.
    These users only have access to the schema IMDB_DWH, and can only use the 
    warehouse IMDB_WH, wich also has a Control Monitor administrated by the
    ACCOUNTADMIN.
-----------------------------------------------------------------------------*/

-- CREATION OF A SPECIFIC ROLE TO ACCESS THE IMDB SCHEMA
USE ROLE useradmin;
CREATE OR REPLACE ROLE USERIMDB WITH
    COMMENT = 'Role given to external users to access the IMDB Schema';

-- CREATION OF A WAREHOUSE ONLY FOR USERIMDB 
USE ROLE sysadmin;
CREATE OR REPLACE WAREHOUSE IMDB_WH WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
    
GRANT USAGE ON WAREHOUSE IMDB_WH TO ROLE USERIMDB;

--  GRANT ACCESS FOR THE IMDB SCHEMA TO THE ROLE USERIMDB
USE ROLE sysadmin; -- The Owner of the database
USE DATABASE PRACTICE_DATASETS;
USE SCHEMA PRACTICE_DATASETS.IMDB_DWH;

GRANT USAGE ON DATABASE PRACTICE_DATASETS TO ROLE USERIMDB;
GRANT USAGE ON SCHEMA IMDB_DWH TO ROLE USERIMDB;

GRANT SELECT ON ALL TABLES IN SCHEMA IMDB_DWH TO ROLE USERIMDB;

-- CREATION OF THE EXTERNAL USERS (IMDB_USER_1 and IMDB_USER_2) WITH ROLE USERIMDB
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

-- CREATION OF A RESOURCE MONITOR, TO CONTROL THE USAGE OF THE IMDB_WH
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

-- Assign the warehouse to the resource monitor
ALTER WAREHOUSE IMDB_WH SET RESOURCE_MONITOR = IMDB_USER_MONITOR;

SHOW RESOURCE MONITORS;

-- Addition of a Row Policy so only the sysadmin can see all years in the dataset:
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

-- Addition of a test user, for testing conexions to the account, that has no rights
--    other than usage (not selects):

USE ROLE useradmin;
CREATE OR REPLACE ROLE TEST_USERIMDB WITH
    COMMENT = 'Role used for testing conexions';

USE ROLE sysadmin;
GRANT USAGE ON WAREHOUSE IMDB_WH TO ROLE TEST_USERIMDB;
USE DATABASE PRACTICE_DATASETS;
USE SCHEMA PRACTICE_DATASETS.IMDB_DWH;
GRANT USAGE ON DATABASE PRACTICE_DATASETS TO ROLE TEST_USERIMDB;
GRANT USAGE ON SCHEMA IMDB_DWH TO ROLE TEST_USERIMDB;

USE ROLE useradmin;
CREATE OR REPLACE USER TEST_USER WITH
    LOGIN_NAME = 'TEST_USER'
    PASSWORD = 'test_userJf^746'
    EMAIL = 'test_user@imdb.snow.com'
    COMMENT = 'User used for testing conexions'
    DEFAULT_ROLE = TEST_USERIMDB
    DEFAULT_WAREHOUSE = IMDB_WH
    DEFAULT_NAMESPACE = 'PRACTICE_DATASETS.IMDB_DWH';

GRANT ROLE TEST_USERIMDB TO USER TEST_USER;