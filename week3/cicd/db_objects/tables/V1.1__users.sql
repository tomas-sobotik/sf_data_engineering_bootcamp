

-- Set the database and schema context
USE DATABASE {{ target_db }};
USE SCHEMA PUBLIC;   
  
-- Create the tables
CREATE OR REPLACE TABLE USERS 
(
     FIRST_NAME VARCHAR
    ,LAST_NAME VARCHAR
    ,EMAIL VARCHAR
    ,BIRTH_YEAR INTEGER
    ,GENDER INTEGER
);