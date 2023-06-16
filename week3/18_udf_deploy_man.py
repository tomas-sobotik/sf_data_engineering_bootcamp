#Step 1: Define the function for the UDF

def double(input_value: int):
  return input_value*2


#Step 2: we need to import data type for returned value
from snowflake.snowpark.types import IntegerType

#Step 3:  Register UDF in Snowflake and Upload UDF to Snowflake
from snowflake.snowpark import Session

connection_parameters = {
    "account": "<your account>",
    "user": "<your username>",
    "password": "<your password>",
   "role": "SYSADMIN",  # optional
   "warehouse": "COMPUTE_WH",  # optional
    "database": "DATA_ENGINEERING",  # optional
    "schema": "PUBLIC",  # optional
}

snowpark_session = Session.builder.configs(connection_parameters).create()

snowpark_session.udf.register(
    func = double
  , return_type = IntegerType()
  , input_types = [IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_DOUBLE'
  , replace = True
  , stage_location = '@UDF_STAGE'
)

#Step 4: Test the UDF by calling
snowpark_session.sql('SELECT SNOWPARK_DOUBLE(2)').show()
