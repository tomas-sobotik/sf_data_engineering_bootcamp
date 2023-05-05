----------------------------- 14 SQL API -----------------------------------------------

 --check if the user has assigned the public key
 desc user <your_username>;

 --run in terminal: generating encrypted private key
 -- openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out rsa_key.p8

 --we got key in pem format
 --cat rsa_key.p8

 --generate public key
--openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

--assigning the public key to your user - without the public key delimiters
use role accountadmin;

alter user <your_username> set rsa_public_key = '<your_public_key>';

--check the user
desc user <your_username>;

--we can verify the generated private key in snowSQL - pass the path to the private key
--snowsql -a <your_account> -u <your_username> --private-key-path rsa_key.p8

--generating the JWT token by using snowsql
/*
This JWT token is time limited token which has been signed with your key and Snowflake will know that you authorized this token to be used to authenticate as you for the SQL API.
*/
--snowsql -a <account> -u <user> --private-key-path <path to private key> --generate-jwt

--We need to set following header in each API request
/*
Authorization: Bearer *jwt_token*
X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT
*/

--The complete request will look like
/*
POST /api/v2/statements?requestId=<UUID> HTTP/1.1
Authorization: Bearer <jwt_token>
Content-Type: application/json
Accept: application/json
User-Agent: myApplication/1.0
X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT

{

..request body
}

*/
--we are going to use this simple query in request body
/*
{
"statement": "select * from gpt_tweets limit 20",
"timeout": 60,
"database": "DATA_ENGINEERING",
"schema": "PUBLIC",
"warehouse": "COMPUTE_WH",
"role": "SYSADMIN"
}
*/

--let's check the query in query history

--we can try to send one more request to check the status of the previous one. We need a statementHandle value from the response of the first API request and use it in following GET command

--GET api/v2/statements/{statementHandle}

--As a response we will a information that this request has been done successfully
/*
...
...

   "statementHandle": "01abb4e1-0202-6f06-0000-cd55001e2232",
    "message": "Statement executed successfully.",

    ....
    ....


*/

------------------------------------ 16 External function -------------------------------------

--use accountadmin role
use role accountadmin;

--create api integration - not run it again
create or replace api integration my_aws_api_integration
  api_provider = aws_api_gateway
  api_aws_role_arn = '<your_role_arn>'
  api_allowed_prefixes = ('<your_api_prefix>')
  enabled = true
;

--desc integration
describe integration my_aws_api_integration;

--DDL for external function
create or replace external function translate_text(message string)
    returns variant
    api_integration = my_aws_api_integration
    as '<your_api_prefix>'
    ;

--test the function out
select 'Hello' msg, translate_text(msg);
select 'How are you?' msg, translate_text(msg);


------------------------------------ 17 Snowpark transformations ------------------------------

select * from gpt_tweets;

--active users summary -> we will rewrite it into snowpark code
select username, count(id) tweets_count, sum(replycount) sum_reply, sum(retweetcount) sum_retweet, sum(likecount) sum_like
from gpt_tweets
group by username
having count(id) > 1
order by count(id) desc;

select * from snowpipe_landing;


------------------------------------ 18 Deploying Snowpark UDF ------------------------------

--create a stage for storing our Python UDF code
create or replace stage udf_stage;

show stages;

--check if function has been created
show user functions;

--show content of the stage
ls @UDF_STAGE;


SELECT SNOWPARK_DOUBLE(2);

--check the stages once we deployed the function via snowCLI
show stages;

--check the content of the stage
ls @DEPLOYMENTS;

--run the udf in Snowflake
SELECT helloFunction();

------------------------------------ 19 Data governance ------------------------------

--let's analyze the snowpipe landing table for sensitive personal data
select extract_semantic_categories('data_engineering.public.snowpipe_landing');

--we can flatten the result
SELECT
    f.key::varchar as column_name,
    f.value:"privacy_category"::varchar as privacy_category,
    f.value:"semantic_category"::varchar as semantic_category,
    f.value:"extra_info":"probability"::number(10,2) as probability,
    f.value:"extra_info":"alternates"::variant as alternates
  FROM
  TABLE(FLATTEN(EXTRACT_SEMANTIC_CATEGORIES('data_engineering.public.snowpipe_landing')::VARIANT)) AS f;

--what are the possible semantic category tags
select system$get_tag_allowed_values('snowflake.core.semantic_category');

--what are the possible privacy category tags
select system$get_tag_allowed_values('snowflake.core.privacy_category');

--apply tags automatically based on result from previus command
call ASSOCIATE_SEMANTIC_CATEGORY_TAGS('data_engineering.public.snowpipe_landing', extract_semantic_categories('data_engineering.public.snowpipe_landing'));

--check out the assigned tags - there is latency up to 2 hours for those tags related views
select * from snowflake.account_usage.tag_references;


-----------------------------------------Access policies----------------------------------------

--let' use the schema owner role - it has CREATE ROW ACCESS POLICY privilege
use role sysadmin;

--create mapping table
create or replace table manager_region
(
    manager_role varchar,
    region varchar
);

--populate table with few records
insert into manager_region values ('SALES_EU', 'EU');
insert into manager_region values ('SALES_APAC', 'APAC');
insert into manager_region values ('SALES_US', 'US');

select * from manager_region;

--prepare the data
create or replace table sales like snowpipe_landing;

--add two more columns
alter table sales add column region varchar;
alter table sales add column revenue number;

--randomly select one region for each person
insert into sales
with arr as (
    select array_construct('EU','APAC','US') region
),
managers as
(select id, first_name, last_name, gender, email,city, random() revenue  from snowpipe_landing)

select
    id,
    first_name,
    last_name,
    gender,
    email,
    city,
    to_varchar(arr.region[ABS(MOD(RANDOM(), array_size(arr.region)))]),
    revenue
from
    arr,
    managers;

--check out our source table
select * from sales;

--check the mapping table
select * from manager_region;

--lets create some more roles to test the policy later
use role securityadmin;
create or replace role SALES_EXECUTIVE;
create or replace role SALES_EU;

--grant them possibility to select the data from our source table
grant usage on database data_engineering to role sales_executive;
grant usage on database data_engineering to role sales_eu;
grant usage on schema public to role sales_executive;
grant usage on schema public to role sales_eu;
grant usage on warehouse compute_wh to role sales_executive;
grant usage on warehouse compute_wh to role sales_eu;
grant select on table data_engineering.public.sales to role sales_executive;
grant select on table data_engineering.public.sales to role sales_eu;

--grant those roles to our user
grant role sales_executive to user sobottom;
grant role sales_eu to user sobottom;

--create row access policy
use role sysadmin;
CREATE OR REPLACE ROW ACCESS POLICY data_engineering.public.sales_policy
AS (sales_region varchar) RETURNS BOOLEAN ->
  'SALES_EXECUTIVE' = CURRENT_ROLE()
      OR EXISTS (
            SELECT 1 FROM manager_region
              WHERE manager_role = CURRENT_ROLE()
                AND region = sales_region
          )
;

--attach policy to the table
alter table data_engineering.public.sales add row access policy data_engineering.public.sales_policy on (region);

--test the policy out - let's activate the sales_executive role first
use role sales_executive;

select current_role();
select * from sales;

use role sales_eu;
select * from sales;

--use any other role
use role sysadmin;
select * from sales;

--even accountadmin
use role accountadmin;
select * from sales;

--access policy can't be dropped or replaced without detach from table
drop ROW ACCESS POLICY data_engineering.public.sales_policy;

--remove it from table first
alter table data_engineering.public.sales drop row access policy data_engineering.public.sales_policy;


------------------------------------ 20 CI/CD ------------------------------

--create change history table
CREATE TABLE IF NOT EXISTS CHANGE_HISTORY
(
    VERSION VARCHAR
   ,DESCRIPTION VARCHAR
   ,SCRIPT VARCHAR
   ,SCRIPT_TYPE VARCHAR
   ,CHECKSUM VARCHAR
   ,EXECUTION_TIME NUMBER
   ,STATUS VARCHAR
   ,INSTALLED_BY VARCHAR
   ,INSTALLED_ON TIMESTAMP_LTZ
);



--let-s check the content of the table
select * from change_history;

--check the objects
select * from users;

show file formats;

show stages;

select * from users_view;

--clean up before retry
drop stage users_stage;
drop table users;
drop file format json;
truncate table change_history;
