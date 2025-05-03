
----------------------------- 09 monitoring the data pipelines ----------------------------------

--switch to accountadmin
use role accountadmin;

--create a notification integration
--Use the ARN of the created SNS topic for AWS_SNS_TOPIC_PARAMETER
--Use the ARN of our IAM role to which we have assigned the policy with that SNS topic
CREATE NOTIFICATION INTEGRATION my_error_notification
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  DIRECTION = OUTBOUND
  AWS_SNS_TOPIC_ARN = '<your sns topic arn>'
  AWS_SNS_ROLE_ARN = '<your role arn>';

  --you can check that this new error integration is available in the list with all of the integrations
show integrations;

--describe the integration to get needed details for granting Snowflake access to the SNS topic
desc notification integration my_error_notification;

--note SF_AWS_IAM_USER_ARN and SF_AWS_EXTERNAL_ID

--add the error notification to the task
ALTER TASK t_gender_cnt SET ERROR_INTEGRATION = my_error_notification;

--using the metadata
show tasks;

select * from table(result_scan(last_query_id())) where "name" = 'T_GENDER_CNT';

--use accountadmin role for that
use role accountadmin;

--create an email notificaton
CREATE NOTIFICATION INTEGRATION my_email_int
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('<your email>');

--we need to grant the integration to sysadmin;
GRANT USAGE ON INTEGRATION my_email_int TO ROLE sysadmin;

--check all the integrations
show integrations;

--switch back to sysadmin
use role sysadmin;

--create a procedure for monitoring the task state
create or replace procedure task_state_monitor(task_name string)
returns varchar not null
language SQL
AS
$$
DECLARE
    task_state string;
    c CURSOR FOR SELECT "state" from table(result_scan(last_query_id())) where "name" = ?;
BEGIN
    show tasks;
    open c USING (task_name);
    fetch c into task_state;
    IF(task_state = 'suspended') THEN
        CALL SYSTEM$SEND_EMAIL(
            'my_email_int',
            '<your email>',
            'Email alert: Task is suspended!',
            'Please check the task state.'
        );
        RETURN 'Email has been sent.';
     ELSE
         RETURN 'Task state is ok.';
     END IF;


END;
$$
;

--call the procedure to check it
call task_state_monitor('T_GENDER_CNT');

----------------------------- 10 stored procedures ----------------------------------
use role sysadmin;
show streams;

SELECT datediff('day', current_timestamp(), "stale_after") stale_in_days from table(result_scan(last_query_id())) where "name" = 'STR_LANDING';


--create a procedure for monitoring the task state
create or replace procedure stream_stale_monitor(stream_name string, staleness_limit number)
returns varchar not null
language SQL
AS
$$
DECLARE
    stale_in_days string;
    c CURSOR FOR SELECT datediff('day', current_timestamp(), "stale_after") stale_difff from         table(result_scan(last_query_id())) where "name" = ?;
    error_msg string;
    return_msg string;
BEGIN
    show streams;
    open c USING (stream_name);
    fetch c into stale_in_days;
    IF(stale_in_days <= staleness_limit) THEN
        error_msg := 'Please check stream. It might became stale in ' || stale_in_days || ' and                        your defined limit is: ' || staleness_limit;
        CALL SYSTEM$SEND_EMAIL(
            'my_email_int',
            '<your email>',
            'Email alert: Stream stale warning',
            :error_msg
        );
        RETURN 'Email has been sent. Stream will soon become stale.';
     ELSE
         return_msg := 'Stream stale state is ok. It will become stale in' || stale_in_days ||                          ' days and limit is: ' || staleness_limit;
         RETURN return_msg;
     END IF;


END;
$$
;

call stream_stale_monitor('STR_LANDING', 15);

----------------------------- 11 UDF -----------------------------------------------
use role sysadmin;

--extract the domain from email UDF
create or replace function get_email_domain(email string)
returns string
language python
runtime_version = '3.8'
handler = 'domain_extractor'
as
$$
def domain_extractor(email):
    at_index = email.index('@')
    return email[at_index+1:]
$$;

--test the function
select email, get_email_domain(email)
from snowpipe_landing;
--where email is not null;

--extract the domain from email UDF
create or replace function get_email_domain(email string)
returns string
language python
runtime_version = '3.8'
handler = 'domain_extractor'
as
$$
def domain_extractor(email):
    if(email):
        at_index = email.index('@')
        return email[at_index+1:]
    else:
        return 'not valid email'
$$;

--test the function again for null values
select email, get_email_domain(email) from snowpipe_landing ;
--where email is null;

--table function to extract email into 3 separate values: username, domain1, domain2
create or replace function extract_email(email string)
returns table (username string, domain1 string, domain2 string)
language python
runtime_version = '3.8'
handler = 'ExtractEmail'
as
$$
class ExtractEmail:

    def process(self,email):
        if(email):
            at_index = email.index('@')
            domain_dot_index = email.index('.',at_index)

            username = email[:at_index]
            domain1 = email[at_index+1:domain_dot_index]
            domain2 = email[domain_dot_index+1:]
            yield (username, domain1, domain2)
        else:
            yield ('not valid email','not valid email','not valid email')
$$;

--try the Python UDTF out
select email, extract_email.username, extract_email.domain1, extract_email.domain2
from
snowpipe_landing,
table(extract_email(email));

----------------------------- 12 External tables -----------------------------------------------

show stages;

show file formats;

--list the files we want to use for external table definition
list @my_s3_stage/external_tables/;

--create a simple external table without knowing the schema of the files
CREATE OR REPLACE EXTERNAL TABLE my_ext_table
WITH LOCATION = @my_s3_stage/external_tables/
FILE_FORMAT = (FORMAT_NAME='my_csv_format');
PATTERN='.*ext_table*.csv';

--let's try to query the table
select * from my_ext_table;

--it creates a VARIANT column named VALUE - it represent single row in the external file
--columns in csv file represented by c1, c2, c3 ...by default.

--querying the individual columns
select $1:c1, $1:c2, $1:c3, $1:c4, $1:c5, $1:c6 from my_ext_table;

--add a datatype
select
    $1:c1::timestamp_ntz created,
    $1:c2::number id,
    $1:c3::varchar first_name,
    $1:c4::varchar last_name,
    $1:c5::varchar email,
    $1:c6::varchar gender
    from my_ext_table;

--creating external table with column definition manually
create or replace external table my_ext_table2 (
    created timestamp_ntz as (value:c1::timestamp_ntz),
    id number as (value:c2::number),
    first_name varchar as (value:c3::varchar),
    last_name varchar as (value:c4::varchar),
    email varchar as (value:c5::varchar),
    gender varchar as (value:c6::varchar),
    city varchar as (value:c7::varchar)
)
location=@my_s3_stage/external_tables/
file_format = (format_name = 'my_csv_format')
;

--query the table
select * from my_ext_table2;

--query the table together with metadata pseudocolumns
select
METADATA$FILENAME,
METADATA$FILE_ROW_NUMBER,
* from my_ext_table2;

--create partitioned external table
/*
A partition column must evaluate as an expression that parses the path and/or filename information in the METADATA$FILENAME pseudocolumn.

*/

--let's create partitions based on file creation yearmonth
select distinct to_date(split_part(METADATA$FILENAME,'/',3), 'MM_YYYY') from @my_s3_stage/external_tables;

--let's create partitioned external table
create or replace external table my_ext_table3 (
    load_yearmonth date as to_date(split_part(METADATA$FILENAME,'/',3), 'MM_YYYY'),
    created timestamp_ntz as (value:c1::timestamp_ntz),
    id number as (value:c2::number),
    first_name varchar as (value:c3::varchar),
    last_name varchar as (value:c4::varchar),
    email varchar as (value:c5::varchar),
    gender varchar as (value:c6::varchar),
    city varchar as (value:c7::varchar)

)
partition by (load_yearmonth)
location=@my_s3_stage/external_tables/
file_format = (format_name = 'my_csv_format')
;

--select data from table
select * from my_ext_table3;

select distinct load_yearmonth from my_ext_table3;

--let-s upload the latest file from April to simulate arrival of the new data

select distinct load_yearmonth from my_ext_table3;

--refresh the external table to fetch the latest data
alter external table my_ext_table3 refresh;

--check the table again
select distinct load_yearmonth from my_ext_table3;

----------------------------- 13 Data Unloading -----------------------------------------------


--check the table stage
list @%gpt_tweets;

--let's prepare user activity overview - we want to have only users with at least two tweets,
--together with:
--total number of sent tweets
--total replies received
--total likes received
--ordered from the most active users
select username, count(id) tweets_count, sum(replycount) reply_sum, sum(likecount) like_sum
from gpt_tweets
group by username
having count(id) > 1
order by 2 desc;

--simple copy unloading command utilizing our csv and parquet file formats
COPY INTO @%gpt_tweets/csv/ FROM
(
    select
        username, count(id) tweets_count, sum(replycount) reply_sum, sum(likecount) like_sum
    from gpt_tweets
    group by username
    having count(id) > 1
    order by 2 desc

)
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

--check out the stage again
list @%gpt_tweets;

--offloading into JSON in case we need to share data via some API
select object_construct(
 'id', id,
 'tweetDate', date,
 'username', username,
 'tweet', tweet,
 'tweetStats', object_construct(
     'replyCount', replycount,
     'retweetCount', retweetcount,
     'likeCount', likecount,
     'quoteCount', quoteCount),
  'sentiment', object_construct(
      'label', sentiment_label,
      'score', sentiment_score
  )) from gpt_tweets;


--offloading into external stage into json - either VARIANT column or constructing the json on the fly
COPY into @my_s3_stage/json/ from
(
    select object_construct(
 'id', id,
 'tweetDate', date,
 'username', username,
 'tweet', tweet,
 'tweetStats', object_construct(
     'replyCount', replycount,
     'retweetCount', retweetcount,
     'likeCount', likecount,
     'quoteCount', quoteCount),
  'sentiment', object_construct(
      'label', sentiment_label,
      'score', sentiment_score
  )) from gpt_tweets
)
FILE_FORMAT = (TYPE = JSON);

--check it out
list @my_s3_stage/json;

--simple array
select object_construct(
    'userName', username,
    'tweets', array_agg(tweet) over (partition by username)

    )
    from gpt_tweets;

--tasks for you
/*
1.
 -  unload the same data into table stage but with parquet format
 -  use parquet subdirectory in the table stage
 -  use custom file name tweet_activity_summary




2. Please create the JSON file with following structure
{
    username: "abc",
    tweetSummary: [
     {
        tweet: "Random tweet text 1",
        tweetStats: {
            likeCount: 4,
            replyCount: 2,
            retweetCount: 3,
            quoteCount: 0

        }

    ]
    }
}
*/













--solution
COPY INTO @%gpt_tweets/parquet/tweet_activity_summary FROM
(
    select
        username, count(id) tweets_count, sum(replycount) reply_sum, sum(likecount) like_sum
    from gpt_tweets
    group by username
    having count(id) > 1
    order by 2 desc

)
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_format');

--solution for the complex array on top of the object
with tweetSummary as (
 select object_construct(
     'tweet', tweet,
      'tweetStats', object_construct(
         'replyCount', replycount,
         'retweetCount', retweetcount,
         'likeCount', likecount,
         'quoteCount', quoteCount
        )
      ) tweet_summary,
      username
from gpt_tweets )

select
    object_construct(
    'username', username,
    'tweetsSummary', array_agg(tweet_summary) over (partition by username)
    )
from tweetsummary;

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
