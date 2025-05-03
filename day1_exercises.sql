/* ---------------------------------------O3 Data ingestion workflow -----------------------*/

--create basics objects
use role sysadmin;

CREATE OR REPLACE database DATA_ENGINEERING;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

--create a storage integration
use role accountadmin;
CREATE OR REPLACE STORAGE INTEGRATION my_s3_access
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<your aws role arn>'
  STORAGE_ALLOWED_LOCATIONS = ('<your bucket and path>');


--grant usage on integration to sysadmin role
  GRANT USAGE ON INTEGRATION my_s3_access TO ROLE sysadmin;

  --Retrieve the AWS IAM User for your Snowflake Account
  desc integration my_s3_access;
  --use the output to Grant Permissions to Access Bucket Objects to IAM User in AWS

  use role sysadmin;

  /* Now we can continue with our FILE FORMAT creation. We are going to import csv file */

  create or replace file format my_csv_format
  type = csv
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

  --Create an external stage
  CREATE OR REPLACE STAGE my_s3_stage
  STORAGE_INTEGRATION = my_s3_access
  URL = '<your bucket and path>'
  FILE_FORMAT = my_csv_format;

show stages;

--let's create a landing table for our source files
CREATE OR REPLACE TABLE GPT_TWEETS (
ID varchar,
Date varchar,
Username varchar,
Tweet varchar,
ReplyCount number,
RetweetCount number,
LikeCount number,
QuoteCount number,
OnlyDate varchar,
OnlyHour varchar,
OnlyMin varchar,
processed_tweet varchar,
sentiment_label varchar,
sentiment_score varchar);


--simple copy command to load data from stage
COPY INTO GPT_TWEETS from @my_s3_stage/chatgpt-tweets-data.csv
ON_ERROR = CONTINUE;

select * from gpt_tweets;

truncate table gpt_tweets;

/* copy command transformation */

--new target table
CREATE OR REPLACE TABLE GPT_TWEETS_PROCCESSED (
ID varchar,
Date timestamp_ntz,
Username varchar,
processed_tweet varchar
);


--COPY command with transformations during the load
COPY INTO GPT_TWEETS_PROCCESSED (id, date, username, processed_tweet) from
( select s.$1, to_timestamp(s.$2), s.$3, s.$12 from @my_s3_stage/chatgpt-tweets-data.csv s)
ON_ERROR = CONTINUE;

select * from GPT_TWEETS_PROCCESSED;

/* query the load metadata */

--load metadata taken directly from staged file - could be loaded along with data
select METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_CONTENT_KEY, METADATA$FILE_LAST_MODIFIED, METADATA$START_SCAN_TIME, s.$1, s.$2, s.$3 from @my_s3_stage/chatgpt-tweets-data.csv s;

--get the load metadata for given table from INFORMATION_SCHEMA
select * from table(information_schema.copy_history(TABLE_NAME => 'GPT_TWEETS_PROCCESSED',
START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));

--this one with errors during ingestion
select * from table(information_schema.copy_history(TABLE_NAME => 'GPT_TWEETS',
START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));

/* --------------------------------------- 04 semi structured data processing -------------- */

--create a landing table for parquet data
CREATE OR REPLACE TABLE GPT_TWEETS_PARQUET (
ID varchar,
Date varchar,
Username varchar,
Tweet varchar,
ReplyCount number,
RetweetCount number,
LikeCount number,
QuoteCount number,
OnlyDate varchar,
OnlyHour varchar,
OnlyMin varchar,
processed_tweet varchar,
sentiment_label varchar,
sentiment_score varchar);

--let's check what we have in stage
ls @MY_S3_STAGE/parquet;

--we need to have parqeut file format for infer_schema function
CREATE OR REPLACE FILE FORMAT my_parquet_format
  TYPE = parquet;

--INFER_SCHEMA - let's find out the file schema
-- Query the INFER_SCHEMA function.
SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@my_s3_stage'
      , FILE_FORMAT=>'my_parquet_format'
      )
    );


--firstly let's write the copy command manually
COPY into gpt_tweets_parquet (ID,
    Date,
    Username,
    Tweet,
    ReplyCount,
    RetweetCount,
    LikeCount,
    QuoteCount,
    OnlyDate,
    OnlyHour,
    OnlyMin,
    processed_tweet,
    sentiment_label,
    sentiment_score)

from (
    select
        $1:ID::TEXT,
        $1:DATE::TEXT,
        $1:USERNAME::TEXT,
        $1:TWEET::TEXT,
        $1:REPLYCOUNT::NUMBER(38, 0),
        $1:RETWEETCOUNT::NUMBER(38, 0),
        $1:LIKECOUNT::NUMBER(38, 0),
        $1:QUOTECOUNT::NUMBER(38, 0),
        $1:ONLYDATE::TEXT,
        $1:ONLYHOUR::TEXT,
        $1:ONLYMIN::TEXT,
        $1:PROCESSED_TWEET::TEXT,
        $1:SENTIMENT_LABEL::TEXT,
        $1:SENTIMENT_SCORE::TEXT
    from @my_s3_stage/parquet/
)
file_format = 'my_parquet_format';

--let's check the table
select * from gpt_tweets_parquet limit 1000;

--PART 2: Automate the whole ingestion with INFER_SCHEMA, GENERATE_COLUMN_DESCRIPTION and CREATE TABLE LIKE TEMPLATE

--first we need to have a target table, let's generate the column description with GENERATE_COLUMN_DESCRIPTION

SELECT GENERATE_COLUMN_DESCRIPTION(ARRAY_AGG(OBJECT_CONSTRUCT(*)), 'table') AS COLUMNS
  FROM TABLE (
    INFER_SCHEMA(
      LOCATION=>'@my_s3_stage',
      FILE_FORMAT=>'my_parquet_format'
    )
  );




 --Create landing table automatically based on the retrieve schema
CREATE OR REPLACE TABLE gpt_tweets_parquet_template
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@my_s3_stage',
          FILE_FORMAT=>'my_parquet_format'
        )
      ));


--check the created landed table
desc table gpt_tweets_parquet_template;

 --let's use INFER_SCHEMA function and copy the output from expression column into our COPY statement
 SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@my_s3_stage'
      , FILE_FORMAT=>'my_parquet_format'
      )
    );

--ingesting data by using the output from INFER_SCHEMA
COPY into gpt_tweets_parquet_template (ID,
    Date,
    Username,
    Tweet,
    ReplyCount,
    RetweetCount,
    LikeCount,
    QuoteCount,
    OnlyDate,
    OnlyHour,
    OnlyMin,
    processed_tweet,
    sentiment_label,
    sentiment_score)

from (
    select
        $1:ID::TEXT,
        $1:DATE::TEXT,
        $1:USERNAME::TEXT,
        $1:TWEET::TEXT,
        $1:REPLYCOUNT::NUMBER(38, 0),
        $1:RETWEETCOUNT::NUMBER(38, 0),
        $1:LIKECOUNT::NUMBER(38, 0),
        $1:QUOTECOUNT::NUMBER(38, 0),
        $1:ONLYDATE::TEXT,
        $1:ONLYHOUR::TEXT,
        $1:ONLYMIN::TEXT,
        $1:PROCESSED_TWEET::TEXT,
        $1:SENTIMENT_LABEL::TEXT,
        $1:SENTIMENT_SCORE::TEXT
    from @my_s3_stage/parquet/
)
file_format = 'my_parquet_format';

/* --------------------------------------- 05 installation of SnowSQL --------------------------------------- */

/*
--install via homebrew

brew install --cask snowflake-snowsql

it will be installed here: /Applications/SnowSQL.app/Contents/MacOS/snowsql

and should be appended to PATH variable

--check the path
echo $PATH

--adding to PATH:
export PATH=$PATH:/Applications/SnowSQL.app/Contents/MacOS/

Configuration
There should be a configuration file under user home directory
~/.snowsql/

let's open the config file and configure the connection to our Snowflake account and check what are other
options which might be configured

Mention also default connection - this will be used by default always
Mentioned the named connection for multiple profiles per environment/customers ,etc.

open the documentation with list of all other options:
https://docs.snowflake.com/en/user-guide/snowsql-config#configuration-options-section

Connecting to Snowflake

by specifying the parameters
snowsql -a <your account> -u <your user> -d <your db> -s <your schema> -w <your warehouse> -r <your role>

--disconnect: !quit

by name connection
snowflake -c dev

Basic commands
SELECT * FROM GTP_TWEETS limit 10 - not readable

lets select only some columns
SELECT ID, DATE, USERNAME, TWEET from GPT_TWEETS limit 10;

we can list our stages;
show stages;

or warehouses
show warehouses;

let-s try to upload some file from local computer to internal stages. We can try to put the parquet file under table stage of our GPT_TWEETS_PARQUET table. And Let's put it under src directory.

put file:///file_path/gpt_tweets.parquet_0_0_1.snappy.parquet @%gpt_tweets_parquet/src/;

let's check the content of the table stag
list @%gpt_tweets_parquet;

We can of course combine parquet and csv files under one stage. Let's try to upload the csv file now and place it under csv subdirectory.

list @%gpt_tweets_parquet;

Final task, let's try opposite direction and download the data from table into the local file via GET command:
we can offload some data into internal stage via copy command first

COPY into @%gpt_tweets from
(SELECT * FROM GPT_TWEETS LIMIT 100)
FILE_FORMAT = 'my_csv_format';

list @%gpt_tweets;

download the data
get @%gpt_tweets file:///local_path/;

--cleaning
remove files from internal stages
remove @%gpt_tweets;

--check it is empty
list @%gpt_tweets;

--remove from other table stage
remove @%gpt_tweets_parquet;

--and check it
list @%gpt_tweets_parquet;

*/

/* --------------------------------------- 06 views - not sharing the commands -------------- */

select onlydate, count(id) number_of_tweets, sum(replycount) reply_count,
sum(retweetcount) retweet_count, sum(likecount) like_count
from gpt_tweets
group by onlydate
;

--view creation
create or replace view tweets_summary as
    select onlydate,
           count(id) number_of_tweets,
           sum(replycount) reply_count,
           sum(retweetcount) retweet_count,
           sum(likecount) like_count
    from gpt_tweets
    group by onlydate
;

--query the view and check the query profile
select * from tweets_summary;

--try to get a ddl for a view
select get_ddl('view', 'tweets_summary');

--create a secure view
create or replace secure view tweets_summary_secure as
    select onlydate,
           count(id) number_of_tweets,
           sum(replycount) reply_count,
           sum(retweetcount) retweet_count,
           sum(likecount) like_count
    from gpt_tweets
    group by onlydate
;

--query the secure view and check the query profile and notice the differences. There won't be visible what kind
--of operations have been performed by query optimizer
select * from tweets_summary_secure;

--try to get a ddl for secure_view and list of views as an owner
select get_ddl('view', 'tweets_summary_secure');
show views;

--let's create an analyst role with usage privelege on DB and schema. Such role should have a possibility to
--query view and secure view but the ddl of the secure view should be hidden (not an owner), same like secure
--definiton won't be visible in show views in text column
use role securityadmin;

create or replace role ANALYST;

grant usage on database data_engineering to role analyst;
grant usage on schema data_engineering.public to role analyst;
grant select on view data_engineering.public.tweets_summary to role analyst;
grant select on view data_engineering.public.tweets_summary_secure to role analyst;
grant role analyst to role sysadmin;

use role analyst;

--try to get DDL for secure view again
select get_ddl('view', 'tweets_summary_secure');
--check the list of view - secure view will be missing the definition
show views;

--last exercise, let's create a same materialized view

--switch back to sysadmin
use role sysadmin;

--create a secure view
create or replace materialized view mv_tweets_summary as
    select onlydate,
           count(id) number_of_tweets,
           sum(replycount) reply_count,
           sum(retweetcount) retweet_count,
           sum(likecount) like_count
    from gpt_tweets
    group by onlydate
;

--let's try to check if the view is visible in show commands - you can see is_materialized column is true
show views;

--let's query the information_schema views for TABLES and VIEWS
--VIEWS = no info about MVs
select * from information_schema.VIEWS where table_schema = 'PUBLIC';
--TABLES = there is info about MVs - you can check what parameters are available there
select * from information_schema.TABLES where table_schema = 'PUBLIC';

--to check the cost related MV refresh you can use following table function. The output will be empty now.
SELECT * FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY());

--there is possiblity to suspend the MV refresh for some time if needed. It can be done by following command
ALTER MATERIALIZED VIEW mv_tweets_summary SUSPEND;

--if you then try to query the view, you will get the following error message
select * from mv_tweets_summary;

--so we need to resume it first
ALTER MATERIALIZED VIEW mv_tweets_summary resume;

--let's try to create another MV and use some unsupported operation like HAVING
create or replace materialized view mv_tweets_summary as
    select onlydate,
           count(id) number_of_tweets,
           sum(replycount) reply_count,
           sum(retweetcount) retweet_count,
           sum(likecount) like_count
    from gpt_tweets
    group by onlydate
    having number_of_tweets > 1000
;

/* --------------------------------------- 07 continous data pipelines ---------------------- */

--describe the storage integration
  desc integration my_s3_access;

  --we will reuse our already defined stage my_s3_stage

--we also need a target table where we will load the data, let's create one:
create or replace table snowpipe_landing (
id varchar,
first_name varchar,
last_name varchar,
email varchar,
gender varchar,
city varchar);

--create a snowpipe
create or replace pipe mypipe
    auto_ingest=true as
        copy into data_engineering.public.snowpipe_landing
        from @data_engineering.public.my_s3_stage/snowpipe/
        file_format = (type = 'CSV'
                      SKIP_HEADER = 1
                      FIELD_OPTIONALLY_ENCLOSED_BY='"');

--check the pipe to find out the notification channel for setting up the notifications
show pipes;

--notification-channel:

--pipe status
select system$pipe_status('mypipe');

--check the table
select * from snowpipe_landing;

--we can try to check the cost associated with snowpipe run - most probably you won't see anything there now
select * from table(information_schema.pipe_usage_history());

select * from snowflake.account_usage.pipe_usage_history;

/* --------------------------------------- 08 streams and task ------------------------------ */

--create a stream on top of table being loaded by snowpipe
create or replace stream str_landing on table snowpipe_landing;

--check the stream;
show streams;

--check if stream has data
select SYSTEM$STREAM_HAS_DATA('str_landing');

--upload the second file into external stage

--check the landing table count - it should have 2000 rows after another snowpipe load
select count(*) from snowpipe_landing;

--check again if stream has data
select SYSTEM$STREAM_HAS_DATA('str_landing');


--try to query a stream
select * from str_landing;

--check what kind of actions stream contains
select distinct metadata$action, metadata$isupdate from str_landing;


------------------------------------------- TASK ----------------------------------------------

--create a target table
create or replace table gender_summary (
load_timestamp timestamp_ntz,
gender varchar,
count number

);



--first we need to grant executing task to sysadmin role
use role accountadmin;
grant execute task on account to role sysadmin;
use role sysadmin;

--create task
create or replace task t_gender_cnt
warehouse = COMPUTE_WH
schedule = '1 minute'
comment = 'Calculates counts for each gender per load'
when SYSTEM$STREAM_HAS_DATA('str_landing')
AS
insert into gender_summary
select current_timestamp()::timestamp_ntz, gender, count(*)
from str_landing
group by current_timestamp(), gender;

--check the task definition
show tasks;

--check the target table - it should be empty
select * from gender_summary;

--resume task
alter task t_gender_cnt resume;

--check the target table again
select * from gender_summary;

--check the stream - it should not have any data now
select SYSTEM$STREAM_HAS_DATA('str_landing');

--check the task history
select *
  from table(information_schema.task_history())
  order by scheduled_time desc;

--let's upload the last file into the stage

--check the target table again
select * from gender_summary;

alter task t_gender_cnt suspend;
