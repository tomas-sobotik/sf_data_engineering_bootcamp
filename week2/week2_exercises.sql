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
