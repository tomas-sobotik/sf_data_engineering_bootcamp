/* --------------------------------------- 04 semi structured data processing --------------------------------------- */

--create a landing table for parquet data
CREATE TABLE GPT_TWEETS_PARQUET (
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

--let's check what we have in stage - you might have uploaded the parquet files into different subdirectory
ls @MY_S3_STAGE/parquet;

--we need to have parqeut file format for infer_schema function
CREATE FILE FORMAT my_parquet_format
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
CREATE TABLE gpt_tweets_parquet_template
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
