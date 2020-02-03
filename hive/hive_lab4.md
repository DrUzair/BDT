# Lab 4: Programming Hive 

1. In this lab session, we will start working with HiveQL
2. In case you don't have the geo-tagged tweet data in Hadoop, you need to reload it 
3. To avoid confusion, please always include database name 'twitter.' as part of your hive table name. e.g.,  twitter.full_text
   - If you don't specify the database name while you're not in the twitter database (did not issue the command 'use twitter'), you will not find the  the corresponding table. By default you're in a database called "default" 
   - **ATTENTION:** This set of lab instructions contain a **'bug'** that we intentionally introduced.
   - Please follow and read the ERROR messages closely and understand what they are referring to.
   - When you find the 'bug' figure out a way to fix it. You are strongly encouraged to search on the internet and/or discuss the problem with your class mates.
   - After you have fixed the 'bug' (or failed to come up with a fix), notify your instructor. 

[HIVE Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)

## Topics <a name="top"></a> 

- [Hive CLI](#cli)
- [Working with Hive Database files](#db)
- List, Connect, Location, Creation, Drop
- [Working with Hive Tables](#tbl)
   - [List](#tbl_list), [Location](#tbl_loc), [Creation](#tbl_create), [Drop](#tbl_drop), [Select data](#select)
- [Hive Functions](#funcs)
   - [Time and Dates](#dt_funcs) 
   - [Stings and Regex](#str_funcs)
   - [Conditionals; Case-When-Then](#cnds)
   - [WHERE Clause](#where)
   - [GROUP By Clause](#groupby)
- [ORDER By Clause](#oderby)
  - [DISTINCT](#distinct)
  - [JOIN Tables](#join)


## Hive CLI <a name="cli"></a> 

- All commands should end with semi-colon ;

### Invoke

If installed and configured properly, hive command will launch the hive CLI

```shell
[root@sandbox lab]# hive
hive
>
> Logging initialized using configuration in file:/etc/hive/2.5.0.0-1245/0/hive-log4j.properties
hive >
```

### Quit

Ctrl-c or

```shell
hive> quit;
```

### List Databases <a name="db"></a> 

```shell
hive> show databases;
OK
default
demo
foodmart
xademo
Time taken: 0.046 seconds, Fetched: 5 row(s)
```

## Create a Database file

```shell
CREATE database twitter;
```

### Connect to a Database

By default, default database is in use. 

```shell
hive> use twitter;
```

All queries will run on the databse in use.

**Note** to make currently in use databse to appear on console; 

```shell
hive> set hive.cli.print.current.db=true;
```

or hive-site.xml

```xml
<property>
  <name>hive.cli.print.current.db</name>
  <value>true</value>
</property>
```

### Drop a Database file

```sql
hive> DROP database IF EXISTS demo;
```

- Dropping a database that doesn't exit will result

```shell
hive> drop database xyx;
FAILED: SemanticException [Error 10072]: Database does not exist: xyx
```

- Drop a database that contains tables 

```shell
hive> drop database twitter;
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. InvalidOperationException(message:Database twitter is not empty. One or more tables exist.)
```

- Use responsibly

```shell
hive> DROP database IF EXISTS demo CASCADE;
```

[Top](#top)

### Working with Hive Tables <a name="tbl"></a> 

### List Tables in currently in use Database <a name="tbl_list"></a> 

```
hive (twitter)> show tables;
OK
day_of_week
full_text
full_text_2
weekend_tweets
Time taken: 0.509 seconds, Fetched: 4 row(s)
```

[Top](#top)

### Location of hive Tables <a name='tbl_loc'></a>

'dfs -ls' command in hive CLI lists contents of HDFS directory. For twitter database, a directory "twitter.db" should be listed because **hive databases** are just **HDFS directories** and each **hive table** is an **HDFS file**

```shell
hive (default)> dfs -ls /apps/hive/warehouse;
Found 11 items
drwxrwxrwx   - root hdfs          0 2019-12-19 14:01 /apps/hive/warehouse/demo.db
drwxrwxrwx   - hive hdfs          0 2019-10-16 01:50 /apps/hive/warehouse/drivers
drwxrwxrwx   - hive hdfs          0 2019-10-16 02:26 /apps/hive/warehouse/drivers2
drwxrwxrwx   - hive hdfs          0 2016-10-25 08:10 /apps/hive/warehouse/foodmart.db
drwxrwxrwx   - hive hdfs          0 2016-10-25 08:11 /apps/hive/warehouse/sample_07
drwxrwxrwx   - hive hdfs          0 2016-10-25 08:11 /apps/hive/warehouse/sample_08
drwxrwxrwx   - hive hdfs          0 2019-10-16 01:09 /apps/hive/warehouse/temp_drivers
drwxrwxrwx   - hive hdfs          0 2019-10-16 01:37 /apps/hive/warehouse/temp_timesheet
drwxrwxrwx   - hive hdfs          0 2019-10-16 01:46 /apps/hive/warehouse/timesheet
drwxrwxrwx   - root hdfs          0 2019-12-30 05:21 /apps/hive/warehouse/twitter.db
drwxrwxrwx   - hive hdfs          0 2016-10-25 08:02 /apps/hive/warehouse/xademo.db
```

and further

```shell
hive (twitter)> dfs -ls /apps/hive/warehouse/twitter.db;
Found 5 items
drwxrwxrwx   - root hdfs          0 2019-12-21 04:08 /apps/hive/warehouse/twitter.db/day_of_week
drwxrwxrwx   - root hdfs          0 2019-12-21 03:46 /apps/hive/warehouse/twitter.db/full_text
drwxrwxrwx   - root hdfs          0 2019-12-21 03:57 /apps/hive/warehouse/twitter.db/full_text_2
drwxrwxrwx   - root hdfs          0 2019-12-30 05:21 /apps/hive/warehouse/twitter.db/test
drwxrwxrwx   - root hdfs          0 2019-12-21 04:13 /apps/hive/warehouse/twitter.db/weekend_tweets
```

[Top](#top)

## Moving file to hdfs

```shell
hadoop fs -put /path/to/twitter_data/full_text.txt /path/to/hdfs/dir
```

## hive Table Creation <a name='tbl_create'></a>

```shell
create table twitter.test (                                                   
          id string, 
          ts string, 
          lat_lon string,
          lat string, 
          lon string, 
          tweet string)
row format delimited 
fields terminated by '\t' ; 
```

## Creating Table from Existing 

```sql
create table twitter.full_text_2 as 
select * from twitter.full_text;
```

### Table Schema Description

```shell
hive (twitter)> describe extended test;
OK
id                      string
ts                      string
lat_lon                 string
lat                     string
lon                     string
tweet                   string

Detailed Table Information      Table(tableName:test, dbName:twitter, owner:root, createTime:1577683317, lastAccessTime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:id, type:string, comment:null), FieldSchema(name:ts, type:string, comment:null), FieldSchema(name:lat_lon, type:string, comment:null), FieldSchema(name:lat, type:string, comment:null), FieldSchema(name:lon, type:string, comment:null), FieldSchema(name:tweet, type:string, comment:null)], location:hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/twitter.db/test, inputFormat:org.apache.hadoop.mapred.TextInputFormat, outputFormat:org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat, compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, parameters:{serialization.format=     , field.delim=
Time taken: 0.795 seconds, Fetched: 8 row(s)
```

### Loading hdfs file in hive Table <a name='tbl_load'></a>

```shell
load data inpath '/user/lab/full_text.txt'  
overwrite into table twitter.full_text;
```

### Loading hdfs file as External hive Table 

```sql
drop table twitter.full_text;
create external table twitter.full_text (                                                 
          id string, 
          ts string, 
          lat_lon string,
          lat string, 
          lon string, 
          tweet string)
row format delimited 
fields terminated by '\t'
location '/user/lab/full_text';   
```

[Top](#top)

### Dropping a Table <a name='tbl_drop'></a>

```sql
drop table twitter.full_text_ts;
```

[Top](#top)

### SELECT Clause <a name="select"></a> 

```sql
hive (twitter)> select id, ts from twitter.full_text limit 2;
OK
USER_79321756   2010-03-03T04:15:26     ÜT: 47.528139,-122.197916       47.528139       -122.197916     RT @USER_2ff4faca: IF SHE DO IT 1 MORE TIME......IMA KNOCK HER DAMN KOOFIE OFF.....ON MY MOMMA&gt;&gt;haha. #cutthatout NULL
USER_79321756   2010-03-03T04:55:32     ÜT: 47.528139,-122.197916       47.528139       -122.197916     @USER_77a4822d @USER_2ff4faca okay:) lol. Saying ok to both of yall about to different things!:*        NULL
Time taken: 0.254 seconds, Fetched: 5 row(s)
```

[Top](#top)

# Hive Functions <a name='funcs'></a>

### Time and Date <a name="dt_funcs"></a>

Example: Convert string to timestamp

- cast() function; convert datatype string to timestamp 
- concat() function; concatanate multiple strings into one
- substr() function; extract some portion of a string

```sql
hive (twitter)> create table twitter.full_text_ts as select id, cast(concat(substr(ts, 1, 10), ' ', substr(ts, 12, 18)) as timestamp) as ts, lat, lon, tweet from twitter.full_text;
```

- output on console as following
- took **112 seconds**. That is why always a good idea to save the results in a seperate table for queries that need repeat execution.

```shell
Query ID = root_20191230055154_4654cec8-a215-4181-b1b3-dd00cd67df31
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Session re-established.


Status: Running (Executing on YARN cluster with App id application_1576992085977_0004)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      4          4        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 112.08 s
--------------------------------------------------------------------------------
Moving data to directory hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/twitter.db/full_text_ts
Table twitter.full_text_ts stats: [numFiles=4, numRows=377616, totalSize=61667310, rawDataSize=61289694]
OK
Time taken: 139.769 seconds
hive (twitter)> describe full_text_ts;
OK
id                      string
ts                      timestamp
lat                     string
lon                     string
tweet                   string
Time taken: 0.67 seconds, Fetched: 5 row(s)
```

Extract year, month and day from timestamp

- unix_timestamp() function; convert datatype string to unix timestamp
- to_date() function; convert datatype string to date
- year() function; extract the year portion of a date
- month() function; extract the month portion of a date
- day() function; extract the day portion of a date

```sql
select ts, unix_timestamp(ts) as unix_timestamp, to_date(ts) as the_date, year(ts) as year, month(ts) as month, day(ts) as day
from twitter.full_text_ts
limit 5;
```

[Top](#top)

### Strings and Regex <a name="str_funcs"></a> 

- Trim spaces from both ends of a string and convert to lowercase

```sql
select id, ts, trim(lower(tweet)) as tweet
from twitter.full_text_ts
limit 5;
```

- Trim spaces from both ends of a string and convert to uppercase

```sql
select id, ts, trim(upper(tweet)) as tweet
from twitter.full_text_ts
limit 5;
```

- length of a string

```sql
select id, ts, length(tweet) as tweet
from twitter.full_text_ts
limit 5;
```

- Tokenize a string into words

```sql
select id, ts, sentences(tweet) as tokens
from twitter.full_text_ts
limit 5;
```

- Regex_extract: twitter handles
  Find twitter handles mentioned in a tweet, NOTE that the regex in this query will only find the first mention. It doesn't work properly when the tweet contains more than one mention. 

```sql
select id, ts, regexp_extract(lower(tweet), '(.*)@user_(\\S{8})([:| ])(.*)',2) as patterns
from twitter.full_text_ts
limit 5;
```

How do we capture all the mentions in a tweet? 

- Regex_replace
  Longest tweets..

```sql
select id, regexp_replace(tweet, "@USER_\\w{8}", "") as trimmed_tweet, 
length(regexp_replace(tweet, "@USER_\\w{8}", " ")) as len 
from twitter.full_text_ts 
limit 5;
```

- Finding top 10 users who tweet long tweets
  - maximum tweet length is 140 characters.. but output of this query shows tweets with length > 140

```sql
select t.id, t.len, t.tweet
from (select id, tweet, length(tweet) as len from twitter.full_text_ts) t 
order by len desc
limit 10;
```

- Removing the mentions will improve the results

```sql
select t.id, t.len, t.trimmed_tweet
from (select id, regexp_replace(tweet, "@USER_\\w{8}", "") as trimmed_tweet, length(regexp_replace(tweet, "@USER_\\w{8}", " ")) as len from twitter.full_text_ts) t 
order by len desc
limit 10;
```

[Top](#top)

### Conditionals: Case-When-Then <a name="cnds"></a> 

- Find users who like to tw-eating

```sql
select * from
    (select id, ts, case when hour(ts) = 7 then 'breakfast'
                        when hour(ts) = 12 then 'lunch'
                        when hour(ts) = 19 then 'dinner'
                   end as tw_eating,
           lat, lon
    from twitter.full_text_ts) t
where t.tw_eating in ('breakfast','lunch','dinner')
limit 10;
```

[Top](#top)

## WHERE Clause - Filtering Data <a name="where"></a> 

- Find all tweets by a user
- Hive is very slow for this type of query because for even one record it still scans through the entire table
- this is because MapReduce works in a streaming fashion
- for fast retrieval, you can either use relational database or different technologies such as Apache Impala or Spark 

```sql
select id, ts, lat, lon, tweet
from twitter.full_text_ts   
where id='USER_ae406f1d'; 
```

- Show a sample, then calculate number of tweets on a specific date

```sql
select *
from twitter.full_text_ts
where to_date(ts) = '2010-03-07'
limit 5; 
```

- Count tweets on a given date...

```sql
select count(*)
from twitter.full_text_ts
where to_date(ts) = '2010-03-07'
```

- Find all tweets tweeted from NYC vicinity (using bounding box -74.2589, 40.4774, -73.7004, 40.9176)
- The square bounding box won't give us very accurate results. We may end up retrieving tweets in New Jersey as well.
- A better approach is to use geo function plugins for Hive. We will re-visit this when we introduce Pig

```sql
select distinct lat, lon 
from twitter.full_text_ts
where lat > 40.4774 and lat < 40.9176 and
      lon > -74.2589 and lon < -73.7004
limit 20;
```

[Top](#top)

## GROUP BY Clause <a name="groupby"></a> 



- Calculate number of tweets per user

```sql
create table twitter.tweets_per_user as
select id, COUNT(*) as cnt
from twitter.full_text_ts
group by id;
```

- **	181 seconds**. 

```shell
Query ID = root_20191230145950_0b2229ec-83b2-46e7-a650-c5df342460d6
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Session re-established.


Status: Running (Executing on YARN cluster with App id application_1576992085977_0010)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      4          4        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 181.43 s
--------------------------------------------------------------------------------
Moving data to directory hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/twitter.db/tweets_per_user
Table twitter.tweets_per_user stats: [numFiles=1, numRows=9475, totalSize=161391, rawDataSize=151916]
```

[Top](#top)

## ORDER BY Clause <a name="oderby"></a> 

```sql
select * from tweets_per_user order by cnt desc limit 10;
```

- console output

```shell
Query ID = root_20191230150713_7735a988-ee88-463c-aad0-9b58bcdda3a7
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1576992085977_0010)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 14.18 s
--------------------------------------------------------------------------------
OK
USER_6b07169e   301
USER_943f9c88   293
USER_f35e4685   259
USER_9506fb5f   256
USER_cd52d26d   255
USER_c913f269   252
USER_7f5ce035   247
USER_2e157dc3   243
USER_c8613ca2   228
USER_e6d61f05   220
Time taken: 17.02 seconds, Fetched: 10 row(s)
```

- Find top 10 tweeters in NYC

```sql
select id, count(*) as cnt
from twitter.full_text_ts
where lat > 40.4774 and lat < 40.9176 and
      lon > -74.2589 and lon < -73.7004
group by id
order by cnt desc
limit 10;
```

[Top](#top)

## DISTINCT <a name="distinct"></a> 

- Find number of distinct days this dataset covers

```sql
select distinct to_date(ts)
```

Console output

```shell
    > from twitter.full_text_ts;
Query ID = root_20191230152033_18f7b2dd-25fe-43c9-8a6e-da2c6ae9864e
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1576992085977_0010)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      4          4        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 44.52 s
--------------------------------------------------------------------------------
OK
2010-03-02
2010-03-03
2010-03-04
2010-03-05
2010-03-06
2010-03-07
Time taken: 46.339 seconds, Fetched: 6 row(s)
```

- Count number of distinct days this dataset covers

```sql
select distinct to_date(ts)
from twitter.full_text_ts;
```

- Find number of distinct days this dataset covers

```sql
select count(distinct to_date(ts))
from twitter.full_text_ts;
```

[Top](#top)

## JOIN Tables <a name="join"></a> 

- prepare lookup table 'dayofweek'

--------------------

| 2010-03-02 | Tuesday   |
| ---------- | --------- |
| 2010-03-03 | Wednesday |
| 2010-03-04 | Thursday  |
| 2010-03-05 | Friday    |
| 2010-03-06 | Saturday  |
| 2010-03-07 | Sunday    |

```sql
create table twitter.dayofweek (t_date string, dayofweek string)
row format delimited
fields terminated by '\t';
```

```shell
load data inpath '/user/lab/dayofweek.txt'
overwrite into table twitter.dayofweek;
```

- Find Weekend Tweets
  - INNER JOIN

```sql
create table twitter.weekend_tweets as
select a.id, a.ts, b.dayofweek, a.lat, a.lon, a.tweet
from twitter.full_text_ts as JOIN twitter.dayofweek as b
     ON to_date(a.ts) = b.date AND b.dayofweek IN ('Saturday','Sunday');
```

[Top](#top)
