# Apache Hive SQL~HQL

[HIVE Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)

1. In this lab session, we will start with the basics of HiveQL
2. To avoid confusion, please always include database name as part of your hive table name. e.g., demo.mytable

- In Hive, the database is considered as a catalog or namespace of tables. 
- So, we can maintain multiple tables within a database where a unique name is assigned to each table. 
- Hive also provides a default database with a name default.

## Topics <a name="top"></a> 
- [Hive Version](#version)
- [Hive CLI](#cli)
- [Working with Hive Databases](#db)
  - [List](#list_db), Connect, Location, Creation, Drop
- [Working with Hive Tables](#tbls)
  - [List](#tbl_lst), [Location](#tbl_loc), [Creation](#tbl_create), [Schema](#tbl_schema), [Drop](#tbl_drop)
- [Hive QL](#hql)
  - [SELECT Clause](#select)
  - [WHERE Clause](#where)
  - [ORDER By Clause](#oderby)
  - [DISTINCT](#distinct)
  - [GROUP By Clause](#groupby)
- Hive Funcations
  - [Date and Time](#dt_funcs)
  - [Strings and Regex](#str_funcs)
  - [Conditionals](#cnds)
  
## Hive Version <a name="version"></a> 
```shell
[root@sandbox ~]# /usr/bin/hive --version
Hive 1.2.1000.2.5.0.0-1245
Subversion git://c66-slave-20176e25-3/grid/0/jenkins/workspace/HDP-parallel-centos6/SOURCES/hive -r da6c690d384d1666f5a5f450be5cbc54e2fe4bd6
Compiled by jenkins on Fri Aug 26 01:39:52 UTC 2016
From source with checksum c30648316a632f7a753f4359e5c8f4d6
```

or
```shell
[root@sandbox ~]# hive --service version
Hive 1.2.1000.2.5.0.0-1245
Subversion git://c66-slave-20176e25-3/grid/0/jenkins/workspace/HDP-parallel-centos6/SOURCES/hive -r da6c690d384d1666f5a5f450be5cbc54e2fe4bd6
Compiled by jenkins on Fri Aug 26 01:39:52 UTC 2016
From source with checksum c30648316a632f7a753f4359e5c8f4d6
```
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
quit;
```

### List Databases <a name="listdb"></a> 

```shell
hive> show databases;
OK
default
demo
foodmart
twitter
xademo
Time taken: 0.046 seconds, Fetched: 5 row(s)
```

### Connect to a Database

By default, default database is in use. If you are connected to another database, start connecting to **default** database for this lab.

```shell
use default;
```

All queries will run on the databse in use.

**Note** to make currently in use databse to appear on console; 

```shell
set hive.cli.print.current.db=true;
```

or hive-site.xml

```xml
<property>
  <name>hive.cli.print.current.db</name>
  <value>true</value>
</property>
```

### Create a Database file

```sql
CREATE database demo;
```

### Drop a Database file

```sql
DROP database IF EXISTS demo;
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

- In Hive, it is not allowed to drop the database that contains the tables directly. 
- In such a case, we can drop the database either BY dropping tables first or use Cascade keyword with the command.
- Use responsibly

```shell
DROP database IF EXISTS demo CASCADE
```

[Top](#top)

# Working with Hive Tables <a name="tbls"></a>

### List Tables in currently in use Database <a name="tbl_lst"></a> 

```
hive (xademo)> show tables;
OK
call_detail_records
customer_details
recharge_details
Time taken: 0.424 seconds, Fetched: 3 row(s)
```

[Top](#top)

### Location of hive Tables <a name="tbl_loc"></a> 

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

## Hive Table Creation <a name="tbl_create"></a>

- [Syntax](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable)

```sql
CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.] table_name

[(col_name data_type [COMMENT col_comment], ...)]
[COMMENT table_comment]
[ROW FORMAT row_format]
[STORED AS file_format]
```

- Example

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

[Top](#top)

### Table Schema Description <a name="tbl_schema"></a>

- Basic

```shell
hive (xademo)> DESCRIBE xademo.customer_details;
OK
phone_number            string
plan                    string
rec_date                string
status                  string
balance                 string
imei                    string
region                  string
Time taken: 7.224 seconds, Fetched: 7 row(s)
```

- Extended schema

```shell
hive (xademo)> DESCRIBE EXTENDED xademo.customer_details;
OK
phone_number            string
plan                    string
rec_date                string
status                  string
balance                 string
imei                    string
region                  string

Detailed Table Information      Table(tableName:customer_details, dbName:xademo, owner:hive, createTime:1477382528, lastAccessTime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:phone_number, type:string, comment:null), FieldSchema(name:plan, type:string, comment:null), FieldSchema(name:rec_date, type:string, comment:null), FieldSchema(name:status, type:string, comment:null), FieldSchema(name:balance, type:string, comment:null), FieldSchema(name:imei, type:string, comment:null), FieldSchema(name:region, type:string, comment:null)], location:hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/xademo.db/customer_details, inputFormat:org.apache.hadoop.mapred.TextInputFormat, outputFormat:org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat, compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, parameters:{serialization.format=|, field.delim=|}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[], parameters:{transient_lastDdlTime=1477382533, totalSize=1532, numRows=0, rawDataSize=0, numFiles=1}, viewOriginalText:null, viewExpandedText:null, tableType:MANAGED_TABLE)
Time taken: 1.295 seconds, Fetched: 9 row(s)

```
- formatted description
```shell
hive> DESCRIBE FORMATTED xademo.customer_details;
OK
# col_name              data_type               comment

phone_number            string
plan                    string
rec_date                string
status                  string
balance                 string
imei                    string
region                  string

# Detailed Table Information
Database:               xademo
Owner:                  hive
CreateTime:             Tue Oct 25 08:02:08 UTC 2016
LastAccessTime:         UNKNOWN
Protect Mode:           None
Retention:              0
Location:               hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/xademo.db/customer_details                               
Table Type:             MANAGED_TABLE
Table Parameters:
        numFiles                1
        numRows                 0
        rawDataSize             0
        totalSize               1532
        transient_lastDdlTime   1477382533

# Storage Information
SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
InputFormat:            org.apache.hadoop.mapred.TextInputFormat
OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputForm                                                                                                                                                             at
Compressed:             No
Num Buckets:            -1
Bucket Columns:         []
Sort Columns:           []
Storage Desc Params:
        field.delim             |
        serialization.format    |
Time taken: 1.109 seconds, Fetched: 37 row(s)

```
[Top](#top)

### Dropping a Table <a name='tbl_drop'></a>
```sql
drop table twitter.full_text_ts;
```

[Top](#top)

## HIVE QL <a name="hql"></a> 

### SELECT Clause <a name="select"></a> 

- display the first 10 rows of the customer_detail table

```sql
SELECT * FROM xademo.customer_details LIMIT 10;
```

outputs ...

```shell
OK
PHONE_NUM       PLAN    REC_DATE        STAUS   BALANCE IMEI    REGION
5553947406      6290    20130328        31      0       012565003040464 R06
7622112093      2316    20120625        21      28      359896046017644 R02
5092111043      6389    20120610        21      293     012974008373781 R06
9392254909      4002    20110611        21      178     357004045763373 R04
7783343634      2276    20121214        31      0       354643051707734 R02
5534292073      6389    20120223        31      83      359896040168211 R06
9227087403      4096    20081010        31      35      356927012514661 R04
9226203167      4060    20060527        21      450     010589003666377 R04
9221154050      4107    20100811        31      3       358665019197977 R04
Time taken: 0.313 seconds, Fetched: 10 row(s)
```

- count the number of rows in the table

```sql
SELECT count(*) 
FROM xademo.customer_details;
```

```shell
Query ID = root_20191230190726_d1438312-f127-4ffe-ab28-f8aef86dc25c
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Session re-established.


Status: Running (Executing on YARN cluster with App id application_1576992085977_0011)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 12.77 s
--------------------------------------------------------------------------------
OK
30
Time taken: 35.485 seconds, Fetched: 1 row(s)
```

[Top](#top)

## Functions 

### Time and Date <a name="dt_funcs"></a>
- Casting a string to date
  - With pattern
  ```shell
  hive > select cast(to_date(from_unixtime(unix_timestamp('01-05-2020', 'dd-MM-yyyy'))) as date);
  ```
  - The default representation of a date is ISO8601, stored as binary.
  ```shell
  hive > select cast('2020-01-05' as date);
  ```
- String to timestamp
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
select id, regexp_replace(tweet, "@USER_\\w{8}", "") as trimmed_tweet, length(regexp_replace(tweet, "@USER_\\w{8}", " ")) as len from twitter.full_text_ts
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

- Find all customers subscribed to a plan 

```sql
hive (xademo)> SELECT plan, phone_number, rec_date, balance
             > FROM xademo.customer_details
             > WHERE plan = '4060';
OK
4060    9226203167      20060527        450
4060    9226907642      20070312        93
4060    9422182637      20041201        117
Time taken: 7.603 seconds, Fetched: 3 row(s)
```

- Find all customers subscribed to plan 2xxx

```sql
hive (xademo)> SELECT plan, phone_number, rec_date, balance
             > FROM xademo.customer_details
             > WHERE plan < 3000 AND plan > 2000;
OK
2316    7622112093      20120625        28
2276    7783343634      20121214        0
2002    7434378689      20100824        0
2285    7482285225      20121130        52
2002    7788070992      20101214        17
2276    7982300380      20121223        0
2389    7582299877      20120610        33
2368    7482229731      20090110        142
2276    7984779801      20121223        14
Time taken: 0.695 seconds, Fetched: 9 row(s)
```

- Count the number of customers on plan 2xxx

```sql
hive (xademo)> SELECT count(*)
             > FROM xademo.customer_details
             > WHERE plan < 3000 AND plan > 2000;
Query ID = root_20200107165053_f4acb4d5-7a2f-4d3e-8d62-70e89c8d7f32
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Session re-established.


Status: Running (Executing on YARN cluster with App id application_1578412825124                                                                                                                                                             _0002)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 44.27 s
--------------------------------------------------------------------------------
OK
9
Time taken: 95.255 seconds, Fetched: 1 row(s)
```

- Alternate method

```sql
hive (xademo)> SELECT count(*)
             > FROM xademo.customer_details
             > WHERE plan LIKE '2%';
Query ID = root_20200107165434_74b31462-b79c-4a53-bcba-e929212eae4f
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1578412825124_0002)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 13.16 s
--------------------------------------------------------------------------------
OK
9
Time taken: 16.838 seconds, Fetched: 1 row(s)
```

[Top](#top)

## ORDER BY Clause <a name="oderby"></a> 

- Find top 10 customers with the highest balance

```sql
hive (xademo)> SELECT *
             > FROM xademo.customer_details
             > ORDER BY balance DESC
             > LIMIT 10;
Query ID = root_20200107165750_c9a52ac8-436e-4467-825a-b287ea086c78
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1578412825124_0002)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 14.18 s
--------------------------------------------------------------------------------
OK
PHONE_NUM       PLAN    REC_DATE        STAUS   BALANCE IMEI    REGION
9882297052      4316    20120107        21      97      357118045463485 R04
9226907642      4060    20070312        21      93              R04
5534292073      6389    20120223        31      83      359896040168211 R06
9227677218      4286    20130121        21      70      354894017753268 R04
9559185951      4276    20120924        31      70      355474044841748 R04
9882259323      4012    20101201        31      68      012239002633949 R04
9752115932      4002    20100526        21      542     358835035011748 R04
7482285225      2285    20121130        31      52      352212033537106 R02
9226203167      4060    20060527        21      450     010589003666377 R04
Time taken: 15.673 seconds, Fetched: 10 row(s)
```

- Find top 10 customers with the lowest balance
  - ASC is the default order

```shell
hive (xademo)> SELECT *
             > FROM xademo.customer_details
             > ORDER BY balance ASC LIMIT 10;
Query ID = root_20200107170007_d2f2f84f-60bd-48f4-8b57-e4358308fee3
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1578412825124_0002)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 20.77 s
--------------------------------------------------------------------------------
OK
5553947406      6290    20130328        31      0       012565003040464 R06
9002245938      4277    20130131        31      0       013111000005512 R04
7434378689      2002    20100824        32      0       355000035507467 R02
7783343634      2276    20121214        31      0       354643051707734 R02
7982300380      2276    20121223        31      0       357210042170690 R02
9790142194      4012    20090406        32      0       011336002603947 R04
5922179682      4282    20130316        21      110     354073042162536 R04
9422182637      4060    20041201        31      117     010440007339548 R04
7984779801      2276    20121223        31      14      013342000049057 R02
7482229731      2368    20090110        21      142     357611009852016 R02
Time taken: 22.916 seconds, Fetched: 10 row(s)
```

[Top](#top)

## DISTINCT <a name="distinct"></a> 

- List the distinct regions 

```sql
hive (xademo)> SELECT DISTINCT region
             > FROM xademo.customer_details;
Query ID = root_20200107170229_222a71fe-009c-441f-a179-1e73441bf844
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1578412825124_0002)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 11.71 s
--------------------------------------------------------------------------------
OK
R02
R04
R06
REGION
Time taken: 13.982 seconds, Fetched: 4 row(s)
```

- Count the number of distinct plans 

```shell
   hive (xademo)> SELECT count(DISTINCT plan)
             > FROM xademo.customer_details;
Query ID = root_20200107170316_24162fd2-de4e-4b51-abba-e9049165f118
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1578412825124_0002)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
Reducer 3 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 22.48 s
--------------------------------------------------------------------------------
OK
21
Time taken: 31.56 seconds, Fetched: 1 row(s)
```

[Top](#top)

## GROUP BY Clause <a name="groupby"></a> 

- The GROUP BY clause is used to group all the records in a result set using a particular collection column. 

- It is used to query a group of records.

  - You can only include the GROUPed columns and aggregations in the SELECT statement

- Calculate number of calls per type using the call_detail_records table

  ```sql
  hive (xademo)> SELECT type, count(*)
               > FROM xademo.call_detail_records
               > GROUP BY type;
  Query ID = root_20200107170558_d21fafdd-c502-42bc-9e62-128c3a3e5b09
  Total jobs = 1
  Launching Job 1 out of 1
  
  
  Status: Running (Executing on YARN cluster with App id application_1578412825124_0002)
  
  --------------------------------------------------------------------------------
          VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
  --------------------------------------------------------------------------------
  Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
  Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
  --------------------------------------------------------------------------------
  VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 22.39 s
  --------------------------------------------------------------------------------
  OK
  INTERNET        38
  SMS     16
  TYPE    1
  VOICE   25
  Time taken: 23.982 seconds, Fetched: 4 row(s)
  ```

- Calculate the number of calls and  total amount per each call type

  ```sql
  hive (xademo)> SELECT type, count(*), sum(amount)
               > FROM xademo.call_detail_records
               > GROUP BY type;
  Query ID = root_20200107170718_6d7bde1d-0543-4a34-a05d-76d84bc238ad
  Total jobs = 1
  Launching Job 1 out of 1
  
  
  Status: Running (Executing on YARN cluster with App id application_1578412825124_0002)
  
  --------------------------------------------------------------------------------
          VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
  --------------------------------------------------------------------------------
  Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
  Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
  --------------------------------------------------------------------------------
  VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 18.66 s
  --------------------------------------------------------------------------------
  OK
  INTERNET        38      27.01
  SMS     16      7.92
  TYPE    1       0.0
  VOICE   25      84.12999999999998
  Time taken: 22.754 seconds, Fetched: 4 row(s)
  ```

- Calculate the number of calls, average amount per each call type

  - Manual calculation

  ```sql
  hive (xademo)> SELECT type, count(*), sum(amount)/count(*)
               > FROM xademo.call_detail_records
               > GROUP BY type;
  Query ID = root_20200107170840_134e6a87-9a09-4475-b9f2-440041f880a6
  Total jobs = 1
  Launching Job 1 out of 1
  
  
  Status: Running (Executing on YARN cluster with App id application_1578412825124_0002)
  
  --------------------------------------------------------------------------------
          VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
  --------------------------------------------------------------------------------
  Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
  Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
  --------------------------------------------------------------------------------
  VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 14.36 s
  --------------------------------------------------------------------------------
  OK
  INTERNET        38      0.7107894736842105
  SMS     16      0.495
  TYPE    1       0.0
  VOICE   25      3.3651999999999993
  Time taken: 19.443 seconds, Fetched: 4 row(s)
  ```

  - Calculate the number of calls, average amount per each call type

    - Using built-in function

  

  ```sql
  hive (xademo)> SELECT type, count(*), avg(amount)
               > FROM xademo.call_detail_records
               > GROUP BY type;
  Query ID = root_20200107171001_5f96161f-ab26-43a3-b49d-fbc4eb340c91
  Total jobs = 1
  Launching Job 1 out of 1
  
  
  Status: Running (Executing on YARN cluster with App id application_1578412825124_0002)
  
  --------------------------------------------------------------------------------
          VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
  --------------------------------------------------------------------------------
  Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
  Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
  --------------------------------------------------------------------------------
  VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 23.37 s
  --------------------------------------------------------------------------------
  OK
  INTERNET        38      0.7107894736842105
  SMS     16      0.495
  TYPE    1       NULL
  VOICE   25      3.3651999999999993
  Time taken: 24.3 seconds, Fetched: 4 row(s)
  ```

[Top](#top)

