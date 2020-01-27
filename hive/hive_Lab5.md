# Lab 5: Programming Hive (2) 

- In case you don't have the geo-tagged tweet data in hadoop, you need reload it 
- To avoid confusion, please always include database name 'twitter.' as part of your hive table name. 
- If you don't specify the database name while you're not in the twitter database (use twitter), you will not find the the corresponding table.  e.g.,  twitter.full_text
- By default you're in a database called "default"

1. load geo-tagged tweets as external hive table

- Note: you can skip this if you already have twitter.full_text_ts table created from previous lab
- [HIVE Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)

## Topics <a name="top"></a> 

- [Dataset](#data)
- [Complex Data Types](#cdt)
  - array, map, struct
- [Collections Funtions](#collections)
  - array, map, struct
- [Advanced String Functions](#advstr)
  - sentences, ngrams, context_ngrams
- [UDAF](#udaf)
- [UDTF](#udtf)
- [Nested Queries](#nestedq)
- [Sqoop](#sqoop)

## Dataset <a name="data"></a> 

- create and load tweet data as external table

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
location '/user/twitter/full_text'; 
```

- note: you may have your data in a different hadoop directory and that's fine!

- convert timestamp

```sql
drop table twitter.full_text_ts;
create table twitter.full_text_ts as
select id, cast(concat(substr(ts,1,10), ' ', substr(ts,12,8)) as timestamp) as ts, lat, lon, tweet
from twitter.full_text;
```

[Top](#top)

# Complex Data Types <a name='cdt'></a>

Creating table and load data with complex types

**NOTE**: adapt twitter data to a proper format to exercise complex types

- create a temporary table schema

  ```sql
  drop table IF EXISTS twitter.full_text_ts_complex_tmp;
  create external table twitter.full_text_ts_complex_tmp (
                         id string,
                         ts timestamp,
                         lat float,
                         lon float,
                         tweet string,
                         location_array string, 
                         location_map string,
                         tweet_struct string
  )
  row format delimited
  fields terminated by '\t'
  stored as textfile
  location '/user/twitter/full_text_ts_complex';
  ```

  verify the directory for newly created table.  

  ```shell
  hive> dfs -ls /user/twitter;
  Found 2 items
  drwxr-xr-x   - root hdfs          0 2019-12-30 06:57 /user/twitter/full_text.txt
  drwxr-xr-x   - root hdfs          0 2019-12-30 20:25 /user/twitter/full_text_ts_complex
  ```

  The directory is empty for now.

  ```shell
  hive> dfs -ls /user/twitter/full_text_ts_complex/;
  ```

  

- load transformed data into the **full_text_ts_complex_tmp** table

  ```sql
  insert overwrite table twitter.full_text_ts_complex_tmp
  select id, ts, lat, lon, tweet, 
         concat(lat,',',lon) as location_array,
         concat('lat:', lat, ',', 'lon:', lon) as location_map, 
         concat(regexp_extract(lower(tweet), '(.*)@user_(\\S{8})([:| ])(.*)',2), ',', length(tweet)) as tweet_struct
  from twitter.full_text_ts;
  ```

  A hadoop cluster job ensued, 

  ```shell
  Query ID = root_20191230203324_4ab9abb9-8dbc-4e38-b05d-558a60a25827
  Total jobs = 1
  Launching Job 1 out of 1
  Tez session was closed. Reopening...
  Session re-established.
  
  
  Status: Running (Executing on YARN cluster with App id application_1576992085977_0013)
  
  --------------------------------------------------------------------------------
          VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
  --------------------------------------------------------------------------------
  Map 1 ..........   SUCCEEDED      4          4        0        0       0       0
  --------------------------------------------------------------------------------
  VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 74.48 s
  --------------------------------------------------------------------------------
  Loading data to table twitter.full_text_ts_complex_tmp
  Table twitter.full_text_ts_complex_tmp stats: [numFiles=4, numRows=377616, totalSize=69213153, rawDataSize=68835537]
  OK
  Time taken: 89.783 seconds
  ```

  Check the files on hdfs

  ```shell
  hive>  dfs -ls /user/twitter/full_text_ts_complex/;
  Found 4 items
  -rwxr-xr-x   1 root hdfs   20340506 2019-12-30 20:34 /user/twitter/full_text_ts_complex/000000_0
  -rwxr-xr-x   1 root hdfs   20317378 2019-12-30 20:34 /user/twitter/full_text_ts_complex/000001_0
  -rwxr-xr-x   1 root hdfs   20315498 2019-12-30 20:34 /user/twitter/full_text_ts_complex/000002_0
  -rwxr-xr-x   1 root hdfs    8239771 2019-12-30 20:34 /user/twitter/full_text_ts_complex/000003_0
  ```

  select first three rows

  ```sql
  select * from twitter.full_text_ts_complex_tmp limit 3;
  ```

  - **NOTE**: we can drop the tmp hive table because all we need is the HDFS file '/user/twitter/full_text_ts_complex'

  ```sql
  drop table twitter.full_text_ts_complex_tmp;
  ```

  - To prove that the directory is still there... 

  ```shell
  dfs -ls /user/twitter/full_text_ts_complex;
  ```

- Redefine the schema and reload the data;

  - Reload the temp file using complex types instead of strings
    **NOTE**: you specify the complex type when you create the table schema

  ```sql
  drop table IF EXISTS twitter.full_text_ts_complex;
  create external table twitter.full_text_ts_complex (
         id                 string,
         ts                 timestamp,
         lat                float,
         lon                float,
         tweet              string,
         location_array     array<float>,
         location_map       map<string, string>,
         tweet_struct       struct<mention:string, size:int>
  )
  ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t'
  COLLECTION ITEMS TERMINATED BY ','
  MAP KEYS TERMINATED BY ':'
  LOCATION '/user/twitter/full_text_ts_complex';
  ```

- display first three rows

  ```sql
  select * from twitter.full_text_ts_complex limit 3;
  ```

  ```shell
  > select * from twitter.full_text_ts_complex limit 3;
  OK
  USER_79321756   2010-03-03 04:15:26     47.528137       -122.197914     RT @USER_2ff4faca: IF SHE DO IT 1 MORE TIME......IMA KNOCK HER DAMN KOOFIE OFF.....ON MY MOMMA&gt;&gt;haha. #cutthatout [47.528137,-122.197914] {"lat":"47.528139","lon":"-122.197916"}  {"mention":"2ff4faca","size":119}
  USER_79321756   2010-03-03 04:55:32     47.528137       -122.197914     @USER_77a4822d @USER_2ff4faca okay:) lol. Saying ok to both of yall about to different things!:*        [47.528137,-122.197914] {"lat":"47.528139","lon":"-122.197916"}  {"mention":"2ff4faca","size":96}
  USER_79321756   2010-03-03 05:13:34     47.528137       -122.197914     RT @USER_5d4d777a: YOURE A FOR GETTING IN THE MIDDLE OF THIS @USER_ab059bdc WHO THE FUCK ARE YOU ? A FUCKING NOBODY !!!!&gt;&gt;Lol! Dayum! Aye!        [47.528137,-122.197914]  {"lat":"47.528139","lon":"-122.197916"} {"mention":"ab059bdc","size":144}
  Time taken: 0.23 seconds, Fetched: 3 row(s)
  hive> Shutting down tez session.
  ```


[Top](#top)

Collection Functions <a name='collections'></a>
-----------------------------------------------

- Create complex type directly using map(), array(), struct() functions

````sql
select id, ts, lat, lon, 
       array(lat, lon) as location_array, 
       map('lat', lat, 'lon', lon)  as location_map, 
       named_struct('lat', lat, 'lon',lon) as location_struct
from twitter.full_text_ts
limit 10;
````

- Work with collection functions
  - extract element from arrays/maps using indexing
  - extract element from struct using 'dot' notation

```sql
select location_array[0] as lat, 
       location_map['lon'] as lon, 
       tweet_struct.mention as mention,
       tweet_struct.size as tweet_length
from twitter.full_text_ts_complex
limit 5;
```

- Work with collection functions
  - extract all keys/values from maps
  - get number of elements in arrays/maps

```sql
select size(location_array), sort_array(location_array),
       size(location_map), map_keys(location_map), map_values(location_map)
from twitter.full_text_ts_complex
limit 5;
```

[Top](#top)

Advanced String Functions <a name='advstr'></a>
-----------------------------------------------

- **sentences** function

```sql
select sentences(tweet)
from twitter.full_text_ts
limit 10;
```

- **ngrams** function
  - find popular bigrams 

```sql
select ngrams(sentences(tweet), 2, 10)
from twitter.full_text_ts
limit 50;
```

- **ngrams** function with **explode()**

```sql
select explode(ngrams(sentences(tweet), 2, 10))
from twitter.full_text_ts
limit 50;
```

- **context_ngrams** function
  - find popular word after 'I need' bi-grams 

```sql
select explode(context_ngrams(sentences(tweet), array('I', 'need', null), 10))
from twitter.full_text_ts
limit 50;
```

- **context_ngrams** function
  -- *** find popular tri-grams after 'I need' bi-grams ***

```sql
select explode(context_ngrams(sentences(tweet), array('I', 'need', null, null, null), 10))
from twitter.full_text_ts
limit 50;
```

- **str_to_map**
  - map a string to map complex type

```sql
select str_to_map(concat('lat:',lat,',','lon:',lon),',',':') 
from twitter.full_text_ts
limit 10;
```

[Top](#top)

Aggregation Functions (UDAF) <a name='udaf'></a>
-----------------------------------------------

- **MIN** function
  - *** Find twitter user who reside on the west most point of U.S. ***
    - You can visualize it using the map tool at: http://www.darrinward.com/lat-long/?id=461435

```sql
select distinct lat, lon
from twitter.full_text_ts_complex x
where cast(x.lon as float) IN (select min(cast(y.lon as float)) as lon from twitter.full_text_ts y);
```



- SAVE the output of the above query in a directory on HDFS

```sql
INSERT OVERWRITE DIRECTORY '/user/lab/westUS'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
SELECT DISTINCT lat, lon
FROM twitter.full_text_ts_complex x
WHERE cast(x.lon as float) IN (select min(cast(y.lon as float)) as lon from twitter.full_text_ts y);
```

- **PERCENTILE_APPROX** function (works with DOUBLE type)
  - *** Find twitter users from north west part of U.S. ***
    - You can visualize it using the map tool: http://www.darrinward.com/lat-long/?id=461435

```sql
select percentile_approx(cast(lat as double), array(0.9))
from twitter.full_text_ts_complex;   --  41.79976907219686


select percentile_approx(cast(lon as double), array(0.1))
from twitter.full_text_ts_complex;   --  -117.06394155417728

select distinct lat, lon
from twitter.full_text_ts_complex
where cast(lat as double) >= 41.79976907219686 AND
      cast(lon as double) <= -117.06394155417728
limit 10;
```



- **HISTOGRAM_NUMERIC**
  - *** Bucket U.S. into 10x10 grids using histogram_numeric ***
    - get 10 variable-sized bins for latitude and longitude first
    - get 10 variable-sized bins for latitude and longitude first
    - use cross-join to create the grid
    - visualize the result using the map tool at: http://www.darrinward.com/lat-long/?id=461435

- get 10 variable-sized bins and their counts

```sql
select explode(histogram_numeric(lat, 10)) as hist_lat from twitter.full_text_ts_complex;
select explode(histogram_numeric(lon, 10)) as hist_lon from twitter.full_text_ts_complex;
```

- extract lat/lon points from the histogram output (struct type) separately 

```sql
select t.hist_lat.x from (select explode(histogram_numeric(lat, 10)) as hist_lat from twitter.full_text_ts_complex) t;

select t.hist_lon.x from (select explode(histogram_numeric(lon, 10)) as hist_lon from twitter.full_text_ts_complex) t;
```

- write a nested query the cross-joins the 10x10 lat/lon points 

```sql
select t1.lat, t2.lon
from 
    (select t.hist_lat.x as lat 
        from (select explode(histogram_numeric(lat, 10)) as hist_lat 
                from twitter.full_text_ts_complex
                where lat>=24.9493 AND lat<=49.5904 AND lon>=-125.0011 and lon<=-66.9326) t
    ) t1
JOIN
    (select t.hist_lon.x as lon 
        from (select explode(histogram_numeric(lon, 10)) as hist_lon 
                from twitter.full_text_ts_complex
                where lat>=24.9493 AND lat<=49.5904 AND lon>=-125.0011 and lon<=-66.9326) t
    ) t2;
```



[Top](#top)

-----------------------------------------------

Table-generating Functions (UDTF) <a name='udtf'></a>
-----------------------------------------------

- explode() function and lateral_view
  - explode() function is often used with lateral_view
    - we extracted twitter mentions from tweets in earlier exercises. 
    - You've probably noticed that it's not optimal solution because the query we wrote didn't handle multiple mentions. 
    - It only extract the very first mention. A better approach is to tokenize the tweet first and then explode the tokens into rows and extract mentions from each token

```sql
drop table twitter.full_text_ts_complex_1;
create table twitter.full_text_ts_complex_1 as
select id, ts, location_map, tweet, regexp_extract(lower(tweet_element), '(.*)@user_(\\S{8})([:| ])(.*)',2) as mention
from twitter.full_text_ts_complex
lateral view explode(split(tweet, '\\s')) tmp as tweet_element
where trim(regexp_extract(lower(tweet_element), '(.*)@user_(\\S{8})([:| ])(.*)',2)) != "" ;

select * from twitter.full_text_ts_complex_1 limit 10;
```



- collect_set function (UDAF)
  - collect_set() is a UDAF aggregation function.. we run the query at this step from the previous step, 
  - we get all the mentions in the tweets but if a user has multiple mentions in the same tweet, they are in different rows. 

  - To transpose all the mentions belonging to the same tweet/user, we can use the collect_set and group by to transpose the them into an array of mentions

```sql
create table twitter.full_text_ts_complex_2 as
select id, ts, location_map, tweet, collect_list(mention) as mentions
from twitter.full_text_ts_complex_1
group by id, ts, location_map, tweet;

describe twitter.full_text_ts_complex_2;

select * from twitter.full_text_ts_complex_2 
where size(mentions) > 5
limit 10;
```

[Top](#top)

-----------------------------------------------

Nested Queries <a name='nestedq'></a>
-----------------------------------------------

- *** tweets that have a lot of mentions ***

```sql
select t.*
from (select id, ts, location_map, mentions, size(mentions) as num_mentions 
      from twitter.full_text_ts_complex_2) t
order by t.num_mentions desc
limit 10;
```

[Top](#top)

# Sqoop <a name='sqoop'></a>

## Moving data between relational database and hadoop

- create a full_text_mysql table in mysql under twitter database and then sqoop the mysql table to hive 

- From your linux prompt, launch mysql

```shell
[root@sandbox etc]# mysql
```

- In mysql prompt...

```shell
mysql> show databases;
mysql> create database twitter;
```

- create mysql table

```mysql
mysql> create table twitter.full_text_mysql (id varchar(20), ts varchar(20), location varchar(20), lat varchar(20), lon varchar(20), tweet varchar(300));
```

- load twitter data into mysql and check whether the load is successful

```mysql
msyql> LOAD DATA LOCAL INFILE '/home/lab/full_text.txt' INTO TABLE twitter.full_text_mysql FIELDS TERMINATED BY '\t';
mysql> select * from twitter.full_text_mysql limit 3;
mysql> quit;
```

- From linux prompt, use sqoop to transfer full_table from mysql to hive

```shell
[root@sandbox etc]#  sqoop import -m 1 --connect jdbc:mysql://0.0.0.0:3306/twitter --username=root --password= --table full_text_mysql --driver com.mysql.jdbc.Driver --columns "id, ts, location" --map-column-hive id=string,ts=string,location=string --hive-import --fields-terminated-by '\t'  --hive-table twitter.full_text_mysql  --warehouse-dir /user/twitter/full_text_mysql
```

[Top](#top)
