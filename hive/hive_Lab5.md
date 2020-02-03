# Lab 5: Programming Hive (2) 
- load geo-tagged tweets as external hive table
  - Note: you can skip this if you already have twitter.full_text_ts table created from previous lab
  - In case you don't have the geo-tagged tweet data in hadoop, you need reload it 
  - To avoid confusion, please always include database name 'twitter.' as part of your hive table name. 
    - set current db property to true so that hive prompt shows currently in-use database
    ```shell
    hive> set hive.cli.print.current.db=true;
    ```
  - If you don't specify the database name while you're not in the twitter database (use twitter), you will not find the the corresponding table.  e.g.,  twitter.full_text
  - By default you're in a database called "default"
- [HIVE Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)

## Topics <a name="top"></a> 

- [Dataset](#data)
- [Complex Data Types](#cdt)
  - array, map, struct
- [Collections Funtions](#collections)
  - array, map, struct
- [Advanced String Functions](#advstr)
  - sentences, ngrams, [context_ngrams](#context_ngrams), str_to_map
- [UDAF](#udaf)
  - [MIN](#min_udaf), [PERCENTILE_APPROX](#percentile_udaf), [HISTOGRAM_NUMERIC](#histo_udaf)
- [UDTF](#udtf)
- [Nested Queries](#nestedq)
- [Partitioning and Bucketing](#pnb)
- [Sqoop](#sqoop)

## Dataset <a name="data"></a> 
- Note: you can skip this if you already have twitter.full_text_ts table created from previous lab
- create and load tweet data as external table

```sql
hive (twitter)> drop table twitter.full_text;
create external table twitter.full_text (                                                 
        id string, 
        ts string, 
        lat_lon string,
        lat string, 
        lon string, 
        tweet string)
row format delimited 
fields terminated by '\t'
location '/user/lab/full_text.txt'; 
```

- note: you may have your data in a different hadoop directory and that's fine!

- convert timestamp

```sql
hive (twitter)> drop table twitter.full_text_ts;
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
  hive (twitter)> drop table IF EXISTS twitter.full_text_ts_complex_tmp;
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
  location '/user/lab/full_text_ts_complex';
  ```

  verify the directory for newly created table.  

  ```shell
  hive (twitter)> dfs -ls /user/lab;
  Found 2 items
  drwxr-xr-x   - root hdfs          0 2019-12-30 06:57 /user/lab/full_text.txt
  drwxr-xr-x   - root hdfs          0 2019-12-30 20:25 /user/lab/full_text_ts_complex
  ```

  The directory is empty for now.

  ```shell
  hive (twitter)> dfs -ls /user/lab/full_text_ts_complex/;
  ```

  

- load transformed data into the **full_text_ts_complex_tmp** table

  ```sql
  hive (twitter)> insert overwrite table twitter.full_text_ts_complex_tmp
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
  hive (twitter)>  dfs -ls /user/lab/full_text_ts_complex/;
  Found 4 items
  -rwxr-xr-x   1 root hdfs   20340506 2019-12-30 20:34 /user/lab/full_text_ts_complex/000000_0
  -rwxr-xr-x   1 root hdfs   20317378 2019-12-30 20:34 /user/lab/full_text_ts_complex/000001_0
  -rwxr-xr-x   1 root hdfs   20315498 2019-12-30 20:34 /user/lab/full_text_ts_complex/000002_0
  -rwxr-xr-x   1 root hdfs    8239771 2019-12-30 20:34 /user/lab/full_text_ts_complex/000003_0
  ```

  select first two rows

  ```sql
  hive (twitter)> select * from twitter.full_text_ts_complex_tmp limit 2;
  OK
  USER_79321756   2010-03-03 04:15:26     47.528137       -122.197914     RT @USER_2ff4faca: IF SHE DO IT 1 MORE TIME......IMA KNOCK HER DAMN KOOFIE OFF.....ON MY MOMMA&gt;&gt;haha. #cutthatout 47.528139,-122.197916   lat:47.528139,lon:-122.197916        2ff4faca,119
  USER_79321756   2010-03-03 04:55:32     47.528137       -122.197914     @USER_77a4822d @USER_2ff4faca okay:) lol. Saying ok to both of yall about to different things!:*        47.528139,-122.197916   lat:47.528139,lon:-122.197916   2ff4faca,96
  Time taken: 0.832 seconds, Fetched: 2 row(s)
  ```

  - **NOTE**: we can drop the tmp hive table because all we need is the HDFS file '/user/lab/full_text_ts_complex'
  ```sql
  hive (twitter)> drop table twitter.full_text_ts_complex_tmp;
  ```
  - Since the full_text_ts_complex_tmp was an __external table__, deleting it won't remove the actual files from hdrs. Verify the directory and files are still there... 
  ```shell
  hive (twitter)> dfs -ls /user/lab/full_text_ts_complex;
  Found 4 items
  -rwxr-xr-x   1 root hdfs   20340506 2020-02-03 15:46 /user/lab/full_text_ts_complex/000000_0
  -rwxr-xr-x   1 root hdfs   20317378 2020-02-03 15:46 /user/lab/full_text_ts_complex/000001_0
  -rwxr-xr-x   1 root hdfs   20315498 2020-02-03 15:46 /user/lab/full_text_ts_complex/000002_0
  -rwxr-xr-x   1 root hdfs    8239771 2020-02-03 15:46 /user/lab/full_text_ts_complex/000003_0
  ```
  - However the table is gone
  ```shell
  hive (twitter)> show tables;
  OK
  dayofweek
  full_text
  full_text_ts
  full_text_ts_complex
  tweets_per_user
  Time taken: 0.356 seconds, Fetched: 5 row(s)
  ````

- Redefine the schema and reload the data;
  - Reload the temp file using complex types instead of strings
    **NOTE**: you specify the complex type when you create the table schema

  ```sql
  hive (twitter)> drop table IF EXISTS twitter.full_text_ts_complex;
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
  LOCATION '/user/lab/full_text_ts_complex';
  ```

- display first two rows

  ```shell
  hive (twitter)> select * from twitter.full_text_ts_complex limit 2;
  OK
  USER_79321756   2010-03-03 04:15:26     47.528137       -122.197914     RT @USER_2ff4faca: IF SHE DO IT 1 MORE TIME......IMA KNOCK HER DAMN KOOFIE OFF.....ON MY MOMMA&gt;&gt;haha. #cutthatout [47.528137,-122.197914] {"lat":"47.528139","lon":"-122.197916"}  {"mention":"2ff4faca","size":119}
  USER_79321756   2010-03-03 04:55:32     47.528137       -122.197914     @USER_77a4822d @USER_2ff4faca okay:) lol. Saying ok to both of yall about to different things!:*        [47.528137,-122.197914] {"lat":"47.528139","lon":"-122.197916"}  {"mention":"2ff4faca","size":96}
  Time taken: 0.23 seconds, Fetched: 2 row(s)
  hive> Shutting down tez session.
  ```
- Check schema of newly created table; notice anything different ?
```shell
hive (twitter)> describe full_text_ts_complex;
OK
id                      string
ts                      timestamp
lat                     float
lon                     float
tweet                   string
location_array          array<float>
location_map            map<string,string>
tweet_struct            struct<mention:string,size:int>
Time taken: 0.786 seconds, Fetched: 8 row(s)
```

[Top](#top)

Collection Functions <a name='collections'></a>
-----------------------------------------------

- Create complex type directly using map(), array(), struct() functions

````sql
hive (twitter)> select id, ts, lat, lon,
              >        array(lat, lon) as location_array,
              >        map('lat', lat, 'lon', lon)  as location_map,
              >        named_struct('lat', lat, 'lon',lon) as location_struct
              > from twitter.full_text_ts
              > limit 2;
OK
USER_79321756   2010-03-03 04:15:26     47.528139       -122.197916     ["47.528139","-122.197916"]     {"lat":"47.528139","lon":"-122.197916"} {"lat":"47.528139","lon":"-122.197916"}
USER_79321756   2010-03-03 04:55:32     47.528139       -122.197916     ["47.528139","-122.197916"]     {"lat":"47.528139","lon":"-122.197916"} {"lat":"47.528139","lon":"-122.197916"}
Time taken: 1.253 seconds, Fetched: 2 row(s)
````

- Work with collection functions
  - extract element from arrays/maps using indexing
  - extract element from struct using 'dot' notation

```sql
hive (twitter)> select location_array[0] as lat,
              >        location_map['lon'] as lon,
              >        tweet_struct.mention as mention,
              >        tweet_struct.size as tweet_length
              > from twitter.full_text_ts_complex
              > limit 5;
OK
47.528137       -122.197916     2ff4faca        119
47.528137       -122.197916     2ff4faca        96
47.528137       -122.197916     ab059bdc        144
47.528137       -122.197916     77a4822d        89
47.528137       -122.197916             82
Time taken: 0.989 seconds, Fetched: 5 row(s)
```

- Work with collection functions
  - extract all keys/values from maps
  - get number of elements in arrays/maps

```sql
hive (twitter)> select size(location_array), sort_array(location_array),
              >        size(location_map), map_keys(location_map), map_values(location_map)
              > from twitter.full_text_ts_complex
              > limit 5;
OK
2       [-122.197914,47.528137] 2       ["lat","lon"]   ["47.528139","-122.197916"]
2       [-122.197914,47.528137] 2       ["lat","lon"]   ["47.528139","-122.197916"]
2       [-122.197914,47.528137] 2       ["lat","lon"]   ["47.528139","-122.197916"]
2       [-122.197914,47.528137] 2       ["lat","lon"]   ["47.528139","-122.197916"]
2       [-122.197914,47.528137] 2       ["lat","lon"]   ["47.528139","-122.197916"]
Time taken: 2.027 seconds, Fetched: 5 row(s)
```

[Top](#top)

Advanced String Functions <a name='advstr'></a>
-----------------------------------------------

- **sentences** function
  - Tokenizes a string of natural language text into words and sentences, where each sentence is broken at the appropriate sentence boundary and returned as an array of words. The 'lang' and 'locale' are optional arguments. For example, sentences('Hello there! How are you?') returns ( ("Hello", "there"), ("How", "are", "you") ).

```sql
hive (twitter)> select sentences(tweet), tweet from full_text_ts limit 2;
OK
[["RT","USER","2ff4faca","IF","SHE","DO","IT","1","MORE","TIME","IMA","KNOCK","HER","DAMN","KOOFIE","OFF","ON","MY","MOMMA","gt","gt","haha"],["cutthatout"]]   RT @USER_2ff4faca: IF SHE DO IT 1 MORE TIME......IMA KNOCK HER DAMN KOOFIE OFF.....ON MY MOMMA&gt;&gt;haha. #cutthatout
[["USER","77a4822d","USER","2ff4faca","okay","lol"],["Saying","ok","to","both","of","yall","about","to","different","things"],[]]       @USER_77a4822d @USER_2ff4faca okay:) lol. Saying ok to both of yall about to different things!:*
Time taken: 1.281 seconds, Fetched: 2 row(s)
```

- **ngrams** function 
  - Returns the top-k N-grams from a set of tokenized sentences,
  - find popular bigrams 

```sql
hive (twitter)> select ngrams(sentences(tweet), 2, 10)
              > from twitter.full_text_ts limit 1;
Query ID = root_20200127150004_2442c16e-e27c-4458-aa20-f108dc7eeedf
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Session re-established.


Status: Running (Executing on YARN cluster with App id application_1580133666416_0002)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      4          4        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 168.09 s
--------------------------------------------------------------------------------
OK
[{"ngram":["RT","USER"],"estfrequency":48782.0},{"ngram":["in","the"],"estfrequency":7327.0},{"ngram":["I","was"],"estfrequency":4760.0},{"ngram":["lt","lt"],"estfrequency":4668.0},{"ngram":["on","the"],"estfrequency":4405.0},{"ngram":["to","the"],"estfrequency":4127.0},{"ngram":["to","be"],"estfrequency":3982.0},{"ngram":["I","don't"],"estfrequency":3944.0},{"ngram":["to","get"],"estfrequency":3501.0},{"ngram":["I","need"],"estfrequency":3214.0}]
Time taken: 217.747 seconds, Fetched: 1 row(s)

```

- **ngrams** function with **explode()**
  - Explodes a map to multiple rows. Returns a row-set with a two columns (*key,value)* , one row for each key-value pair from the input map. 

```sql
hive (twitter)> select explode(ngrams(sentences(tweet), 2, 10))
              > from twitter.full_text_ts
              > limit 50;
Query ID = root_20200127151500_4df40261-e64a-49b6-8cda-8dc85dd16fba
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Session re-established.


Status: Running (Executing on YARN cluster with App id application_1580133666416_0003)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      4          4        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 103.68 s
--------------------------------------------------------------------------------
OK
{"ngram":["RT","USER"],"estfrequency":48782.0}
{"ngram":["in","the"],"estfrequency":7327.0}
{"ngram":["I","was"],"estfrequency":4760.0}
{"ngram":["lt","lt"],"estfrequency":4668.0}
{"ngram":["on","the"],"estfrequency":4405.0}
{"ngram":["to","the"],"estfrequency":4127.0}
{"ngram":["to","be"],"estfrequency":3982.0}
{"ngram":["I","don't"],"estfrequency":3944.0}
{"ngram":["to","get"],"estfrequency":3501.0}
{"ngram":["I","need"],"estfrequency":3214.0}
Time taken: 126.239 seconds, Fetched: 10 row(s)
```

- **context_ngrams** function
  - Returns the top-k contextual N-grams from a set of tokenized sentences, given a string of "context". 
  - find popular word after 'I need' bi-grams 

```sql
hive (twitter)> select explode(context_ngrams(sentences(tweet), array('I', 'need', null), 10))
              > from twitter.full_text_ts
              > limit 50;
Query ID = root_20200127152020_95b613b5-7029-4642-9126-3e41fa539c6e
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1580133666416_0003)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      4          4        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 66.24 s
--------------------------------------------------------------------------------
OK
{"ngram":["to"],"estfrequency":125.0}
{"ngram":["a"],"estfrequency":87.0}
{"ngram":["some"],"estfrequency":19.0}
{"ngram":["my"],"estfrequency":10.0}
{"ngram":["2"],"estfrequency":9.0}
{"ngram":["it"],"estfrequency":9.0}
{"ngram":["more"],"estfrequency":7.0}
{"ngram":["that"],"estfrequency":7.0}
{"ngram":["you"],"estfrequency":7.0}
{"ngram":["something"],"estfrequency":6.0}
Time taken: 70.641 seconds, Fetched: 10 row(s)

```

- **context_ngrams** function <a name='context_ngrams'></a>
  -- *** find popular tri-grams after 'I need' bi-grams ***

```sql
hive (twitter)> select explode(context_ngrams(sentences(tweet), array('I', 'need', null, null, null), 10))
              > from twitter.full_text_ts
              > limit 50;
Query ID = root_20200127152633_bd64f5c0-75b8-46ae-a5ad-36adc2207f51
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1580133666416_0003)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      4          4        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 56.10 s
--------------------------------------------------------------------------------
OK
{"ngram":["my","hair","done"],"estfrequency":3.0}
{"ngram":["to","take","a"],"estfrequency":3.0}
{"ngram":["a","new","job"],"estfrequency":2.0}
{"ngram":["a","new","phone"],"estfrequency":2.0}
{"ngram":["is","a","red"],"estfrequency":2.0}
{"ngram":["it","in","my"],"estfrequency":2.0}
{"ngram":["to","get","a"],"estfrequency":2.0}
{"ngram":["to","get","it"],"estfrequency":2.0}
{"ngram":["to","wash","my"],"estfrequency":2.0}
{"ngram":["to","work","on"],"estfrequency":2.0}
Time taken: 58.385 seconds, Fetched: 10 row(s)

```

[Top](#top)

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

- **MIN** function <a name='min_udaf'></a>
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

[Top](#top)

- **PERCENTILE_APPROX** function (works with DOUBLE type) <a name='percentile_udaf'></a>
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

[Top](#top)

- **HISTOGRAM_NUMERIC** <a name='histo_udaf'></a>
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

[Top](#top)

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



- **collect_set** function (UDAF)
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

# Partitioning and Bucketing <a name='pnb'></a>

## The Dataset <a name='pnb_data'></a>
- the MovieLens dataset
  - The movielens dataset is a collection of movie ratings data and has been widely used in the industry and academia for experimenting with recommendation algorithms and we see many publications using this dataset to benchmark the performance of their algorithms.
  - For access to full-sized movielens data, go to http://grouplens.org/datasets/movielens/

- Loading User Ratings Data into Hive - u.data

1. Upload movielens.tgz file to linux sandbox /root/lab
2. Extract the data from the MovieLens dataset

```shell
$ cd /root/lab
$ tar -zxvf movielens.tgz
$ ll
```

3. Examine the files

```shell
$ cd ml-data
$ more u.data
[root@sandbox ml-data]# more u.data
196     242     3       881250949
186     302     3       891717742
22      377     1       878887116
244     51      2       880606923
166     346     1       886397596
298     474     4       884182806
115     265     2       881171488
253     465     5       891628467
305     451     3       886324817
6       86      3       883603013
...
```

- Table description "u.data"

field_1     userid
field_2     movieid
field_3     rating 

field_4     unixtime

- Create a database called ml and table called user_ratings (tab-delimited)'

  ```sql
  hive> CREATE DATABASE ml;
  hive> CREATE TABLE ml.userratings
       (userid INT, movieid INT, rating INT,
        unixtime BIGINT) ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE;
  ```

- Move the u.data file into HDFS

  ```shell
  $ hadoop fs -put /root/lab/ml-data/u.data /user/lab/u.data
  ```
- Load the u.data into hive table

  ```sql
  hive> LOAD DATA INPATH '/user/lab/u.data'
        INTO TABLE ml.userratings;
  ```

  

- Verify that data was loaded

  ```sql
  hive> SELECT * FROM ml.userratings LIMIT 10;
  ```

## loading movies data into hive

- Move the movies data u.item into hadoop

```shell
$ hadoop fs -put /root/lab/ml-data/u.item /user/lab/u.item
```

- Examine the file

```shell
$ hadoop fs -cat /user/lab/u.item | head -n 5
```

- Create a table called movies 
  -- Read the README file for u.item column description

```sql
hive> CREATE TABLE ml.movies
         (movieid INT, 
          movie_title STRING, 
          release_date STRING,
          v_release_date STRING,
          imdb_url STRING,
          cat_unknown INT,
          cat_action INT, 
          cat_adventure INT,
          cat_animation INT,
          cat_children INT,
          cat_comedy INT,
          cat_crime INT,
          cat_documentary INT, 
          cat_drama INT, 
          cat_fantasy INT,
          cat_fill_noir INT,
          cat_horror INT,
          cat_musical INT,
          cat_mystery INT,
          cat_romance INT,
          cat_scifi INT,
          cat_thriller INT,
          cat_war INT,
          cat_western INT) 
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '|'
      STORED AS TEXTFILE;
```

- Load the u.item into hive table ml.Movies
  ```sql
  hive> LOAD DATA INPATH '/user/lab/u.item'
        INTO TABLE ml.Movies;
  ```

- verify

  ```sql
  hive> SELECT * from ml.Movies limit 20;
  ```

- examine the data in hdfs

  ```shell
  $ hadoop fs -ls /apps/hive/warehouse
  $ hadoop fs -ls /apps/hive/warehouse/ml.db/userratings
  $ hadoop fs -ls /apps/hive/warehouse/ml.db/movies
  ```

## Partitioning and bucketing data in hive


1. load action.txt, comedy.txt, thriller.txt into hdfs
You need to download these files from course shell and then upload to the sandbox first. Then copy to HDFS:

$ hadoop fs -put /home/lab/action.txt /user/lab/action
$ hadoop fs -put /home/lab/comedy.txt /user/lab/comedy
$ hadoop fs -put /home/lab/thriller.txt /user/lab/thriller



2. create a table called movies_partition with 4 columns (movieid, movie_title, release_date, imdb_url) that is partitioned on genre


hive> CREATE TABLE ml.movies_part 
      (movieid int, 
      movie_name string, 
      release_date string, 
      imdb_url string)
PARTITIONED BY (genre string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


3. load each file into a partition

hive> LOAD DATA INPATH '/user/lab/action'
      INTO TABLE ml.movies_part
      PARTITION(genre='action');
hive> LOAD DATA INPATH '/user/lab/comedy'
      INTO TABLE ml.movies_part
      PARTITION(genre='comedy');
hive> LOAD DATA INPATH '/user/lab/thriller'
      INTO TABLE ml.movies_part
      PARTITION(genre='thriller');

4. describe the structure of the table and list the partitions (hint: describe and show partitions command)
hive> DESCRIBE ml.movies_part;
hive> SHOW PARTITIONS ml.movies_part;

5. look at the hive warehouse to see the 3 subdirectories

hive> dfs -ls /apps/hive/warehouse/ml.db/movies_part
6. create a table called rating_buckets with the same column definitions as user_ratings, but with 8 buckets, clustered on movieid
hive> CREATE TABLE ml.rating_buckets 
         (userid int, 
          movieid int, 
          rating int, 
          unixtime int)
      CLUSTERED BY (movieid) INTO 8 BUCKETS;

7. use insert overwrite table to load the rows in user_ratings into rating_buckets. Dont' forget to set mapred.reduce.tasks to 8

hive> SET mapred.reduce.tasks = 8;
hive> INSERT OVERWRITE TABLE ml.rating_buckets 
      SELECT *
      FROM ml.userratings CLUSTER BY movieid;


8. view the 8 files that were created. 
$ hadoop fs -ls /user/hive/warehouse/rating_buckets

9. count the rows in bucket 3 using tablesample
hive> SELECT count(1) FROM ml.rating_buckets
      TABLESAMPLE (BUCKET 3 OUT OF 8);[

10. Inspect table description
```shell
hive> DESCRIBE FORMATTED ml.rating_buckets;
```
[Top](#top)

# Sqoop <a name='sqoop'></a>

## Moving data between relational database and hadoop

- create a full_text_mysql table in mysql under twitter database and then sqoop the mysql table to hive 
- From your linux prompt, launch mysql by typing mysql -u root -p in the terminal
  - for the sandbox, the password for mysql root is 'hadoop'

```shell
[root@sandbox /]# mysql -u root -p
Enter password:
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 52
Server version: 5.6.34 MySQL Community Server (GPL)

Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

- In mysql prompt... 
  - show databases;
  - create database twitter;

```shell
mysql> show databases;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| hive               |
| mysql              |
| performance_schema |
| ranger             |
+--------------------+
5 rows in set (0.82 sec)

mysql> create database twitter;
Query OK, 1 row affected (0.08 sec)

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
[root@sandbox etc]#  sqoop import -m 1 --connect jdbc:mysql://0.0.0.0:3306/twitter --username=root --password=hadoop --table full_text_mysql --driver com.mysql.jdbc.Driver --columns "id, ts, location" --map-column-hive id=string,ts=string,location=string --hive-import --fields-terminated-by '\t'  --hive-table twitter.full_text_mysql  --warehouse-dir /user/lab/full_text_mysql
```

[Top](#top)
