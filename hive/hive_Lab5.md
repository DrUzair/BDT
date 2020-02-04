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
  - [Directily create array, map, struct](#direct_create_collect)
  - [Extract values](#extract_val)
  - [Sort_array, Keys, Values](#more_collect_funcs)
- [Advanced String Functions](#advstr)
  - [sentences](#sentences), [ngrams](#ngrams), [explode](#explode), [context_ngrams](#context_ngrams), [str_to_map](#str_to_map)
- [UDAF](#udaf)
  - [MIN](#min_udaf), [PERCENTILE_APPROX](#percentile_udaf), [HISTOGRAM_NUMERIC](#histo_udaf), [COLLECT_SET & COLLECT_LIST](#collect_set_udaf)
- [UDTF](#udtf)
  - [explode and lateral view](#udtf_explode_lv)
- [Nested Queries](#nestedq)
- [Partitioning and Bucketing](#pnb)
  - [The Dataset](#ml_data)
  - [Loading Movielense Data into Hive](#movies_data)
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

## Collection Functions <a name='collections'></a>

### Create complex type directly using map(), array(), struct() functions <a name='direct_create_collect'></a>

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
[Top](#top)
### Extract Array/Map Elements <a name='extract_val'></a>
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
[Top](#top)
### Work with collection functions <a name='more_collect_funcs'></a>
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

## Advanced String Functions <a name='advstr'></a>

### **sentences** function <a name='sentences'></a>
  - Tokenizes a string of natural language text into words and sentences, where each sentence is broken at the appropriate sentence boundary and returned as an array of words. The 'lang' and 'locale' are optional arguments. For example, sentences('Hello there! How are you?') returns ( ("Hello", "there"), ("How", "are", "you") ).

```sql
hive (twitter)> select sentences(tweet), tweet from full_text_ts limit 2;
OK
[["RT","USER","2ff4faca","IF","SHE","DO","IT","1","MORE","TIME","IMA","KNOCK","HER","DAMN","KOOFIE","OFF","ON","MY","MOMMA","gt","gt","haha"],["cutthatout"]]   RT @USER_2ff4faca: IF SHE DO IT 1 MORE TIME......IMA KNOCK HER DAMN KOOFIE OFF.....ON MY MOMMA&gt;&gt;haha. #cutthatout
[["USER","77a4822d","USER","2ff4faca","okay","lol"],["Saying","ok","to","both","of","yall","about","to","different","things"],[]]       @USER_77a4822d @USER_2ff4faca okay:) lol. Saying ok to both of yall about to different things!:*
Time taken: 1.281 seconds, Fetched: 2 row(s)
```
[Top](#top)
### **ngrams** function <a name='ngrams'></a>
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
[Top](#top)
### **ngrams** function with **explode()** explode <a name='explode'></a>
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
[Top](#top)
### **context_ngrams** function <a name='context_ngrams'></a>
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
[Top](#top)
- **context_ngrams** function 
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

```shell
hive (twitter)> select distinct lat, lon
              > from twitter.full_text_ts_complex x
              > where cast(x.lon as float) IN (select min(cast(y.lon as float)) as lon from twitter.full_text_ts y);
Query ID = root_20200204152323_80d5d036-6df7-45aa-a672-8877a3ea5e62
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Session re-established.


Status: Running (Executing on YARN cluster with App id application_1580765662954_0002)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      6          6        0        0       0       0
Map 3 ..........   SUCCEEDED      4          4        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
Reducer 4 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 04/04  [==========================>>] 100%  ELAPSED TIME: 446.74 s
--------------------------------------------------------------------------------
OK
43.17136        -124.37165
Time taken: 535.386 seconds, Fetched: 1 row(s)

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
  - __percentile_approx(DOUBLE col, array(p1 [, p2]...) [, B])__: Returns an approximate pth percentile of a numeric column (including floating point types) in the group. The B parameter controls approximation accuracy at the cost of memory. accepts and returns an array of percentile values.
  - *** Find twitter users from north west part of U.S. ***
    - You can visualize it using the map tool: http://www.darrinward.com/lat-long/?id=461435

```sql
select percentile_approx(cast(lat as double), array(0.9))
from twitter.full_text_ts_complex;   --  41.79976907219686


select percentile_approx(cast(lon as double), array(0.1))
from twitter.full_text_ts_complex;   --  -117.06394155417728
```

```sql
select distinct lat, lon
from twitter.full_text_ts_complex
where cast(lat as double) >= 41.79976907219686 AND
      cast(lon as double) <= -117.06394155417728
limit 10;
```

[Top](#top)

- **HISTOGRAM_NUMERIC** <a name='histo_udaf'></a>
  - __histogram_numeric(col, b)__: Computes a histogram of a numeric column in the group using b non-uniformly spaced bins. The output is an array of size b of double-valued (x,y) coordinates that represent the bin centers and heights
  - *** Bucket U.S. into 10x10 grids using histogram_numeric ***
    - get 10 variable-sized bins for latitude and longitude first
    - get 10 variable-sized bins for latitude and longitude first
    - use cross-join to create the grid
    - visualize the result using the map tool at: http://www.darrinward.com/lat-long/?id=461435

- get 10 variable-sized bins and their counts

```shell
hive (twitter)> select explode(histogram_numeric(lat, 10)) as hist_lat from twitter.full_text_ts_complex;
Query ID = root_20200204163106_927e72e5-cf58-49e3-89e4-5c5db290a838
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1580765662954_0003)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      6          6        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 60.49 s
--------------------------------------------------------------------------------
OK
{"x":-25.507317679268976,"y":42.0}
{"x":-7.171378443638485,"y":144.0}
{"x":3.775214721759161,"y":12.0}
{"x":13.004202445348104,"y":12.0}
{"x":18.60583110731475,"y":49.0}
{"x":28.66888114305991,"y":41611.0}
{"x":34.69055706225886,"y":109796.0}
{"x":40.755542000526084,"y":221597.0}
{"x":46.834658134682165,"y":4334.0}
{"x":55.82227104588559,"y":19.0}
Time taken: 69.709 seconds, Fetched: 10 row(s)
```

```sql
select explode(histogram_numeric(lon, 10)) as hist_lon from twitter.full_text_ts_complex;
```

- extract lat/lon points from the histogram output (struct type) separately 

```shell
hive (twitter)> select t.hist_lat.x from (select explode(histogram_numeric(lat, 10)) as hist_lat 
from twitter.full_text_ts_complex) t;

Query ID = root_20200204163358_3a66680e-d099-44b7-9bd7-b78de94b6c5a
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1580765662954_0003)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      6          6        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 62.04 s
--------------------------------------------------------------------------------
OK
-25.507317679268976
-7.171378443638485
3.775214721759161
13.004202445348104
18.60583110731475
28.66888114305991
34.69055706225886
40.755542000526084
46.834658134682165
55.82227104588559
Time taken: 75.356 seconds, Fetched: 10 row(s)
```

```sql
select t.hist_lon.y from (select explode(histogram_numeric(lon, 10)) as hist_lon 
from twitter.full_text_ts_complex) t;
```

[Top](#top)

- write a nested query the cross-joins the 10x10 lat/lon points 

```shell
hive (twitter)> select t1.lat, t2.lon
              > from
              >     (select t.hist_lat.x as lat
              >         from (select explode(histogram_numeric(lat, 10)) as hist_lat
              >                 from twitter.full_text_ts_complex
              >                 where lat>=24.9493 AND lat<=49.5904 AND lon>=-125.0011 and lon<=-66.9326) t
              >     ) t1
              > JOIN
              >     (select t.hist_lon.x as lon
              >         from (select explode(histogram_numeric(lon, 10)) as hist_lon
              >                 from twitter.full_text_ts_complex
              >                 where lat>=24.9493 AND lat<=49.5904 AND lon>=-125.0011 and lon<=-66.9326) t
              >     ) t2;
Warning: Map Join MAPJOIN[25][bigTable=?] in task 'Reducer 2' is a cross product
Query ID = root_20200204164218_d5086421-5f65-4506-9e11-66a64817c22d
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Dag submit failed due to java.io.IOException: All datanodes DatanodeInfoWithStorage[172.17.0.2:50010,DS-10c300da-2e2c-4d0e-bb2f-a01867179944,DISK] are bad. Aborting... stack trace: [org.apache.hadoop.service.ServiceStateException.convert(ServiceStateException.java:59), org.apache.hadoop.service.AbstractService.stop(AbstractService.java:225), org.apache.tez.dag.history.ats.acls.ATSV15HistoryACLPolicyManager.close(ATSV15HistoryACLPolicyManager.java:259), org.apache.tez.client.TezClient.stop(TezClient.java:594), org.apache.hadoop.hive.ql.exec.tez.TezSessionState.close(TezSessionState.java:270), org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager.close(TezSessionPoolManager.java:185), org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager.closeAndOpen(TezSessionPoolManager.java:310), org.apache.hadoop.hive.ql.exec.tez.TezTask.submit(TezTask.java:451), org.apache.hadoop.hive.ql.exec.tez.TezTask.execute(TezTask.java:181), org.apache.hadoop.hive.ql.exec.Task.executeTask(Task.java:160), org.apache.hadoop.hive.ql.exec.TaskRunner.runSequential(TaskRunner.java:89), org.apache.hadoop.hive.ql.Driver.launchTask(Driver.java:1745), org.apache.hadoop.hive.ql.Driver.execute(Driver.java:1491), org.apache.hadoop.hive.ql.Driver.runInternal(Driver.java:1289), org.apache.hadoop.hive.ql.Driver.run(Driver.java:1156), org.apache.hadoop.hive.ql.Driver.run(Driver.java:1146), org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:216), org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168), org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379), org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739), org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684), org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624), sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method), sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62), sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43), java.lang.reflect.Method.invoke(Method.java:498), org.apache.hadoop.util.RunJar.run(RunJar.java:233), org.apache.hadoop.util.RunJar.main(RunJar.java:148)] retrying...


Status: Running (Executing on YARN cluster with App id application_1580765662954_0004)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      6          6        0        0       2       0
Map 3 ..........   SUCCEEDED      6          6        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
Reducer 4 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 04/04  [==========================>>] 100%  ELAPSED TIME: 101.39 s
--------------------------------------------------------------------------------
OK
25.989036884035983      -122.0496318538321
25.989036884035983      -74.43433728110445
25.989036884035983      -79.24579998902354
25.989036884035983      -82.6378458384166
25.989036884035983      -88.53423418706629
25.989036884035983      -95.7496482747749
25.989036884035983      -100.21637271756143
25.989036884035983      -105.13660620194453
25.989036884035983      -111.754289292571
25.989036884035983      -117.79901808300262
28.257638877620384      -122.0496318538321
28.257638877620384      -74.43433728110445
28.257638877620384      -79.24579998902354
28.257638877620384      -82.6378458384166
28.257638877620384      -88.53423418706629
28.257638877620384      -95.7496482747749
28.257638877620384      -100.21637271756143
28.257638877620384      -105.13660620194453
28.257638877620384      -111.754289292571
28.257638877620384      -117.79901808300262
29.9119122372187        -122.0496318538321
29.9119122372187        -74.43433728110445
29.9119122372187        -79.24579998902354
29.9119122372187        -82.6378458384166
29.9119122372187        -88.53423418706629
29.9119122372187        -95.7496482747749
29.9119122372187        -100.21637271756143
29.9119122372187        -105.13660620194453
29.9119122372187        -111.754289292571
29.9119122372187        -117.79901808300262
33.647963519193596      -122.0496318538321
33.647963519193596      -74.43433728110445
33.647963519193596      -79.24579998902354
33.647963519193596      -82.6378458384166
33.647963519193596      -88.53423418706629
33.647963519193596      -95.7496482747749
33.647963519193596      -100.21637271756143
33.647963519193596      -105.13660620194453
33.647963519193596      -111.754289292571
33.647963519193596      -117.79901808300262
36.08724346662918       -122.0496318538321
36.08724346662918       -74.43433728110445
36.08724346662918       -79.24579998902354
36.08724346662918       -82.6378458384166
36.08724346662918       -88.53423418706629
36.08724346662918       -95.7496482747749
36.08724346662918       -100.21637271756143
36.08724346662918       -105.13660620194453
36.08724346662918       -111.754289292571
36.08724346662918       -117.79901808300262
38.634435623491505      -122.0496318538321
38.634435623491505      -74.43433728110445
38.634435623491505      -79.24579998902354
38.634435623491505      -82.6378458384166
38.634435623491505      -88.53423418706629
38.634435623491505      -95.7496482747749
38.634435623491505      -100.21637271756143
38.634435623491505      -105.13660620194453
38.634435623491505      -111.754289292571
38.634435623491505      -117.79901808300262
40.92781411988116       -122.0496318538321
40.92781411988116       -74.43433728110445
40.92781411988116       -79.24579998902354
40.92781411988116       -82.6378458384166
40.92781411988116       -88.53423418706629
40.92781411988116       -95.7496482747749
40.92781411988116       -100.21637271756143
40.92781411988116       -105.13660620194453
40.92781411988116       -111.754289292571
40.92781411988116       -117.79901808300262
43.03514002841662       -122.0496318538321
43.03514002841662       -74.43433728110445
43.03514002841662       -79.24579998902354
43.03514002841662       -82.6378458384166
43.03514002841662       -88.53423418706629
43.03514002841662       -95.7496482747749
43.03514002841662       -100.21637271756143
43.03514002841662       -105.13660620194453
43.03514002841662       -111.754289292571
43.03514002841662       -117.79901808300262
44.700213582059 -122.0496318538321
44.700213582059 -74.43433728110445
44.700213582059 -79.24579998902354
44.700213582059 -82.6378458384166
44.700213582059 -88.53423418706629
44.700213582059 -95.7496482747749
44.700213582059 -100.21637271756143
44.700213582059 -105.13660620194453
44.700213582059 -111.754289292571
44.700213582059 -117.79901808300262
47.568058500336996      -122.0496318538321
47.568058500336996      -74.43433728110445
47.568058500336996      -79.24579998902354
47.568058500336996      -82.6378458384166
47.568058500336996      -88.53423418706629
47.568058500336996      -95.7496482747749
47.568058500336996      -100.21637271756143
47.568058500336996      -105.13660620194453
47.568058500336996      -111.754289292571
47.568058500336996      -117.79901808300262
Time taken: 131.722 seconds, Fetched: 100 row(s)

```
[Top](#top)

- **collect_set** function (UDAF) <a name='collect_set_udaf'></a>
  - collect_set(col) returns a set of objects with duplicate elements eliminated.
  - collect_list(col) returns a set of objects without duplicate elements eliminated.
  - we run the query after completing [this step](#udtf_explode_lv) , 
  - we get all the mentions in the tweets but if a user has multiple mentions in the same tweet, they are in different rows. 
  - To transpose all the mentions belonging to the same tweet/user, we can use the collect_set and group by to transpose the them into an array of mentions

```sql
create table twitter.full_text_ts_complex_2 as
select id, ts, location_map, tweet, collect_list(mention) as mentions
from twitter.full_text_ts_complex_1
group by id, ts, location_map, tweet;
```
  - check schema
```shell
hive (twitter)> describe twitter.full_text_ts_complex_2;
OK
id                      string
ts                      timestamp
location_map            map<string,string>
tweet                   string
mentions                array<string>
Time taken: 1.85 seconds, Fetched: 5 row(s)
```
  - select first 5 rows
```shell
hive (twitter)> select id, mentions from twitter.full_text_ts_complex_2 
where size(mentions) > 5 AND size(mentions) < 10
limit 5;
```
[Top](#top)

## Table-generating Functions (UDTF) <a name='udtf'></a>

### explode() function and lateral_view <a name='udtf_explode_lv'></a>
  - __lateral view__ : Consider a table pageAds
  
  | Column        | Type          |
  | ------------- | ------------- |
  | PageID        | String        |
  | AdList        | Array\<int\>    |
  
  |   PageID      |   AdList   |
  |----------     |   ----------- |
  | front_page    |   [1, 2, 3]   |
  | contact_page  |   [3, 4, 5]   |
  ```sql
  SELECT pageid, adid
  FROM pageAds LATERAL VIEW explode(AdList) adTable AS adid;
  ```
  
  |   pageid      |   adid   |
  |----------     |   ----------- |
  | front_page    |   1   |
  | front_page    |   2   |
  | front_page    |   3   |
  | contact_page  |   3   |
  | contact_page  |   4   |
  | contact_page  |   5   |
  
  - explode() function is often used with lateral_view
  - 
    - we extracted twitter mentions from tweets in earlier exercises. 
    - You've probably noticed that it's not optimal solution because the query we wrote didn't handle multiple mentions. 
    - It only extract the very first mention. A better approach is to tokenize the tweet first and then explode the tokens into rows and extract mentions from each token

```sql
hive (twitter)> drop table twitter.full_text_ts_complex_1;
hive (twitter)> create table twitter.full_text_ts_complex_1 as
              > select id, ts, location_map, tweet, regexp_extract(lower(tweet_element), '(.*)@user_(\\S{8})([:| ])(.*)',2) as mention
              > from twitter.full_text_ts_complex
              > lateral view explode(split(tweet, '\\s')) tmp as tweet_element
              > where trim(regexp_extract(lower(tweet_element), '(.*)@user_(\\S{8})([:| ])(.*)',2)) != "" ;
Query ID = root_20200204165258_72bf1a14-7ce8-4859-9e06-206b7e7f2170
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1580765662954_0004)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      6          6        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 40.09 s
--------------------------------------------------------------------------------
Moving data to directory hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/twitter.db/full_text_ts_complex_1
Table twitter.full_text_ts_complex_1 stats: [numFiles=6, numRows=72856, totalSize=13061147, rawDataSize=12988291]
OK
Time taken: 76.764 seconds
```
Check one row
```shell
hive (twitter)> select * from twitter.full_text_ts_complex_1 limit 1;
OK
USER_79321756   2010-03-03 04:15:26     {"lat":"47.528139","lon":"-122.197916"} RT @USER_2ff4faca: IF SHE DO IT 1 MORE TIME......IMA KNOCK HER DAMN KOOFIE OFF.....ON MY MOMMA&gt;&gt;haha. #cutthatout 2ff4faca
```
[Top](#top)


## Nested Queries <a name='nestedq'></a>

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

## The Dataset <a name='ml_data'></a>
- the MovieLens dataset
  - The movielens dataset is a collection of movie ratings data and has been widely used in the industry and academia for experimenting with recommendation algorithms and we see many publications using this dataset to benchmark the performance of their algorithms.
  - For access to full-sized movielens data, go to http://grouplens.org/datasets/movielens/

- Loading User Ratings Data into Hive - u.data

**Steps:**

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
--------------------
| column     | name |
-------------------
| field_1    |  userid |
| field_2    | movieid |
| field_3    | rating  |
| field_4    | unixtime |
--------------------

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

### loading movies data into hive <a name='movies_data'></a>

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

```shell
$ hadoop fs -put /home/lab/action.txt /user/lab/action
$ hadoop fs -put /home/lab/comedy.txt /user/lab/comedy
$ hadoop fs -put /home/lab/thriller.txt /user/lab/thriller
```


2. create a table called movies_partition with 4 columns (movieid, movie_title, release_date, imdb_url) that is partitioned on genre

```shell
hive> CREATE TABLE ml.movies_part 
      (movieid int, 
      movie_name string, 
      release_date string, 
      imdb_url string)
PARTITIONED BY (genre string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
```

3. load each file into a partition
```shell
hive> LOAD DATA INPATH '/user/lab/action'
      INTO TABLE ml.movies_part
      PARTITION(genre='action');
hive> LOAD DATA INPATH '/user/lab/comedy'
      INTO TABLE ml.movies_part
      PARTITION(genre='comedy');
hive> LOAD DATA INPATH '/user/lab/thriller'
      INTO TABLE ml.movies_part
      PARTITION(genre='thriller');
```
4. describe the structure of the table and list the partitions (hint: describe and show partitions command)
```shell
hive> DESCRIBE ml.movies_part;
hive> SHOW PARTITIONS ml.movies_part;
```
5. look at the hive warehouse to see the 3 subdirectories
```shell
hive> dfs -ls /apps/hive/warehouse/ml.db/movies_part
```
6. create a table called rating_buckets with the same column definitions as user_ratings, but with 8 buckets, clustered on movieid
```shell
hive> CREATE TABLE ml.rating_buckets 
         (userid int, 
          movieid int, 
          rating int, 
          unixtime int)
      CLUSTERED BY (movieid) INTO 8 BUCKETS;
```
7. use insert overwrite table to load the rows in user_ratings into rating_buckets. Dont' forget to set mapred.reduce.tasks to 8
```shell
hive> SET mapred.reduce.tasks = 8;
hive> INSERT OVERWRITE TABLE ml.rating_buckets 
      SELECT *
      FROM ml.userratings CLUSTER BY movieid;
---

8. view the 8 files that were created. 
```shell
$ hadoop fs -ls /user/hive/warehouse/rating_buckets
```
9. count the rows in bucket 3 using tablesample
```shell
hive> SELECT count(1) FROM ml.rating_buckets
      TABLESAMPLE (BUCKET 3 OUT OF 8);[
```
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
