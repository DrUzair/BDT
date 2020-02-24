# Tutorial
# Topics <a name='top'></a>
- [Introduction](#intro)
- [Dataset and Scripts](#dataset)
- [Pig Utilities](#pigshell)
	- [Launching pig grunt shell](#laungch_grunt_shell)
	- [Executing pig commands/scripts from linux shell](#linux_shell)
	- [Executing pig commands/scripts from grunt shell](#grunt_shell)
		- [> grunt interactive](#grunt_interactive) , [> grunt exec](#grunt_batch) , [> grunt run](#grunt_run)
	- [linux shell from grunt](#shell_from_grunt), [hdfs from grunt](#hdfs_from_grunt)
- [Pig Basics](#piglatin)
- [Pig Functions](#pigfuncs)
  - [1. Datetime functions](#dtefuncs)
  - [2. String functions](#strfuncs); 
  	- [LOWER](#lower), [UPPER](#upper), [STARTSWITH](#STARTSWITH), [STRSPLIT](#STRSPLIT), [SUBSTRING](#SUBSTRING)
	- [TOKENIZE](#tokenize), [FLATTEN](#flatten)
  - [3. Conditional](#cond)
  - [4. Pig Relational Operations](#filter)
  	- [Filter](#filter), [group](#groupby), [cogroup](#cogroup)
  	- [Join](#join), [flatten](#flatten), [nested foreach](#nested_foreach),  [Cross](#cross)
- [5. Complex Data  Types](#cdt)
	- [Map](#map)
	- [Bag](#bag)
- [6. Working with Pig UDFs](#udf)
  - [Piggybank](#piggy),  [DataFu](#datafu), [Pigeon](#pigeon)



# Introduction <a name='intro'></a>

1. In this lab session, we will start working with Pig Shell - Grunt
2. File full_text.txt is available under D2L -> Resources -> Geo-tagged Tweets Dataset 
   	Use Filezilla to copy file onto the virtual machine to /home/lab
3. File dayofweek.txt is available under D2L -> Resources -> Geo-tagged Tweets Dataset
   	Use Filezilla to copy file onto the virtual machine to /home/lab
[Top](#top)

# Lab Data Preparation <a name='dataset'></a>
Run the following shell commands to place full_text.txt under /user/pig folder in HDFS

1. create a working directory in Linux and hdfs for this exercise
```shell
[hdfs@sandbox ~]$ hadoop fs -mkdir /user/pig
```

2. go to the directory that stores **full_text.txt** file in Linux (it may be different than _/home/lab_ on your machine)
```shell
[hdfs@sandbox ~]$ cd /home/lab
[root@sandbox lab]$ ll
total 56180
-rw-r--r-- 1 root root   374603 Apr 12  2018 cities15000.txt
-rw-r--r-- 1 root root      115 Jun 29  2016 dayofweek.txt
-rw-r--r-- 1 root root 57135918 May  7  2018 full_text.txt
-rw-r--r-- 1 root root      164 Dec 17 17:30 wc_mapper-2.py
-rw-r--r-- 1 root root      678 Dec 17 17:30 wc_reducer-2.py
```

3. load **full_text.txt** it into HDFS

```shell
[hdfs@sandbox lab]$ hadoop fs -put full_text.txt /user/pig/
[root@sandbox ~]# hadoop fs -cat /user/pig/full_text.txt | head
USER_79321756   2010-03-03T04:15:26     ÜT: 47.528139,-122.197916       47.528139       -122.197916     RT @USER_2ff4faca: IF SHE DO IT 1 MORE TIME......IMA KNOCK HER DAMN KOOFIE OFF.....ON MY MOMMA&gt;&gt;haha. #cutthatout
USER_79321756   2010-03-03T04:55:32     ÜT: 47.528139,-122.197916       47.528139       -122.197916     @USER_77a4822d @USER_2ff4faca okay:) lol. Saying ok to both of yall about to different things!:*
...
```

4. Create two pig scripts that we will use later in this lab for invocation of the scripts from pig grunt

- test1.pig
```shell
[hdfs@sandbox lab]$ echo -e "set job.name 'pig_test' \n a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray); \n b = limit a 5; \n dump b;" > /home/lab/test1.pig
```
- check using cat command
```shell
[hdfs@sandbox lab]$ cat test1.pig
set job.name 'pig_test'
 a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
 b = limit a 5;
 dump b;
```
- test2.pig
	- read the full_text from hdfs
	- apply schema
	- take first 5
	- store in /user/pig/full_text_limit3

```shell
[hdfs@sandbox lab]$ echo -e "set job.name 'pig_test' \n a2 = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray); \n b2 = limit a2 5; \n store b2 into '/user/pig/full_text_limit3';" > /home/lab/test2.pig
```
- check using cat command
```shell
[hdfs@sandbox lab]$ cat test2.pig
set job.name 'pig_test'
 a2 = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
 b2 = limit a2 5;
 store b2 into '/user/pig/full_text_limit3';
```

[Top](#top)

## Pig Shell/Utility Commands <a name='pigshell'></a>

### Launch pig grunt shell (interactive mode) <a name='laungch_grunt_shell'></a>

- NOTE: you can launch pig from any directory, ideally the directory where you store your pig scripts, functions and related files

```shell
[hdfs@sandbox lab]$  pig
```
- Quiting pig grunt shell
```shell
grunt>  quit;
```
[Top](#top)

### Executing pig commands/script (linux shell) <a name='linux_shell'></a>
#### Batch Mode
- test1.pig script
```shell
[hdfs@sandbox lab]$  pig test1.pig
```

- test2.pig script
	- In case you encounter _Cannot create directory /tmp/temp68693105. Name node is in safe mode._ error. Remove some files from hdfs that were used in previous exercises.
```shell
[root@sandbox lab]# pig test2.pig
20/02/11 16:37:47 INFO pig.ExecTypeProvider: Trying ExecType : LOCAL
20/02/11 16:37:47 INFO pig.ExecTypeProvider: Trying ExecType : MAPREDUCE
.
.
.
.
.
HadoopVersion   PigVersion      UserId  StartedAt       FinishedAt      Features
2.7.3.2.5.0.0-1245      0.16.0.2.5.0.0-1245     root    2020-02-11 16:38:03     2020-02-11 16:41:32     LIMIT

Success!

Job Stats (time in seconds):
JobId   Maps    Reduces MaxMapTime      MinMapTime      AvgMapTime      MedianMapTime   MaxReduceTime   MinReduceTime   AvgReduceTime   MedianReducetime        Alias   Feature Outputs
job_1581437856867_0001  1       1       13      13      13      13      15      15      15      15      a2,b2
job_1581437856867_0002  1       1       24      24      24      24      11      11      11      11      a2              /user/pig/full_text_limit3,

Input(s):
Successfully read 5 records (131457 bytes) from: "/user/pig/full_text.txt"

Output(s):
Successfully stored 5 records (950 bytes) in: "/user/pig/full_text_limit3"

Counters:
Total records written : 5
Total bytes written : 950
Spillable Memory Manager spill count : 0
Total bags proactively spilled: 0
Total records proactively spilled: 0

Job DAG:
job_1581437856867_0001  ->      job_1581437856867_0002,
job_1581437856867_0002


2020-02-11 16:41:33,069 [main] INFO  org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl - Timeline service address: http://sandbox.hortonworks.com:8188/ws/v1/timeline/
2020-02-11 16:41:33,069 [main] INFO  org.apache.hadoop.yarn.client.RMProxy - Connecting to ResourceManager at sandbox.hortonworks.com/172.17.0.2:8050
2020-02-11 16:41:33,070 [main] INFO  org.apache.hadoop.yarn.client.AHSProxy - Connecting to Application History server at sandbox.hortonworks.com/172.17.0.2:10200
2020-02-11 16:41:33,081 [main] INFO  org.apache.hadoop.mapred.ClientServiceDelegate - Application state is completed. FinalApplicationStatus=SUCCEEDED. Redirecting to job history server
2020-02-11 16:41:33,310 [main] INFO  org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl - Timeline service address: http://sandbox.hortonworks.com:8188/ws/v1/timeline/
2020-02-11 16:41:33,310 [main] INFO  org.apache.hadoop.yarn.client.RMProxy - Connecting to ResourceManager at sandbox.hortonworks.com/172.17.0.2:8050
2020-02-11 16:41:33,311 [main] INFO  org.apache.hadoop.yarn.client.AHSProxy - Connecting to Application History server at sandbox.hortonworks.com/172.17.0.2:10200
2020-02-11 16:41:33,326 [main] INFO  org.apache.hadoop.mapred.ClientServiceDelegate - Application state is completed. FinalApplicationStatus=SUCCEEDED. Redirecting to job history server
2020-02-11 16:41:33,531 [main] INFO  org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl - Timeline service address: http://sandbox.hortonworks.com:8188/ws/v1/timeline/
2020-02-11 16:41:33,532 [main] INFO  org.apache.hadoop.yarn.client.RMProxy - Connecting to ResourceManager at sandbox.hortonworks.com/172.17.0.2:8050
2020-02-11 16:41:33,532 [main] INFO  org.apache.hadoop.yarn.client.AHSProxy - Connecting to Application History server at sandbox.hortonworks.com/172.17.0.2:10200
2020-02-11 16:41:33,547 [main] INFO  org.apache.hadoop.mapred.ClientServiceDelegate - Application state is completed. FinalApplicationStatus=SUCCEEDED. Redirecting to job history server
2020-02-11 16:41:33,747 [main] INFO  org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl - Timeline service address: http://sandbox.hortonworks.com:8188/ws/v1/timeline/
2020-02-11 16:41:33,747 [main] INFO  org.apache.hadoop.yarn.client.RMProxy - Connecting to ResourceManager at sandbox.hortonworks.com/172.17.0.2:8050
2020-02-11 16:41:33,747 [main] INFO  org.apache.hadoop.yarn.client.AHSProxy - Connecting to Application History server at sandbox.hortonworks.com/172.17.0.2:10200
2020-02-11 16:41:33,763 [main] INFO  org.apache.hadoop.mapred.ClientServiceDelegate - Application state is completed. FinalApplicationStatus=SUCCEEDED. Redirecting to job history server
2020-02-11 16:41:33,951 [main] INFO  org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl - Timeline service address: http://sandbox.hortonworks.com:8188/ws/v1/timeline/
2020-02-11 16:41:33,951 [main] INFO  org.apache.hadoop.yarn.client.RMProxy - Connecting to ResourceManager at sandbox.hortonworks.com/172.17.0.2:8050
2020-02-11 16:41:33,952 [main] INFO  org.apache.hadoop.yarn.client.AHSProxy - Connecting to Application History server at sandbox.hortonworks.com/172.17.0.2:10200
2020-02-11 16:41:33,973 [main] INFO  org.apache.hadoop.mapred.ClientServiceDelegate - Application state is completed. FinalApplicationStatus=SUCCEEDED. Redirecting to job history server
2020-02-11 16:41:34,262 [main] INFO  org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl - Timeline service address: http://sandbox.hortonworks.com:8188/ws/v1/timeline/
2020-02-11 16:41:34,265 [main] INFO  org.apache.hadoop.yarn.client.RMProxy - Connecting to ResourceManager at sandbox.hortonworks.com/172.17.0.2:8050
2020-02-11 16:41:34,269 [main] INFO  org.apache.hadoop.yarn.client.AHSProxy - Connecting to Application History server at sandbox.hortonworks.com/172.17.0.2:10200
2020-02-11 16:41:34,285 [main] INFO  org.apache.hadoop.mapred.ClientServiceDelegate - Application state is completed. FinalApplicationStatus=SUCCEEDED. Redirecting to job history server
2020-02-11 16:41:34,386 [main] INFO  org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher - Success!
2020-02-11 16:41:34,504 [main] INFO  org.apache.pig.Main - Pig script completed in 3 minutes, 48 seconds and 196 milliseconds (228196 ms)
```

- Check the /user/pig directory on hdfs to verify the output.

```shell
[root@sandbox lab]$ hadoop fs -ls /user/pig/
Found 2 items
-rw-r--r--   1 root hdfs   57135918 2019-12-30 23:50 /user/pig/full_text.txt
drwxr-xr-x   - root hdfs          0 2020-01-01 00:47 /user/pig/full_text_limit3
```
[Top](#top)

### Executing pig commands/script (grunt shell) <a name='grunt_shell'></a>

#### interactive mode: pig grunt shell <a name='grunt_interactive'></a>
- Run individual lines of pig Latin;
```shell
grunt>  a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt>  b = limit a 2;
grunt> dump b;
2020-02-11 16:26:42,195 [main] INFO  org.apache.pig.tools.pigstats.ScriptState - Pig features used in the script: LIMIT
2020-02-11 16:26:42,392 [main] INFO  org.apache.pig.data.SchemaTupleBackend - Key [pig.schematuple] was not set... will not generate code.
2020-02-11 16:26:42,444 [main] INFO  org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer - {RULES_ENABLED=[AddForEach, ColumnMapKeyPrune, ConstantCalculator, GroupByConstParallelSetter, LimitOptimizer, LoadTypeCastInserter, MergeFilter, MergeForEach, PartitionFilterOptimizer, PredicatePushdownOptimizer, PushDownForEachFlatten, PushUpFilter, SplitFilter, StreamTypeCastInserter]}
2020-02-11 16:26:42,618 [main] INFO  org.apache.pig.impl.util.SpillableMemoryManager - Selected heap (PS Old Gen) of size 699400192 to monitor. collectionUsageThreshold = 489580128, usageThreshold = 489580128
2020-02-11 16:26:42,735 [main] INFO  org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter - File Output Committer Algorithm version is 1
2020-02-11 16:26:42,735 [main] INFO  org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter - FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2020-02-11 16:26:42,893 [main] INFO  org.apache.pig.data.SchemaTupleBackend - Key [pig.schematuple] was not set... will not generate code.
2020-02-11 16:26:43,025 [main] WARN  org.apache.pig.data.SchemaTupleBackend - SchemaTupleBackend has already been initialized
2020-02-11 16:26:43,032 [main] INFO  org.apache.pig.builtin.PigStorage - Using PigTextInputFormat
2020-02-11 16:26:43,063 [main] INFO  org.apache.hadoop.mapreduce.lib.input.FileInputFormat - Total input paths to process : 1
2020-02-11 16:26:43,074 [main] INFO  org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil - Total input paths to process : 1
2020-02-11 16:26:43,320 [main] INFO  com.hadoop.compression.lzo.GPLNativeCodeLoader - Loaded native gpl library
2020-02-11 16:26:43,395 [main] INFO  com.hadoop.compression.lzo.LzoCodec - Successfully loaded & initialized native-lzo library [hadoop-lzo rev 7a4b57bedce694048432dd5bf5b90a6c8ccdba80]
2020-02-11 16:26:46,846 [main] INFO  org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter - Saved output of task 'attempt__0001_m_000001_1' to hdfs://sandbox.hortonworks.com:8020/tmp/temp442487576/tmp813752665/_temporary/0/task__0001_m_000001
2020-02-11 16:26:46,947 [main] WARN  org.apache.pig.data.SchemaTupleBackend - SchemaTupleBackend has already been initialized
2020-02-11 16:26:46,973 [main] INFO  org.apache.hadoop.mapreduce.lib.input.FileInputFormat - Total input paths to process : 1
2020-02-11 16:26:46,973 [main] INFO  org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil - Total input paths to process : 1
(USER_79321756,2010-03-03T04:15:26,ÜT: 47.528139,-122.197916,47.528137,-122.197914,RT @USER_2ff4faca: IF SHE DO IT 1 MORE TIME...
(USER_79321756,2010-03-03T04:55:32,ÜT: 47.528139,-122.197916,47.528137,-122.197914,@USER_77a4822d @USER_2ff4faca okay:) lol. Saying ok to both of yall about to different things!:*)
```
- Describe a
```shell
grunt> describe a
a: {id: chararray,ts: chararray,location: chararray,lat: float,lon: float,tweet: chararray}
```
[Top](#top)

#### Batch Mode grunt shell <a name='grunt_batch'></a>
- execute a pig script from pig grunt (parameters/relations in the script are NOT passed to the current grunt environment)

```shell
grunt>  exec test1.pig
```

- test environment for parameters

```shell
grunt> describe a
2020-02-11 17:18:51,176 [main] ERROR org.apache.pig.tools.grunt.Grunt - ERROR 1003: Unable to find an operator for alias a
Details at logfile: /home/lab/pig_1581441417154.log
```
- Why did you get an error message? What went wrong here?
- __exec__ (batch mode) does not allow access from grunt shell to aliases within the script, 

[Top](#top)

#### Batch+ Mode grunt shell <a name='grunt_run'></a>
- run a pig script from pig grunt (parameters are passed to the current grunt environment)
- make sure you are in the same director where test1.pig script is
```shell
grunt> run test1.pig
grunt> set job.name 'pig_test'
grunt>  a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt>  b = limit a 5;
grunt>  dump b;
2020-02-11 18:27:40,412 [main] INFO  org.apache.pig.tools.pigstats.ScriptState - Pig features used in the script: LIMIT
.
.
.
(USER_79321756,2010-03-03T04:15:26,ÜT: 47.528139,-122.197916,47.528137,-122.197914,RT @USER_2ff4faca: IF SHE DO IT 1 MORE TIME......I
(USER_79321756,2010-03-03T04:55:32,ÜT: 47.528139,-122.197916,47.528137,-122.197914,@USER_77a4822d @USER_2ff4faca okay:) lol. Saying ok to both of yall about to different things!:*)
...
```

- test environment for parameters

```shell
grunt> describe a
a: {id: chararray,ts: chararray,location: chararray,lat: float,lon: float,tweet: chararray}
```
- __run__ (interactive mode) allows access to aliases defined in the script. Moreover, all the script commands are available in command history.

[Top](#top)

### Shell commands (running from Pig grunt) <a name='shell_from_grunt'></a>


```shell
[hdfs@sandbox lab]$  pig
grunt>  sh pwd
/home/lab
grunt> sh ls -alF /home/lab
total 61232
drwxr-xr-x 2 root root     4096 Feb 11 16:00 ./
drwxr-xr-x 1 root root     4096 Dec 17 17:42 ../
-rw-r--r-- 1 root root    25038 Jan 28 02:35 action.txt
-rw-r--r-- 1 root root   374603 Apr 12  2018 cities15000.txt
-rw-r--r-- 1 root root    51553 Jan 28 02:35 comedy.txt
-rw-r--r-- 1 root root       24 Jan  1 04:40 data
-rw-r--r-- 1 root root      115 Jun 29  2016 dayofweek.txt
-rw-r--r-- 1 root root    12527 Jan 28 02:04 full_text_mysql.java
-rw-r--r-- 1 root root 57135918 May  7  2018 full_text.txt
-rw-r--r-- 1 root root  4948405 Jan 28 02:35 movielens.tgz
-rw-r--r-- 1 root root    33637 Jan  1 04:57 pig_1577750226111.log
-rw-r--r-- 1 root root     1280 Jan  1 00:40 pig_1577839220501.log
-rw-r--r-- 1 root root     5232 Feb 11 15:55 pig_1581436344541.log
-rw-r--r-- 1 root root     6872 Feb 11 15:55 pig_1581436540458.log
-rw-r--r-- 1 root root    10708 Feb 11 16:00 pig_1581436792101.log
-rw-r--r-- 1 root root      180 Feb 11 15:44 test1.pig
-rw-r--r-- 1 root root      219 Feb 11 15:49 test2.pig
-rw-r--r-- 1 root root       71 Jan  1 04:56 text_data
-rw-r--r-- 1 root root    23876 Jan 28 02:35 thriller.txt
-rw-r--r-- 1 root root      164 Dec 17 17:30 wc_mapper-2.py
-rw-r--r-- 1 root root      678 Dec 17 17:30 wc_reducer-2.py
```
[Top](#top)
### Hadoop fs Shell commands (working with HDFS files) <a name='hdfs_from_grunt'></a>


- Run HDFS commands in pig grunt

	- Note: make sure you update the path properly 

```shell
grunt>  fs -ls /user
grunt>  fs -ls /user/pig
grunt>  fs -put /home/lab/full_text.txt /user/pig/full_text_1.txt
```

[Top](#top)


Utility commands
-------------------



1.7 list an HDFS directory in pig grunt

```shell
grunt>  fs -ls /user/pig
```

-- 1.8 remove a file/directory in pig grunt

```shell
grunt>  rmf /user/pig/full_text_limit3
```

**Note** In contrast to hadoop fs -rmdir /2/dir/ the rmf /2/dir doesn't throw exception if the director is not empty.

1.9 set pig job properties in pig grunt/script

```shell
grunt>  set job.name 'testing'
grunt>  set default_parallel 10
grunt>  set job.priority high
```

1.10 clear screen

```shell
grunt>  clear
```

1.11 quit pig grunt and return to Linux

```shell
grunt>  quit
```

**Note** The rest of the lab exercises should be run in grunt shell unless stated otherwise.

[Top](#top)

# 2. Pig Latin Basics <a name='piglatin'></a>

### 2.1 Data exploration using limit and dump

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = limit a 5;
grunt> dump b;
```

### 2.2 load and store data

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
rmf /user/pig/full_text_1.txt
grunt> c = store a into '/user/pig/full_text_1.txt';
```

### 2.3 Referencing fields (using position and field names)

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate $0, $1, location, tweet;
grunt> c = limit b 5;
grunt> dump c;
grunt> 
grunt> describe b;
```

[Top](#top)

# 3. Pig Functions <a name='pigfuncs'></a>

### 3.1 DATE/Time functions <a name='dtefuncs'></a>

- CurrentTime()
- GetYear()
- GetMonth()
- GetDay()
- GetWeek()
- ToDate()
- ToString()
- ToUnixTime()

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, ToDate(ts) as ts1;
grunt> c = foreach b generate id, ts, ts1, ToString(ts1) as ts_iso, ToUnixTime(ts1), GetYear(ts1) as year, GetMonth(ts1) as month, GetWeek(ts1) as week;
grunt> d = limit c 5;
grunt> dump d;
```

[Top](#top)

### 3.2 STRING functions <a name='strfuncs'></a>

- LOWER() <a name='lower'></a>

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, location, LOWER(tweet) as tweet;
grunt> c = limit b 5;
grunt> dump c;
```
[Top](#top)
- UPPER() <a name='upper'></a>

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, location, UPPER(tweet) as tweet;
grunt> c = limit b 5;
grunt> dump c;
```
[Top](#top)
- STARTSWITH()  <a name='STARTSWITH'></a>
  - to get retweets : tweets starting with RT 
  - explanation of filter, group, count will follow later in the exercise.

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = filter a by STARTSWITH(tweet,'RT');
grunt> c = group b all;
grunt> d = foreach c generate COUNT(b);
grunt> dump d;
```
[Top](#top)
- ENDSWITH()


- STRSPLIT() <a name='STRSPLIT'></a>
- [STRSPLITTOBAG(string, regex, limit)](https://pig.apache.org/docs/r0.17.0/api/org/apache/pig/builtin/STRSPLITTOBAG.html)
	- First parameter: a string to split;
	- The second parameter:  the delimiter or regex to split on; (optional) '\s' if not provided
	- The third parameter: a limit to the number of results. (max limit) (optional)
```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, STRSPLITTOBAG(tweet, '[ ",()*]', 0);
grunt> c = limit b 3;
grunt> dump c;
```
output
```shell
2020-02-15 13:57:57,318 [main] INFO  org.apache.pig.tools.pigstats.ScriptState - Pig features used in the script: LIMIT
2020-02-15 13:57:57,372 [main] INFO  org.apache.pig.data.SchemaTupleBackend - Key [pig.schematuple] was not set... will not generate code.
2020-02-15 13:57:57,372 [main] INFO  org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer - {RULES_ENABLED=[AddForEach, ColumnMapKeyPrune, ConstantCalculator, GroupByConstParallelSetter, LimitOptimizer, LoadTypeCastInserter, MergeFilter, MergeForEach, PartitionFilterOptimizer, PredicatePushdownOptimizer, PushDownForEachFlatten, PushUpFilter, SplitFilter, StreamTypeCastInserter]}
2020-02-15 13:57:57,374 [main] INFO  org.apache.pig.newplan.logical.rules.ColumnPruneVisitor - Columns pruned for a: $1, $2, $3, $4
2020-02-15 13:57:57,406 [main] INFO  org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter - File Output Committer Algorithm version is 1
2020-02-15 13:57:57,406 [main] INFO  org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter - FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2020-02-15 13:57:57,420 [main] INFO  org.apache.pig.data.SchemaTupleBackend - Key [pig.schematuple] was not set... will not generate code.
2020-02-15 13:57:57,449 [main] WARN  org.apache.pig.data.SchemaTupleBackend - SchemaTupleBackend has already been initialized
2020-02-15 13:57:57,450 [main] INFO  org.apache.pig.builtin.PigStorage - Using PigTextInputFormat
2020-02-15 13:57:57,453 [main] INFO  org.apache.hadoop.mapreduce.lib.input.FileInputFormat - Total input paths to process : 1
2020-02-15 13:57:57,453 [main] INFO  org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil - Total input paths to process : 1
2020-02-15 13:57:57,557 [main] INFO  org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter - Saved output of task 'attempt__0001_m_000001_1' to hdfs://sandbox.hortonworks.com:8020/tmp/temp-298388458/tmp1160460878/_temporary/0/task__0001_m_000001
2020-02-15 13:57:57,591 [main] WARN  org.apache.pig.data.SchemaTupleBackend - SchemaTupleBackend has already been initialized
2020-02-15 13:57:57,600 [main] INFO  org.apache.hadoop.mapreduce.lib.input.FileInputFormat - Total input paths to process : 1
2020-02-15 13:57:57,600 [main] INFO  org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil - Total input paths to process : 1
(USER_79321756,{(RT),(@USER_2ff4faca:),(IF),(SHE),(DO),(IT),(1),(MORE),(TIME......IMA),(KNOCK),(HER),(DAMN),(KOOFIE),(OFF.....ON),(MY),(MOMMA&gt;&gt;haha.),(#cutthatout)})
(USER_79321756,{(@USER_77a4822d),(@USER_2ff4faca),(okay:),(),(lol.),(Saying),(ok),(to),(both),(of),(yall),(about),(to),(different),(things!:)})
(USER_79321756,{(RT),(@USER_5d4d777a:),(YOURE),(A),(FOR),(GETTING),(IN),(THE),(MIDDLE),(OF),(THIS),(@USER_ab059bdc),(WHO),(THE),(),(ARE),(YOU),(?),(A),(ING),(NOBODY),(!!!!&gt;&gt;Lol!),(Dayum!),(Aye!)})

```
[Top](#top)

- TOKENIZE <a name='tokenize'></a>
	- **Case sensitive**
```shell
[root@sandbox ]# echo -e "this is a line; with a comma, and a semicolon; the end." > a_line
[root@sandbox ]# hadoop fs -put a_line /user/pig
```
In grunt shell
```shell
grunt> a = load '/user/pig/a_line' as (text:chararray);
grunt> b = foreach a generate TOKENIZE(text) as t;
grunt> dump b
...
({(this),(is),(a),(line;),(with),(a),(comma),(and),(a),(semicolon;),(the),(end.)})
grunt> c = foreach a generate flatten(TOKENIZE(text)) as t;
...
(this)
(is)
(a)
(line;)
(with)
(a)
(comma)
(and)
(a)
(semicolon;)
(the)
(end.)
```

- FLATTEN() <a name='FLATTEN'></a>
```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, FLATTEN(STRSPLITTOBAG(tweet, ' ', 0));
grunt> c = limit b 15;
grunt> dump c;
```
output
```shell
(USER_79321756,1)
(USER_79321756,DO)
(USER_79321756,IF)
(USER_79321756,IT)
(USER_79321756,MY)
(USER_79321756,RT)
(USER_79321756,HER)
(USER_79321756,SHE)
(USER_79321756,DAMN)
(USER_79321756,MORE)
(USER_79321756,KNOCK)
(USER_79321756,KOOFIE)
(USER_79321756,OFF.....ON)
(USER_79321756,TIME......IMA)
(USER_79321756,@USER_2ff4faca:)
```
[Top](#top)
- SIZE()

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, location, SIZE(tweet) as tweet_len;
grunt> c = order b by tweet_len desc;
grunt> d = limit c 10;
grunt> dump d;
```

- REPLACE()
  - Finding users who tweet long tweets

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, location, SIZE(REPLACE(tweet, '@USER_\\w{8}', '') ) as tweet_len;
grunt> c = order b by tweet_len desc;
grunt> d = limit c 10;
grunt> dump d;
```
[Top](#top)
- SUBSTRING() <a name='SUBSTRING'></a>
  - to extract year from ts string

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, SUBSTRING(ts, 0,4);
grunt> c = limit b 5;
grunt> dump c;
```
[Top](#top)

- TRIM()
- INDEXOF()
- LASTINDEXOF()
- REGEX_EXTRACT_ALL()
- REGEX_EXTRACT()
  - Find first twitter handles mentioned in a tweet

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, location, LOWER(tweet) as tweet;
grunt> c = foreach b generate id, ts, location, REGEX_EXTRACT(tweet, '(.*)@user_(\\S{8})([:| ])(.*)',2) as tweet;
grunt> d = limit c 5;
grunt> dump d;
```

-  Find first  3 twitter handles mentioned in a tweet 
  -  method 1

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, location, LOWER(tweet) as tweet;
grunt> c = foreach b generate id, ts, location,
     REGEX_EXTRACT(tweet, '[^@]*@user_(\\S{8})[^@]*', 1) as mentions1,
     REGEX_EXTRACT(tweet, '[^@]*@user_(\\S{8})[^@]*@user_(\\S{8})[^@]*', 2) as mentions2,
     REGEX_EXTRACT(tweet, '[^@]*@user_(\\S{8})[^@]*@user_(\\S{8})[^@]*@user_(\\S{8})[^@]*', 3) as mentions3;
grunt> d = limit c 20;
grunt> dump d;
```

  - method 2

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, location, LOWER(tweet) as tweet;
grunt> c = foreach b generate id, ts, location,
     SUBSTRING(STRSPLIT(tweet,'@user_').$1,0,8), 
     SUBSTRING(STRSPLIT(tweet,'@user_').$2,0,8),
     SUBSTRING(STRSPLIT(tweet,'@user_').$3,0,8);
grunt> d = limit c 20;
grunt> dump d;
```

- method 3

```shell
grunt> DEFINE TOP_ASC TOP('ASC'); 
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, location, LOWER(tweet) as tweet;
grunt> c = foreach b generate id, ts, location, tweet, FLATTEN(TOKENIZE(tweet)) as tokens;
grunt> d = foreach c generate id, ts, location, tweet, REGEX_EXTRACT(tokens,'.*@user_(\\w{8}).*',1) as token;
grunt> e = filter d by token IS NOT NULL;
grunt> f = foreach e generate id, ts, location, tweet, INDEXOF(tweet, token) as pos;
grunt> g = foreach f generate id, ts, pos, SUBSTRING(tweet,pos,pos+8) as mention;
grunt> h = group g by (id, ts);
grunt> i = foreach h {
        top3 = TOP_ASC(3,1,g);
        generate flatten(group) as (id, ts), top3 as mentions_top3;
    };
grunt> j = limit i 3000;
grunt> dump j;
```

- Tweet word count using Pig TOKENIZE() and FLATTEN()

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate FLATTEN(TOKENIZE(tweet)) as token;
grunt> c = group b by token;
grunt> d = foreach c generate group as token, COUNT(b) as cnt;
grunt> e = order d by cnt desc;
grunt> f = limit e 20;
grunt> dump f;
```

[Top](#top)

### CONDITIONAL function <a name='cond'></a>

- Find users who like to tw-eating

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, (GetHour(ToDate(ts))==7 ? 'breakfast' : 
                               (GetHour(ToDate(ts))==12 ? 'lunch' :
                               (GetHour(ToDate(ts))==19 ? 'dinner' : null))) as tw_eating, lat, lon;
grunt> c = filter b by tw_eating=='breakfast' or tw_eating=='lunch' or tw_eating=='dinner';
grunt> d = limit c 50;
grunt> dump d;
```

[Top](#top)

## 4. Pig Relational Operations 

### Filter <a name='filter'></a>

- 4.1 Find tweets that have mentions using FILTER

```shell
grunt> data = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> filtr = FILTER data BY tweet MATCHES '.*@USER_\\S{8}.*';
grunt> limt = limit filtr 500;
grunt> dump limt;
```

- 4.2 Find all tweets by a user

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = FILTER a by id=='USER_ae406f1d'; 
grunt> dump b;
```

- 4.3 Find all tweets tweeted from NYC vicinity (using bounding box -74.2589, 40.4774, -73.7004, 40.9176)
  -- http://www.darrinward.com/lat-long/?id=461435

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = FILTER a by lat > 40.4774 and lat < 40.9176 and lon > -74.2589 and lon < -73.7004 and 
                SIZE(tweet)<50 and 
                GetHour(ToDate(ts))==12;
grunt> c = foreach b generate lat, lon;
grunt> d = distinct c;
grunt> e = limit d 500;
grunt> dump e;
```

- 4.4 Filtering data in pig, find retweets in NYC on 12th with length smaller than 50 characters

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = FILTER a by lat > 40.4774 and lat < 40.9176 and lon > -74.2589 and lon < -73.7004 and 
                SIZE(tweet)<50 and 
                GetHour(ToDate(ts))==12;
grunt> c = foreach b generate id, ts, lat, lon, tweet;
grunt> d = limit c 10;
grunt> dump d;
```

[Top](#top)

 Group <a name='groupby'></a>
---------------------------

### GROUP BY - Aggregation

- 4.5 Calculate number of tweets per user 

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = group a by id;
grunt> c = foreach b generate group as id, COUNT(a) as cnt;
grunt> d = order c by cnt desc;
grunt> e = limit d 5;
grunt> dump e;
```

- 4.6 Count total number of records

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = group a ALL;
grunt> c = foreach b generate COUNT_STAR(a);
grunt> dump c;
```

[Top](#top)

--------------------------------------
### ORDER BY

- 4.7 Find top 10 tweeters in NYC

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = filter a by lat > 40.4774 and lat < 40.9176 and
      lon > -74.2589 and lon < -73.7004;
grunt> c = group b by id;
grunt> d = foreach c generate group as id, COUNT(b) as cnt;
grunt> e = order d by cnt desc;
grunt> f = limit e 10;
grunt> dump f;
```

[Top](#top)

# 5. Complex Data Types <a name='cdt'></a>

- 5.1 MAP <a name='map'></a>
	- Example 1
		- [FILTER](#filter)
		- [FLATTEN](#flatten)
		- PigStorage
Create example data file and store in hdfs.
```shell
[hdfs@sandbox ~]$ echo -e "user1\t{([address#123 st]),([name#abc]),([phone#222-222-2222]),([city#toronto])} \nuser2\t{([address#456 st]),([name#xyz]),([occupation#doctor]),([city#toronto])}\nuser3\t{([city#neverland]),([name#def]),([interest#sports])}" > data_test_map

[hdfs@sandbox ~]$ cat data_test_map
user1   {([address#123 st]),([name#abc]),([phone#222-222-2222]),([city#toronto])}
user2   {([address#456 st]),([name#xyz]),([occupation#doctor]),([city#toronto])}
user3   {([city#neverland]),([name#def]),([interest#sports])}

[hdfs@sandbox ~]$ hadoop fs -put data_test_map /user/pig/data_test_map
```
Open grunt shell.
```shell
grunt> a = load '/user/pig/data_test_map' using PigStorage('\t') as (id:chararray, info:bag{t:(m:map[])});
grunt> b = foreach a generate id, info, flatten(info) as info_flat;
grunt> c = filter b by info_flat#'city'=='toronto';
grunt> dump c;
.
.
.
(user1,{([address#123 st]),([name#abc]),([phone#222-222-2222]),([city#toronto])},[city#toronto])
(user2,{([address#456 st]),([name#xyz]),([occupation#doctor]),([city#toronto])},[city#toronto])
```
[Top](#top)
- 5.2 MAP example 2:  
	- data prep (transformations on the original full_text file and store into another file in HDFS)

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, TOTUPLE(lat, lon) as loc_tuple:tuple(lat:chararray, lon:chararray), flatten(TOKENIZE(tweet)) as token;
grunt> c = group b by (id, token);
grunt> d = foreach c generate flatten(group) as (id, token), COUNT(b) as cnt; 
grunt> e = group d by id;
grunt> f = foreach e generate group as id, flatten(TOP(10, 2, d)) as (id1, word,cnt);
grunt> g = foreach f generate id, TOMAP(word, cnt) as freq_word:map[];
grunt> h = group g by id;
grunt> store h into '/user/pig/full_text_t_map';

grunt> aa = load '/user/pig/full_text_t_map';       
grunt> bb = limit aa 3;
grunt> dump bb;
```

- load map type and extract tweeters who have tweeted word 'I' more than 5 times

```shell
grunt> a = load '/user/pig/full_text_t_map' as (id:chararray, freq_word:bag{t:(id1:chararray, freq_word_m:map[])});
grunt> b = foreach a generate id, flatten(freq_word) as (id1, freq_word_m);
grunt> c = filter b by (int)freq_word_m#'I' > 5;
grunt> d = limit c 10;
grunt> dump d;
```
[Top](#top)
- 5.3 BAG example <a name='bag'></a>
	- star expression

```shell
grunt> quit

[hdfs@sandbox ~]$ echo -e "user1\ta\tb\tc\nuser2\ta\tb\nuser3\ta" > data_test_bag
[hdfs@sandbox ~]$ cat data_test_bag
[hdfs@sandbox ~]$ hadoop fs -rmr /user/pig/data_test_bag
[hdfs@sandbox ~]$ hadoop fs -put data_test_bag /user/pig/data_test_bag



grunt> a = load '/user/pig/data_test_bag' using PigStorage('\t') as (id:chararray, f1:chararray, f2:chararray, f3:chararray);
grunt> b = group a ALL;
grunt> c = foreach b generate COUNT(a.$0);
grunt> d = foreach b generate COUNT(a.$1);
grunt> e = foreach b generate COUNT(a.$2);
grunt> f = foreach b generate COUNT(a.$3);
grunt> g = foreach b generate COUNT(a);
grunt> h = foreach b generate COUNT(a.*);   -- error
grunt> dump c;  -- 3
grunt> dump d;  -- 3
grunt> dump e;  -- 2
grunt> dump f;  -- 1
grunt> dump g;  -- 3
```

[Top](#top)

# 6. ADVANCED FUNCTIONS

### COGROUP <a name='cogroup'></a>

- 6.1 COGROUP example

```shell
[hdfs@sandbox ~]$ echo -e "u1,14,M,US\nu2,32,F,UK\nu3,22,M,US" > /home/lab/user.txt
[hdfs@sandbox ~]$ hadoop fs -put /home/lab/user.txt /user/pig/user.txt

-- u1, 14, M, US
-- u2, 32, F, UK
-- u3, 22, M, US

```shell
[hdfs@sandbox ~]$ echo -e "u1,US\nu1,UK\nu1,CA\nu2,US" > /home/lab/session.txt
[hdfs@sandbox ~]$ hadoop fs -put /home/lab/session.txt /user/pig/session.txt
```

-- u1, US
-- u1, UK
-- u1, CA
-- u2, US

```shell
grunt> user = load '/user/pig/user.txt' using PigStorage(',') as (uid:chararray, age:int, gender:chararray, region:chararray);
grunt> session = load '/user/pig/session.txt' using PigStorage(',') as (uid:chararray, region:chararray);
grunt> C = cogroup user by uid, session by uid;
grunt> D = foreach C {
    crossed = cross user, session;
    generate crossed;
}
grunt> dump D;  
```

- 6.2 Use COGROUP for SET Intersection 

```shell
[hdfs@sandbox ~]$ echo -e "John,3\nHarry,4\nGeorge,2" > /home/lab/s1.txt
[hdfs@sandbox ~]$ echo -e "John,2\nJohn,3\nGeorge,0\nSue,1" > /home/lab/s2.txt
[hdfs@sandbox ~]$ hadoop fs -put s1.txt /user/pig/  
[hdfs@sandbox ~]$ hadoop fs -put s2.txt /user/pig/  
[hdfs@sandbox ~]$ pig
grunt> s1 = load '/user/pig/s1.txt' using PigStorage(',') as (name:chararray, hits:int);
grunt> s2 = load '/user/pig/s2.txt' using PigStorage(',') as (name:chararray, errors:int);
grunt> grps = COGROUP s1 BY name, s2 BY name;
grunt> grps2 = FILTER grps by NOT(IsEmpty(s1)) AND NOT(IsEmpty(s2));
grunt> dump grps2;
```
-- 6.3 Use COGROUP for set difference 
```shell
grunt> s1 = load '/user/pig/s1.txt' using PigStorage(',') as (name:chararray, hits:int);
grunt> s2 = load '/user/pig/s2.txt' using PigStorage(',') as (name:chararray, errors:int);
grunt> grps = COGROUP s1 BY name, s2 BY name;
grunt> grps2 = FILTER grps by IsEmpty(s2);
grunt> set_diff = FOREACH grps2 GENERATE group as grp, s1, s2 ;
grunt> dump set_diff;
```

- 6.3 Use COGROUP for set difference 

```shell
grunt> s1 = load '/user/pig/s1.txt' using PigStorage(',') as (name:chararray, hits:int);
grunt> s2 = load '/user/pig/s2.txt' using PigStorage(',') as (name:chararray, errors:int);
grunt> grps = COGROUP s1 BY name, s2 BY name;
grunt> grps2 = FILTER grps by IsEmpty(s2);
grunt> set_diff = FOREACH grps2 GENERATE group as grp, s1, s2 ;
grunt> dump set_diff;
```

[Top](#top)

### Advanced JOIN <a name='join'></a>

- prepare lookup table 'dayofweek'

```shell
fs -put /home/lab/dayofweek.txt /user/pig/
```

- 6.4 **INNER JOIN** : Find Weekend Tweets

```shell
grunt> a = load '/user/pig/full_text.txt' using PigStorage('\t') AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> a1 = foreach a generate id, ts, SUBSTRING(ts,0,10) as date;

grunt> b = load '/user/pig/dayofweek.txt' using PigStorage('\t') as (date:chararray, dow:chararray);
grunt> b1 = filter b by dow=='Saturday' or dow=='Sunday';

grunt> c = join a1 by date, b1 by date;
grunt> d = foreach c generate a1::id .. a1::date, b1::dow as dow;
grunt> e = limit d 5;
grunt> dump e;
```

- 6.5 **Using Replicated JOIN** : Find Weekend Tweets


```shell
grunt> a = load '/user/pig/full_text.txt' using PigStorage('\t') AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> a1 = foreach a generate id, ts, SUBSTRING(ts,0,10) as date;

grunt> b = load '/user/pig/dayofweek.txt' using PigStorage('\t') as (date:chararray, dow:chararray);
grunt> b1 = filter b by dow=='Saturday' or dow=='Sunday';

grunt> c = join a1 by date, b1 by date using 'replicated';
grunt> d = foreach c generate a1::id .. a1::date, b1::dow as dow;
grunt> e = limit d 5;
grunt> dump e;
```

[Top](#top)

Flatten <a name='flatten'></a>
---------------------

- 6.6 **Flatten Tuples** : Calculate number of tweets per user per day

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, SUBSTRING(ts, 0, 10) as date, lat, lon, tweet;
grunt> c = GROUP b BY (id, date);
grunt> d = FOREACH c GENERATE FLATTEN(group) AS (id,date) , COUNT(b) as cnt;
grunt> e = order d by cnt desc;
grunt> f = limit e 5;
grunt> dump f;
```
-- visualize group
```shell
grunt> illustrate d;
```

- 6.7 **Flatten Bags** : Flatten Bags Example

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, SUBSTRING(ts, 0, 10) as date, lat, lon, tweet;
grunt> c = GROUP b BY (id, date);
grunt> d = FOREACH c GENERATE FLATTEN(b) AS (id, date, lat, lon, tweet);
grunt> e = order d by id, date;
grunt> f = limit e 50;
grunt> dump f;
```

[Top](#top)

Nested Foreach <a name='nested_foreach'></a>
--------------------------------------

- 6.8 Get top word of each user and store in bags
  - method 1

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, TOTUPLE(lat, lon) as loc_tuple:tuple(lat:chararray, lon:chararray), flatten(TOKENIZE(tweet)) as token;
grunt> c = group b by (id, token);
grunt> d = foreach c generate flatten(group) as (id, token), COUNT(b) as cnt; 
grunt> e = group d by id;
grunt> f = foreach e {
	sortd = order d by cnt desc;
	top = limit sortd 10;
	generate group as id, top as pop_word_bag;
};
grunt> g = limit f 10;
grunt> dump g;
```

- method 2

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, TOTUPLE(lat, lon) as loc_tuple:tuple(lat:chararray, lon:chararray), flatten(TOKENIZE(tweet)) as token;
grunt> c = group b by (id, token);
grunt> d = foreach c generate flatten(group) as (id, token), COUNT(b) as cnt; 
grunt> e = group d by id;
grunt> f = foreach e generate group as id, TOP(10, 2, d);
grunt> g = limit f 10;
grunt> dump g;
```

- 6.9 Nested Foreach example

```shell
hdfs@sandbox ~]$ echo -e "www.ccc.com,www.hjk.com\nwww.ddd.com,www.xyz.org\nwww.aaa.com,www.cvn.org\nwww.www.com,www.kpt.net\nwww.www.com,www.xyz.org\nwww.ddd.com,www.xyz.org" > /home/lab/url.txt
[hdfs@sandbox ~]$ hadoop fs -put url.txt /user/pig/
[hdfs@sandbox ~]$ pig
grunt> A = LOAD '/user/pig/url.txt' using PigStorage(',') AS (url:chararray,outlink:chararray);
grunt> B = GROUP A BY url;
grunt> X = FOREACH B {
        FA= FILTER A BY outlink == 'www.xyz.org';
        PA = FA.outlink;
        GENERATE group, COUNT(PA);
}
grunt> dump X;	
```

[Top](#top)

### CROSS <a name='cross'></a>

- 6.10 Parameter distribution using CROSS
  -- parameter file:
  --(nfriends, 2)
  --(ndays, 100)
  --(nvists, 13)

- friend table:
  --(Amy, George)
  --(George, Fred)
  --(Fred, Anne)
  --(George, Joe)
  --(George, Harry)

- Create parameter file and friend table using the Linux commands below

```shell
[hdfs@sandbox ~]$ echo -e "nfriends,2\nndays,100\nnvisits,13" > /home/lab/params.txt
[hdfs@sandbox ~]$ echo -e "Amy,George\nGeorge,Fred\nFred,Anne\nGeorge,Joe\nGeorge,Harry" > /home/lab/friend.txt
[hdfs@sandbox ~]$ hadoop fs -put params.txt /user/pig/  
[hdfs@sandbox ~]$ hadoop fs -put friend.txt /user/pig/  
[hdfs@sandbox ~]$ pig
grunt> params = load '/user/pig/params.txt' using PigStorage(',') as (p_name:chararray, value:int);
grunt> friend = load '/user/pig/friend.txt' using PigStorage(',') as (name:chararray, friend:chararray);

grunt> friend_grp = group friend by name;
grunt> friend_cnt = foreach friend_grp generate group as name, COUNT(friend.friend) as cnt;

grunt> friend_param = filter params by p_name=='nfriends';
grunt> friend_param_p = foreach friend_param generate value;

grunt> friend_cross = CROSS friend_cnt, friend_param_p;
grunt> friend_cross_1 = filter friend_cross by friend_cnt::cnt >= friend_param_p::value;
grunt> dump friend_cross_1;
```

[Top](#top)

Scalar Projection
--------------------------------------

- 6.11 Normalize the number of tweets of each user against global average number

```shell
grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = group a by id;
grunt> c = foreach b generate group as id, COUNT(a) as user_cnt;
grunt> d = group c ALL;
grunt> e = foreach d generate AVG(c.user_cnt) as global_avg;
grunt> f = foreach c generate id, user_cnt/(float)e.global_avg as index;
grunt> store f into '/user/pig/tweet_count_index';

grunt> a = load '/user/pig/tweet_count_index/*' as (id:chararray, index:float);
grunt> b = limit a 10;
grunt> dump b;
```

7. Pig UDF (User Defined Function) <a name='udf'></a>
----------------------------------------------------------------------------

piggybank UDFs <a name='piggy'></a>
-----------------

- 7.1 iso time to unix time conversion, register UDFs and define functions first

```shell
grunt> register '/home/lab/piggybank-0.15.0.jar';
grunt> define isotounix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

grunt> a = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> b = foreach a generate id, ts, isotounix(ts) as ts_unix;
grunt> c = limit b 3;
grunt> dump c;
```

[Top](#top)

-----------------
DataFu UDFs <a name='datafu'></a>
-----------------

- 7.2 Calculate median latitude value using DataFu median function, register UDFs and define functions first

```shell
grunt> register /home/lab/datafu-pig-incubating-1.3.0.jar
grunt> define Median datafu.pig.stats.StreamingMedian();
grunt> data = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> data1 = foreach (group data ALL) generate Median(data.lat);
grunt> dump data1;
```

- 7.3 Simple Random Sampling : Take a 1% random sample from the dataset
  - register UDFs and define functions first

```shell
grunt> register /home/lab/datafu-pig-incubating-1.3.0.jar
grunt> DEFINE SRS datafu.pig.sampling.SimpleRandomSample('0.01');
grunt> data = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> sampled = foreach (group data all) generate flatten(SRS(data));
grunt> store sampled into '/user/pig/full_text_src';
```

- 7.4 Stratified Sampling (by date)
  -- Take a 1% random sample from each date using SRS and group by date
  - register UDFs and define functions first

```shell
grunt> register /home/lab/datafu-pig-incubating-1.3.0.jar
grunt> DEFINE SRS datafu.pig.sampling.SimpleRandomSample('0.01');

grunt> data = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:float, lon:float, tweet:chararray);
grunt> data1 = foreach data generate id, SUBSTRING(ts, 0, 10) as date, lat, lon, tweet;
grunt> grouped = group data1 BY date;
grunt> sampled = foreach grouped generate flatten(SRS(data1));
grunt> store sampled into '/user/pig/full_text_stratified';
```

[Top](#top)

-----------------
Pigeon UDFs <a name='pigeon'></a>
-----------------

- 7.5 Find tweets tweeted from NYC using bounding box and Pigeon UDF
  -- Plot the results at: http://www.darrinward.com/lat-long/?id=490564 
  - register UDFs 

```shell
grunt> register /home/lab/pigeon-0.1.jar;
grunt> register /home/lab/esri-geometry-api-1.2.1.jar;
```

-- define functions

```shell
grunt> DEFINE ST_MakeBox edu.umn.cs.pigeon.MakeBox;
grunt> DEFINE ST_Contains edu.umn.cs.pigeon.Contains;
grunt> DEFINE ST_MakePoint edu.umn.cs.pigeon.MakePoint;

grunt> data = load '/user/pig/full_text.txt' AS (id:chararray, ts:chararray, location:chararray, lat:double, lon:double, tweet:chararray);
grunt> data1 = FOREACH data GENERATE id, ts, lat, lon, ST_MakePoint(lat, lon) AS geom_point, tweet;
grunt> data2 = FILTER data1 BY ST_Contains(ST_MakeBox(40.4774, -74.2589, 40.9176, -73.7004), geom_point);
grunt> data3 = limit data2 200;
grunt> data4 = foreach data3 generate lat, lon;
grunt> dump data4;
```

[Top](#top)
