# Dataset
Load datasets into HDFS
1. Resources ->  full_text.txt
2. Resources ->  shakespeare.txt

# Starting Spark Shell
- First check your Spark directory to see if it matches the one shown below... change hdp version according to your installation
```shell
[root@sandbox ~]# cd /usr/hdp/2.4.0.0-169/
[root@sandbox 2.4.0.0-169]# ls
atlas             hive                 ranger-hive-plugin   spark
datafu            hive-hcatalog        ranger-kafka-plugin  sqoop
etc               kafka                ranger-kms           storm
falcon            knox                 ranger-knox-plugin   storm-slider-client
flume             oozie                ranger-solr-plugin   tez
hadoop            phoenix              ranger-storm-plugin  usr
hadoop-hdfs       pig                  ranger-usersync      zeppelin
hadoop-mapreduce  ranger-admin         ranger-yarn-plugin   zookeeper
hadoop-yarn       ranger-hbase-plugin  slider
hbase             ranger-hdfs-plugin   solr
```
- Setting SPARK environment variable 
```shell
[root@sandbox ~]# export SPARK_HOME=/usr/hdp/2.6.1.0-129/spark/
[root@sandbox ~]# export PATH=$SPARK_HOME/bin:$PATH
```

- Starting pySpark (at Linux prompt)
```shell
[root@sandbox ~]# pyspark
```

- Quitting pySpark
```shell
>>> quit()
```
# PySpark Shell Commands
These commands are for reference only. DO NOT RUN these now.
- Turn a Python collection into an RDD and print to screen
```shell
numtest = sc.parallelize([1, 2, 3])
```
- Load text file from local FS
```shell
texttest1 = sc.textFile("file:///home/YourUser/lab/full_text.txt")
```
- Load text file from HDFS
```shell
texttest2 = sc.textFile("/user/lab/shakespeare.txt")
```
```shell
texttest3 = sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/lab/shakespeare.txt")
```
# Basic Transformation (Numeric)

- Numeric transformation example
```shell
nums = sc.parallelize([1, 2, 3])
```
- Map each element to zero or more others and flatten into single large list
numrange=nums.flatMap(lambda x: range(x))
```shell
- Pass each element through a function
squares = nums.map(lambda x: x*x)
```
- Keep elements passing a predicate
```shell
even = squares.filter(lambda x: x % 2 == 0)
```
####################
# Basic Action (Numeric)
####################

-- Retrieve RDD contents as a local collection

nums.collect()
numrange.collect()
squares.collect()
even.collect()

-- Return first K elements

nums.take(2) 

-- Count number of elements

nums.count()

-- Merge elements with an associative function

nums.reduce(lambda x, y: x + y)

-- Write elements to a text file in HDFS

nums.saveAsTextFile("numberfile.txt")

-- To save to local file system

X = nums.collect()

-- Then save 'X' using standard Python write operations


####################
# Basic Transformation (Text)
####################

-- Text transformation example

text = sc.textFile("/user/root/shakespeare.txt")

-- Map each element to zero or more others and flatten into single large list
words = text.flatMap(lambda line: line.split())

-- Pass each element through a function
wordWithCount = words.map(lambda word: (word, 1))

####################
# Basic Action (Text)
####################

-- Return first K elements

words.take(10)

-- Count number of elements

words.count()


####################
#RDD Operations
####################

-- Read in a text file
mydata = sc.textFile("/user/lab/shakespeare.txt")

-- Convert text to uppercase
mydata_uc = mydata.map(lambda line: line.upper() )

-- Filter the lines that start with 'I'
mydata_filt = mydata_uc.filter(lambda line: line.startswith('I') )

-- Count the number of filtered lines
mydata_filt.count()


-- Pair RDDs for Map Reduce Operations

-- You can pipe Spark operations one after another using the dot notation. 
-- '\' stands for non-breaking new line.

text = sc.textFile("/user/lab/full_text.txt") \

.map(lambda line: line.split("\t")) \

.map(lambda fields: (fields[0], fields[1]))


-- Pair RDDS adding a key

text = sc.textFile("/user/lab/full_text.txt") \

.keyBy(lambda line: line.split("\t")[0])


-- Pairs with Complex Values

text = sc.textFile(("/user/lab/full_text.txt")) \

  .map(lambda line: line.split("\t")) \

  .map(lambda fields: (fields[0], (fields[1], fields[2])))


#####################
# WordCount example
####################


counts = sc.textFile("/user/lab/shakespeare.txt") \

  .flatMap(lambda line: line.split() ) \

  .map(lambda word: (word,1) ) \

  .reduceByKey(lambda v1,v2: v1+v2)

counts.take(10)

####################
# Using Spark Submit
####################

-- Submit a job (WordCount.py)  to the spark cluster without using the shell
-- First copy shakespeare.txt to /user/lab in HDFS
-- Copy WordCount.py into your Linux machine

spark-submit --master yarn-client --executor-memory 512m --num-executors 3 --executor-cores 1 --driver-memory 512m wordCount.py




