Topics <a name='top'></a>
- [Dataset](#dataset)
- [Starting Spark Shell](#spark_shell)
- [PySpark Shell Commands](#spark_cmds)
- Transformations: [Numeric](#transformations_numeric), [Text](#transformations_text)
- Actions: [Numeric](#actions_numeric), [Text](#actions_text)
- [RDD Operations](#rdd_ops)
- [Wordcount Example](#wordcount_exp)
- [Spark Submit](#spark_submit)

# Dataset <a name='dataset'></a>
Load datasets into HDFS
1. Resources -> full_text.txt
2. Resources -> Other Datasets and Resources -> shakespeare.txt

[Top](#top)
# Starting Spark Shell <a name='spark_shell'></a>
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
[Top](#top)
# PySpark Shell Commands <a name='spark_cmds'></a>
These commands are for reference only. DO NOT RUN these now.
- Turn a Python collection into an RDD and print to screen
```shell
>>> numtest = sc.parallelize([1, 2, 3])
```
- Load text file from local FS
```shell
>>> texttest1 = sc.textFile("file:///home/YourUser/lab/full_text.txt")
```
- Load text file from HDFS
```shell
>>> texttest2 = sc.textFile("/user/lab/shakespeare.txt")
```
```shell
>>> texttest3 = sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/lab/shakespeare.txt")
```
[Top](#top)
# Basic Transformation (Numeric) <a name='transformations_numeric'></a>
- Numeric transformation example
```shell
>>> nums = sc.parallelize([1, 2, 3])
```
- Map each element to zero or more others and flatten into single large list
```shell
>>> numrange=nums.flatMap(lambda x: range(x))
```
- Pass each element through a function
```shell
>>> squares = nums.map(lambda x: x*x)
```
- Keep elements passing a predicate
```shell
>>> even = squares.filter(lambda x: x % 2 == 0)
```
[Top](#top)
# Basic Action (Numeric) <a name='actions_numeric'></a>

- Retrieve RDD contents as a local collection
```shell
>>> nums.collect()
>>> numrange.collect()
>>> squares.collect()
>>> even.collect()
```
- Return first K elements
```shell
>>> nums.take(2) 
```
-- Count number of elements
```shell
>>> nums.count()
```
- Merge elements with an associative function
```shell
>>> nums.reduce(lambda x, y: x + y)
```
- Write elements to a text file in HDFS
```shell
>>> nums.saveAsTextFile("numberfile.txt")
```
- To save to local file system
```shell
>>> X = nums.collect()
```
- Then save 'X' using standard Python write operations
[Top](#top)
# Basic Transformation (Text) <a name='transformations_text'></a>
- Text transformation example
```shell
>>> text = sc.textFile("/user/root/shakespeare.txt")
```
- Map each element to zero or more others and flatten into single large list
```shell
>>> words = text.flatMap(lambda line: line.split())
```
- Pass each element through a function
```shell
>>> wordWithCount = words.map(lambda word: (word, 1))
```
[Top](#top)
# Basic Action (Text) <a name='actions_text'></a>
- Return first K elements
```shell
>>> words.take(10)
```
- Count number of elements
```shell
>>> words.count()
```
[Top](#top)
# RDD Operations <a name='RDD_Ops'></a>
- Read in a text file
```shell
>>> mydata = sc.textFile("/user/lab/shakespeare.txt")
```
- Convert text to uppercase
```shell
>>> mydata_uc = mydata.map(lambda line: line.upper() )
```
- Filter the lines that start with 'I'
```shell
>>> mydata_filt = mydata_uc.filter(lambda line: line.startswith('I') )
```
- Count the number of filtered lines
```shell
>>> mydata_filt.count()
```
- Pair RDDs for Map Reduce Operations
  - You can pipe Spark operations one after another using the dot notation. 
  - Backslash \ stands for non-breaking new line.
```shell
>>> text = sc.textFile("/user/lab/full_text.txt") \
.map(lambda line: line.split("\t")) \
.map(lambda fields: (fields[0], fields[1]))
```
- Pair RDDS adding a key
```shell
>>> text = sc.textFile("/user/lab/full_text.txt") \
.keyBy(lambda line: line.split("\t")[0])
```
- Pairs with Complex Values
```shell
>>> text = sc.textFile(("/user/lab/full_text.txt")) \
.map(lambda line: line.split("\t")) \
.map(lambda fields: (fields[0], (fields[1], fields[2])))
```
# WordCount example <a name='wordcount_exp'></a>
```shell
>>> counts = sc.textFile("/user/lab/shakespeare.txt") \
.flatMap(lambda line: line.split() ) \
.map(lambda word: (word,1) ) \
.reduceByKey(lambda v1,v2: v1+v2)

>>> counts.take(10)
```

[Top](#top)
# Using Spark Submit <a name='spark_submit'></a>
- Submit a job (WordCount.py)  to the spark cluster without using the shell
  - First copy shakespeare.txt to /user/lab in HDFS
  - Copy WordCount.py into your Linux machine (Resources -> Spark Resources -> wordcount.py)
```shell
spark-submit --master yarn-client --executor-memory 512m --num-executors 3 --executor-cores 1 --driver-memory 512m wordCount.py
```
```py
from pyspark import SparkConf, SparkContext
def main(sc):
    textFile = sc.textFile("/user/lab/shakespeare.txt")
    wordList = textFile.flatMap(lambda line: line.split())
    wordCount = wordList.map(lambda word: (word,1))
    wordsWithTotalCount = wordCount.reduceByKey(lambda v1, v2: v1+v2)
    wordsWithTotalCount.saveAsTextFile("/user/root/spark_word_count.txt")
    topK = wordsWithTotalCount.collect()
    print(topK)
if __name__  == "__main__":
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf = conf)
    main(sc)
    sc.stop()
```
[Top](#top)


