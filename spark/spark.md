# All about Apache Spark 

## Cluster Management
- Ambari


Zeppelin: Development Notebook (SPARK applications)
	- web-based notebook that enables interactive data analytics
	- collaborative documents with a rich set of pre-built language back-ends (or interpreters) such as 
		Scala (with Apache Spark), 
		Python (with Apache Spark), 
		SparkSQL, Hive, 
		Markdown, 
		Angular, 
		Shell.

Interpreters:
	Zeppelin Notebooks supports various interpreters which allow you to perform many operations on your data. Below are just a few of operations you can do with Zeppelin interpreters:

	- Ingestion
	- Munging
	- Wrangling
	- Visualization
	- Analysis
	- Processing
Launch Zeppelin Notbook
	- http://hdp.sandbox.com:9995/#/

Spark: a fast, in-memory data processing engine with elegant and expressive development APIs in Scala, Java, Python, and R that allow developers to execute a variety of data intensive workloads.
	- Spark itself written in Scala

## Use Cases:
- batch processing, 
- real-time streaming, 
- advanced modeling and analytics

## Userbase
- NASA
- Ebay
- Amazon
- Groupon
- TripAdvisor
- Yahoo
	
## Spark Datasets & DataFrames
- strongly typed distributed collections of data created from a variety of sources: 
- JSON and XML files, tables in Hive, external databases and more. 
- Conceptually, they are equivalent to a table in a relational database or a DataFrame in R or Python.
- Datasets are strongly typed (contrary to DataFrames)

## RDD
- Atomic
- Resilient	: to node failures
- Distributed : Across cluster
- Datasets : think of table
	

## Spark Components
- Core
- Streaming: Realtime ingestion & Analysis
- Spark SQL: SQL Interface to Spark
- MLLib: Iterative: Clustering, Classification, Regression
- GraphX: Social Networks, 

## Spark Context
- Spark shell creates an "sc"
- nums = parallelize([1,2,3,4])
- sc.textFile("file:///c:/path/to/a/huge/file.txt") or hdfs or s3n
- hiveContext = HiveContext(sc) 
- rows = hiveContext.sql("SELECT name, age FROM Employee")
- Spark Context can also be initialized using
	- JDBC
	- Cassandra
	- HBase
	- Json, CSV, XML, etc

## RDD Transformations
- map: rdd = sc.parallelize([1,2,3,4]); sqrRDD = rdd.map(lambda x: x*x)
- flatmap: 
- filter
- distinct
- sample
- union, intersection, subtract, cartesian

## Spark & HDP
	The following features and associated tools are not officially supported by Hortonworks:
- Spark Standalone
- Spark on Mesos
- Jupyter Notebook (formerly IPython)

	
Spark-submit

[Spark Distribution](https://www-us.apache.org/dist/spark/)

## Spark-submit
```
spark-submit --version
```

https://github.com/XD-DENG/Spark-practice
