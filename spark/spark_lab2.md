# Transforming a Log File for Further Processing
# Topics
- Dataset 
- PIG Scripts
    - define, split, CSVExcelStorage, CommonLoagLoader
    - 

## Dataset & Preparation
1. Download [NASA_access_log_Aug95.gz](http://www.ftpstatus.com/file_properties.php?sname=ftp.cs.umass.edu&fid=66) .
2. Copy it over into your Hadoop VM.
3. Unzip and ingest it into Hadoop, that is, move it to HDFS.
```shell
[root@sandbox data]# gunzip -f NASA_access_log_Aug95.gz
[root@sandbox data]# ls
NASA_access_log_Aug95
[root@sandbox data]# hadoop fs -put NASA_access_log_Aug95 /user/spark
[root@sandbox data]# hadoop fs -ls /user/spark
Found 1 item
-rw-r--r--   3 root hdfs  167813770 2020-03-09 14:36 /user/spark/NASA_access_log_Aug95
```
4. Download pyspark-csv-master
5. Copy it over into your Hadoop VM.
6. Unzip and move *.py files to /usr/lib/python2.6/site-packages. Note that the archive creates its own directory when extracted.
7. ssh login to your HDP2.4 vm.
8. Use vi editor to write the scripts ! ! !
9. Have A Primer on Spark using Python document available for reference purposes. 
# PIG Scripts
10. Create lab10.split-log.pig script which splits the log file into 3 based on http status codes. 
    - ![log_split script](log_split.png)
    - Pay particular attention to how the log file is loaded as log file and stored in csv format.
    - [CSVExcelStorage](https://pig.apache.org/docs/latest/api/org/apache/pig/piggybank/storage/CSVExcelStorage.html): CSV loading and storing with support for multi-line fields, and escaping of delimiters and double quotes within fields; uses CSV conventions of Excel 2007. 
    - [CommonLoagLoader](https://pig.apache.org/docs/r0.17.0/api/org/apache/pig/piggybank/storage/apachelog/CommonLogLoader.html): to load logs based on Apache's common log format, based on a format like LogFormat "%h %l %u %t \"%r\" %>s %b".
11. Display the contents of /user/spark in HDFS to see that there's only the log file stored in it.
```shell
[root@sandbox data]# hadoop fs -ls /user/spark
Found 1 item
-rw-r--r--   3 root hdfs  167813770 2020-03-09 14:36 /user/spark/NASA_access_log_Aug95
```
12. Run the script. Note how the messages are avoided using stderr redirection.
13. Display the contents of /user/spark in HDFS to see the additional files. Note that the split creates directories and data is partitioned in these directories. This is transparent when using HDFS. You don't need to make any reference to these partitions directly in your pig and/or pyspark scripts.
```shell
[root@sandbox data]# hadoop fs -ls /user/spark
Found 4 items
-rw-r--r--   3 root hdfs  167813770 2020-03-09 14:36 /user/spark/NASA_access_log_Aug95
drwxr-xr-x   - root hdfs          0 2020-03-09 15:39 /user/spark/NASA_log_200
drwxr-xr-x   - root hdfs          0 2020-03-09 15:39 /user/spark/NASA_log_404
drwxr-xr-x   - root hdfs          0 2020-03-09 15:39 /user/spark/NASA_log_else
```
14. Display contents of part-m-00000 to see the header line and the csv format.
```shell
[root@sandbox data]# hadoop fs -cat /user/spark/NASA_log_200/part-m-00000 | head -n 20
host,method,status,bytes
in24.inetnebr.com,GET,200,1839
ix-esc-ca2-07.ix.netcom.com,GET,200,1713
slppp6.intermind.net,GET,200,1687
piweba4y.prodigy.com,GET,200,11853
slppp6.intermind.net,GET,200,9202
slppp6.intermind.net,GET,200,3635
ix-esc-ca2-07.ix.netcom.com,GET,200,1173
```
15. Using vi, write lab10.sql.py and lab10.sql.sh scripts. Inspect lab10.sql.sh script and see how lab10.sql.py, the python, script is submitted. Also note that lab10.sql.sh script must have execute bit turned on.
16. Note how pyspark_csv.py script is imported in line 3 and incorporated into SparkContext in line 10. 
17. Read A Primer ... document for explanations on integration of Python with Spark.
18. Inspect the script source on the LHS with its output on the RHS. Note how pyspark_csv.py creates a dataframe directly from a csv file with header.

-------------------------
--  Hints
-------------------------

1. The original dataset is available at http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html . Visit this Webpage for dataset description.
2. Visit http://hadooptutorial.info/processing-logs-in-pig to learn how to load and process a log file in pig. Visit its homepage for other useful info.
3. Visit https://pig.apache.org for Pig project homepage and http://pig.apache.org/docs/r0.17.0 for Pig documentation.
4. Visit http://www.westwind.com/reference/OS-X/commandline/pipes.html, or https://www.tutorialspoint.com/unix/unix-pipes-filters.htm for I/O redirection and pipes in Unix/Linux/OSX.
5. Consult the Unix/Linux cheatsheets in the Course Shell.
6. Venerable vi editor ! Visit https://www.cs.colostate.edu/helpdocs/vi.html and https://engineering.purdue.edu/ECN/Support/KB/Docs/ViTextEditorTutorial .
