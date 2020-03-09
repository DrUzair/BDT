# Transforming a Log File for Further Processing
## Dataset & Preparation
1. Download [NASA_access_log_Aug95.gz](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) .
2. Copy it over into your Hadoop VM.
3. Unzip and ingest it into Hadoop, that is, move it to HDFS.
4. Download pyspark-csv-master
5. Copy it over into your Hadoop VM.
6. Unzip and move *.py and *.pyc files to /usr/lib/python2.6/site-packages. Note that the archive creates its own directory when extracted.
7. ssh login to your HDP2.4 vm.
8. Use vi editor to write the scripts ! ! !
9. Have A Primer on Spark using Python document available for reference purposes. 

Screenshot.0 :
-------------------------
-- LHS - Pig Script
-------------------------

10. Create lab10.split-log.pig script which splits the log file into 3 based on http status codes. Pay particular attention to how the log file is loaded as log file and stored in csv format.
11. Display the contents of /user/a2 in HDFS to see that there's only the log file stored in it.
12. Run the script. Note how the messages are avoided using stderr redirection.
13. Display the contents of /user/a2 in HDFS to see the additional files. Note that the split creates directories and data is partitioned in these directories. This is transparent when using HDFS. You don't need to make any reference to these partitions directly in your pig and/or pyspark scripts.
14. Display contents of part-m-00000 to see the header line and the csv format.

-------------------------
-- RHS - Python Script
-------------------------
15. Using vi, write lab10.sql.py and lab10.sql.sh scripts. Inspect lab10.sql.sh script and see how lab10.sql.py, the python, script is submitted. Also note that lab10.sql.sh script must have execute bit turned on.
16. Note how pyspark_csv.py script is imported in line 3 and incorporated into SparkContext in line 10. 
17. Read A Primer ... document for explanations on integration of Python with Spark.

Screenshot.1 :
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
