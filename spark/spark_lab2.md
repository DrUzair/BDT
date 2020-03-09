# Transforming a Log File for Further Processing
## Dataset & Preparation
1. Download [NASA_access_log_Aug95.gz](http://www.ftpstatus.com/file_properties.php?sname=ftp.cs.umass.edu&fid=66) .
2. Copy it over into your Hadoop VM.
3. Unzip and ingest it into Hadoop, that is, move it to HDFS.
```shell
[root@sandbox data]# gunzip -f NASA_access_log_Aug95.gz
[root@sandbox data]# ls
full_text.txt  NASA_access_log_Aug95  shakespeare.txt
[root@sandbox data]#
```
4. Download pyspark-csv-master
5. Copy it over into your Hadoop VM.
6. Unzip and move *.py and *.pyc files to /usr/lib/python2.6/site-packages. Note that the archive creates its own directory when extracted.
```py
import csv
import sys
import dateutil.parser
from pyspark.sql.types import (StringType, DoubleType, TimestampType, NullType, IntegerType, StructType, StructField)
py_version = sys.version_info[0]
def csvToDataFrame(sqlCtx, rdd, columns=None, sep=",", parseDate=True):
    """Converts CSV plain text RDD into SparkSQL DataFrame (former SchemaRDD)
    using PySpark. If columns not given, assumes first row is the header.
    If separator not given, assumes comma separated
    """
    if py_version < 3:
        def toRow(line):
            return toRowSep(line.encode('utf-8'), sep)
    else:
        def toRow(line):
            return toRowSep(line, sep)

    rdd_array = rdd.map(toRow)
    rdd_sql = rdd_array

    if columns is None:
        columns = rdd_array.first()
        rdd_sql = rdd_array.zipWithIndex().filter(
            lambda r_i: r_i[1] > 0).keys()
    column_types = evaluateType(rdd_sql, parseDate)

    def toSqlRow(row):
        return toSqlRowWithType(row, column_types)

    schema = makeSchema(zip(columns, column_types))

    return sqlCtx.createDataFrame(rdd_sql.map(toSqlRow), schema=schema)

def makeSchema(columns):
    struct_field_map = {'string': StringType(),
                        'date': TimestampType(),
                        'double': DoubleType(),
                        'int': IntegerType(),
                        'none': NullType()}
    fields = [StructField(k, struct_field_map[v], True) for k, v in columns]

    return StructType(fields)

def toRowSep(line, d):
    """Parses one row using csv reader"""
    for r in csv.reader([line], delimiter=d):
        return r

def toSqlRowWithType(row, col_types):
    """Convert to sql.Row"""
    d = row
    for col, data in enumerate(row):
        typed = col_types[col]
        if isNone(data):
            d[col] = None
        elif typed == 'string':
            d[col] = data
        elif typed == 'int':
            d[col] = int(round(float(data)))
        elif typed == 'double':
            d[col] = float(data)
        elif typed == 'date':
            d[col] = toDate(data)
    return d


# Type converter
def isNone(d):
    return (d is None or d == 'None' or
            d == '?' or
            d == '' or
            d == 'NULL' or
            d == 'null')


def toDate(d):
    return dateutil.parser.parse(d)


def getRowType(row):
    """Infers types for each row"""
    d = row
    for col, data in enumerate(row):
        try:
            if isNone(data):
                d[col] = 'none'
            else:
                num = float(data)
                if num.is_integer():
                    d[col] = 'int'
                else:
                    d[col] = 'double'
        except:
            try:
                toDate(data)
                d[col] = 'date'
            except:
                d[col] = 'string'
    return d


def getRowTypeNoDate(row):
    """Infers types for each row"""
    d = row
    for col, data in enumerate(row):
        try:
            if isNone(data):
                d[col] = 'none'
            else:
                num = float(data)
                if num.is_integer():
                    d[col] = 'int'
                else:
                    d[col] = 'double'
        except:
            d[col] = 'string'
    return d


def reduceTypes(a, b):
    """Reduces column types among rows to find common denominator"""
    type_order = {'string': 0, 'date': 1, 'double': 2, 'int': 3, 'none': 4}
    reduce_map = {'int': {0: 'string', 1: 'string', 2: 'double'},
                  'double': {0: 'string', 1: 'string'},
                  'date': {0: 'string'}}
    d = a
    for col, a_type in enumerate(a):
        # a_type = a[col]
        b_type = b[col]
        if a_type == 'none':
            d[col] = b_type
        elif b_type == 'none':
            d[col] = a_type
        else:
            order_a = type_order[a_type]
            order_b = type_order[b_type]
            if order_a == order_b:
                d[col] = a_type
            elif order_a > order_b:
                d[col] = reduce_map[a_type][order_b]
            elif order_a < order_b:
                d[col] = reduce_map[b_type][order_a]
    return d


def evaluateType(rdd_sql, parseDate):
    if parseDate:
        return rdd_sql.map(getRowType).reduce(reduceTypes)
    else:
        return rdd_sql.map(getRowTypeNoDate).reduce(reduceTypes)

```
7. ssh login to your HDP2.4 vm.
8. Use vi editor to write the scripts ! ! !
9. Have A Primer on Spark using Python document available for reference purposes. 
10. Create lab10.split-log.pig script which splits the log file into 3 based on http status codes. Pay particular attention to how the log file is loaded as log file and stored in csv format.
11. Display the contents of /user/a2 in HDFS to see that there's only the log file stored in it.
12. Run the script. Note how the messages are avoided using stderr redirection.
13. Display the contents of /user/a2 in HDFS to see the additional files. Note that the split creates directories and data is partitioned in these directories. This is transparent when using HDFS. You don't need to make any reference to these partitions directly in your pig and/or pyspark scripts.
14. Display contents of part-m-00000 to see the header line and the csv format.
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
