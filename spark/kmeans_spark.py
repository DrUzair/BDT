from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Spark_Clustering_Lab") \
    .getOrCreate()

# !wget https://raw.githubusercontent.com/DrUzair/MLSD/master/Datasets/galton.csv
df_galton = spark.read.csv('galton.csv',inferSchema=True, header=True)

inputcols = ['parent', 'child']
assembler = VectorAssembler(inputCols= inputcols, outputCol = "features")
df_galton = assembler.transform(df_galton)
df_galton.show(5)

kmeans = KMeans(k=8, seed=1)  # 2 clusters here
model = kmeans.fit(df_galton.select('features'))

transformed = model.transform(df_galton)

import seaborn as sns
sns.lmplot( x="parent", y="child", data=transformed.toPandas(), fit_reg=False, hue='prediction', legend=True)

transformed.toPandas().to_csv('galton_clusters1.csv')
transformed.write.csv('galton_clusters2.csv')
