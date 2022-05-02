from pyspark.sql import SparkSession
import os

os.environ["SPARK_HOME"] = "/opt/spark"
os.environ["AWS_ACCESS_KEY_ID"] = "5a201820-4987-42af-9360-bd57efe7fa23"
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
os.environ["HADOOP_HOME"] = "/opt/spark"
os.environ['PYTHONPATH'] = "/usr/bin/python"

spark = SparkSession.builder.appName("abc").master("local[*]").getOrCreate()
df = spark.read.option('header', 'false').parquet("cleaned_data.parquet")
print(df.columns)
# for removing spaces in column name if exists
for c in df.columns:
    df = df.withColumnRenamed(c, c.replace(" ", ""))

df = spark.read.schema(df.schema).parquet("cleaned_data.parquet").drop('Unnamed:0').drop('__index_level_0__')
df.printSchema
df.show()
postgres_uri = "jdbc:postgresql://database-1.chdx6w9e4vtm.us-east-1.rds.amazonaws.com:5432/postgres"
dbtable = "abc"
user = "postgres"
password = 'database'

df.write \
    .format("jdbc") \
    .mode("append") \
    .option("url", postgres_uri) \
    .option("dbtable", dbtable) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .save()
