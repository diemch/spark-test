import pyspark
from datetime import datetime, timedelta
import time

def read_csv(spark):
    df = spark.read.csv("source.csv", header=True)
    return df

def create_spark_session():
    spark = (pyspark.sql.SparkSession.builder
             .appName("common-words")
             .getOrCreate())

    return spark

if __name__ == '__main__':
    spark = create_spark_session()
    print(spark.version)
    df = read_csv(spark)
    print(df.show())