from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import get_json_object, col

if __name__ == '__main__':
    
    spark = SparkSession.builder.appName("Artists_Split").getOrCreate()

    itunes_df = spark.read.parquet("s3://stock-news-cs777/Itunes_Flat")

    target_df = itunes_df.select("artist_id","artist_name","artist_view_url","country",
    ).filter(itunes_df.artist_id.isNotNull())

    target_df = target_df.distinct()

    target_df.coalesce(1).write.option("header","true").mode('overwrite').csv("s3://stock-news-cs777/Artists")

    spark.stop()


