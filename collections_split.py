from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import get_json_object, col

if __name__ == '__main__':
    
    spark = SparkSession.builder.appName("Collections_Split").getOrCreate()

    itunes_df = spark.read.parquet("s3://stock-news-cs777/Itunes_Flat")

    target_df = itunes_df.select("collection_id","collection_name","artist_name","collection_censored_name",
    "collection_view_url","collection_price","collection_HD_price","currency","country","collection_explicitness",
    ).filter(itunes_df.collection_id.isNotNull())

    target_df = target_df.distinct()

    target_df.coalesce(1).write.option("header","true").mode('overwrite').csv("s3://stock-news-cs777/Collections")

    spark.stop()


