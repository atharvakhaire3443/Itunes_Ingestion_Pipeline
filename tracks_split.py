from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import get_json_object, col

if __name__ == '__main__':
    
    spark = SparkSession.builder.appName("Tracks_Split").getOrCreate()

    itunes_df = spark.read.parquet("s3://stock-news-cs777/Itunes_Flat")

    target_df = itunes_df.select("track_id","track_name","artist_name","track_censored_name","kind",
    "track_view_url","track_price","track_HD_price","currency","country","release_date","track_explicitness",
    "track_count","track_number","track_time_millis","is_streamable","content_advisory_rating"
    ).filter(itunes_df.track_id.isNotNull())

    target_df = target_df.distinct()

    target_df.coalesce(1).write.option("header","true").mode('overwrite').csv("s3://stock-news-cs777/Tracks")

    spark.stop()


