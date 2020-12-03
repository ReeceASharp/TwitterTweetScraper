import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._


val musk_schema = new StructType().add("dateGMT", StringType).add("musk_ID_", StringType).add("tweet", StringType).add("replies_count", StringType)
val musk_data = spark.read.format("csv").option("escape", "\"").schema(musk_schema).load("/group/output/musk_out_3/part*")

val sentiment_schema = new StructType().add("musk_ID", StringType).add("positive", StringType).add("negative", StringType);
val sentiment_count = spark.read.format("csv").schema(sentiment_schema).load("/group/output/sentiment_musk_final/part*")

val joined_data = musk_data.join(sentiment_count, musk_data("musk_ID_") === sentiment_count("musk_ID"))
joined_data.show(false)

val final_data = joined_data.select("dateGMT", "replies_count", "positive", "negative")

final_data.write.option("header", "true").csv("/group/output/final_final_output")

//val final_data = joined_data.select("reply_ID", "musk_ID", "sentiment")
