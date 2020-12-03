import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._


val sentiment_schema = new StructType().add("reply_ID_", StringType).add("sentiment", StringType);
val sentiment = spark.read.format("csv").option("escape", "\"").option("inferSchema", "true").schema(sentiment_schema).load("/group/output/identity_final/part*")
sentiment.rdd.getNumPartitions
sentiment.count()

val replies_schema = new StructType().add("musk_ID", StringType).add("reply_ID", StringType).add("tweet", StringType);
val replies = spark.read.format("csv").option("escape", "\"").option("inferSchema", "true").schema(replies_schema).load("/group/output/replyTweets_c/part*")
replies.rdd.getNumPartitions
replies.count()

val joined_data = sentiment.join(replies, sentiment("reply_ID_") === replies("reply_ID"), "inner")
joined_data.rdd.getNumPartitions

val final_data = joined_data.select("reply_ID", "musk_ID", "sentiment").distinct().repartition(400)

final_data.write.csv("/group/output/joinedSentiment")