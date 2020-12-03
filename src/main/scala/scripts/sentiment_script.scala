import com.johnsnowlabs.nlp._
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.annotator._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

val path = "/group/out/replyTweets"
val output = "/group/out/bee"
val final_path = "/group/out/clean_ughhhhhhh"

val spark = SparkSession.builder.getOrCreate()
val sc = SparkContext.getOrCreate()
val model = PipelineModel.load("/group/modelData/analyze_sentiment_en_2.4.0_2.4_1580483464667")

//sc.textFile(path + "/part*").coalesce(1).saveAsTextFile(output)

val schema = new StructType().add("musk_ID", StringType).add("reply_ID", StringType).add("tweet", StringType);
val df = spark.read.format("csv").option("escape", "\"").option("inferSchema", "true").schema(schema).load("/group/output/replyTweets_c" + "/part*")
df.count()


val transformed_data = model.transform(df.select("tweet").withColumnRenamed("tweet", "text"))

val joined_data = transformed_data.join(df, transformed_data("text") === df("tweet"), "inner").select("reply_ID", "sentiment.result").withColumn("result", concat_ws("|", col("result")))

joined_data.rdd.getNumPartitions

val partitioned_data = joined_data.repartition(400)

partitioned_data.write.csv("/group/output/replyTweets_sentiment_final")



//val model = PipelineModel.load("/group/modelData/sentimentModel")

// val documentAssembler = new DocumentAssembler().setInputCol("tweet").setOutputCol("document")
// val finisher = new Finisher().setInputCols("document").setOutputAsArray(false).setAnnotationSplitSymbol("@").setValueSplitSymbol("#").setIncludeMetadata(true) // set to False to remove metadata

// val pipeline = new RecursivePipeline().setStages(Array(documentAssembler, finisher))


//combine outputs from ReplyGenerator MapReduce jop into one giant file

//sc.textFile("/group/output/replyTweets_2011/part*").coalesce(1).saveAsTextFile("/group/output/replyTweets_2011_c")
// read in the input data, and the


//grab the text to be transformed by the sentiment analysis



//val newPipeOut = pipeline.fit(df).transform(df.select("tweet"))

//val output_data = transformed_data.select("text", "sentiment.result").withColumn("result_out", concat_ws("|", col("result")))

//val final_out = output_data.select("text", "result_out")


//final_out.write.csv(final_path)



//joined_data.repartition(10).write.csv("/group/out/new_exec____")

//val joined_data = joined_data.join(df, joined_data("text") === df("tweet"), "inner").select("musk_ID", "reply_ID", "tweet", "result_out")


//:load ../../spark/commands.scala