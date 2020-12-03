import com.johnsnowlabs.nlp._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SentimentDL {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()

    val schema = new StructType()
      .add("id", StringType)
      .add("creationTime", StringType)
      .add("tweet", StringType);

    //convert the RDD into a dataframe for usage in the pipeline
    val df = spark.read.format("csv")
      .option("escape", "\"")
      .option("header", "true")
      .schema(schema)
      .load("hdfs://atlanta:30215/group/data/sentiment_data.csv")
      //.repartition(3 * 32)


    //Load in the pre-trained model
    // Having trouble with the deep learning sentiment analysis pipeline, so the basic analysis pipeline will be used instead
    val model = PipelineModel.load("hdfs://atlanta:30215/group/modelData/analyze_sentiment_en_2.4.0_2.4_1580483464667")


    //val analyzed_data = model.transform(df.select("tweet").withColumnRenamed("tweet", "text"))

    //val results = analyzed_data.select("sentiment.result")
    val transformed_data = model.transform(df.select("tweet").withColumnRenamed("tweet", "text"))
    val joined_data = transformed_data.join(df, transformed_data("text") === df("tweet"), "inner").select("musk_ID", "reply_ID", "tweet", "sentiment.result")
      .withColumn("result", concat_ws("|", col("result")))



    val documentAssembler = new DocumentAssembler().setInputCol("tweet").setOutputCol("document")
    val finisher = new Finisher().setInputCols("document").setOutputAsArray(false).setIncludeMetadata(true) // set to False to remove metadata

    val pipeline = new RecursivePipeline().setStages(Array(documentAssembler, finisher))
    //results.write.csv("/group/out/sentiment_test")

  }
}
