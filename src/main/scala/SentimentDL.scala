import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructType}

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
      .schema(schema)
      .load("hdfs://atlanta:30215/group/data/sentiment_data.csv")
      .repartition(3 * 32)

    //Load in the pre-trained model
    // Having trouble with the deep learning sentiment analysis pipeline, so the basic analysis pipeline will be used instead
    val model = PipelineModel.load("hdfs://atlanta:30215/group/modelData/analyze_sentiment_en_2.4.0_2.4_1580483464667")

    val analyzed_data = model.transform(df.select("tweet").withColumnRenamed("tweet", "text"))

    val results = analyzed_data.select("sentiment.result")

    results.write.csv("/group/out/test.csv")

  }
}
