object CSVClean {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .option("inferSchema", "true")
      .load("hdfs://atlanta:30215/project/data/Question.csv")
      .repartition(3 * 32)

    val ds = df.select($"Body").map(line => {
      val body = line.getAs[String]("Body").toLowerCase()
      body.replaceAll("\\.|<[^>]*>|,|\\?", " ")
    })

    //Load in the pre-trained model, this model has multiple stages, in which it first converts the dataframe
    //into a
    val sentiment_model_DL = PipelineModel.load("hdfs://atlanta:30215/group/modelData/sentimentModel")

    val analyzed_data = sentiment_model_DL.transform(df)




  }
}
