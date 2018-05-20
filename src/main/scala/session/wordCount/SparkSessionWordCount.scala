package session.wordCount

import org.apache.spark.sql.SparkSession

object SparkSessionWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL WordCount")
      //.config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()
    //For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val wordsLine = spark.read.textFile("hdfs://hd1:9000/wordCount/words")
    val tuple = wordsLine.flatMap(_.split(" ")).map((_,1))
    val result  = tuple.rdd.reduceByKey((x,y) => x + y).sortByKey(true)
    result.foreach(println)


  }
}
