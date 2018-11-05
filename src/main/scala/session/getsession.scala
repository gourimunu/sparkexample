package example.session

import org.apache.spark.sql.SparkSession

object GetSession {
  def GetSparkSession(): SparkSession ={

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("TEstByGouri")
      .getOrCreate()
    return spark
  }
}