package test.dataframe

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object WorkWithDataframe {
  def dfoperation(ss: SparkSession): Unit ={

    val inspictions = ss.read.option("header","true").format("csv").load("./data/inspections_plus.csv")
    inspictions.printSchema()
    inspictions.createGlobalTempView("globalrank")
    inspictions.select(inspictions("business_id"),inspictions("Score"),inspictions("date"),inspictions("type")).show(10)
    //val df = ss.read.csv("globalrank.csv")
   // val df = ss.read.option("header","true").format("csv").load("globalrank.csv")
    //df.show()
    //df.printSchema()
    //df.select("_c0").show()
    //df.select($"_c0").show()
    //df.createGlobalTempView("globalrank")
    //ss.sql("select * from global_temp.globalrank").show()
   // df.select(df("SourceSKU"),df("TargetSKU"), df("VersionSetID"), df("SystemScore"), df("SystemScore")+10).show()

    //df.select(df("SourceSKU"),df("TargetSKU"), df("SystemScore") < 100).show()
    //df.filter(df("SystemScore") < 100).show()
    //df.groupBy("SystemScore").count().show()
  }
}