package gouri.test

import java.io.File

import scala.math.random
import example.wc.GetWordCount.wordCount
import example.session.GetSession
import org.apache.commons.io
import org.apache.commons.io.FileUtils
import test.dataframe
import test.dataframe.WorkWithDataframe
/**
 * Hello world!
 *
 */
object App extends App {
  val ss = GetSession.GetSparkSession()

  //FileUtils.deleteQuietly(new File("gouri"))

  //val tmp = File.createTempFile("gouri", "tmp")
 // tmp.deleteOnExit()

//  var count = 0
//  for (i <- 1 to 100000) {
//    val x = random * 2 - 1
//    val y = random * 2 - 1
//    if (x * x + y * y <= 1) count += 1
//  }
//  println(s"Pi is roughly ${4 * count / 100000.0}")

  WorkWithDataframe.dfoperation(ss)
  //wordCount(ss)
}
