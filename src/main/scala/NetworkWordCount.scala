
/**
 * Created by akhld on 13/9/14.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {

  def main(a: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val ssc = new StreamingContext("local[30]","Streaming Job",Seconds(5),
      "/home/akhld/mobi/localclusterxx/spark-1")


    val lines=ssc.socketTextStream("localhost", 12345)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}