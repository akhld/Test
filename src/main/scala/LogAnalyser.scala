import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * Created by akhld on 26/11/14.
 */

//So this is how the log file looks like
case class Logschema(log_ts: String, log_router: String, log_at: String, log_method: String,
                     log_path: String, log_host: String, log_fwd: String, log_dyno: String,
                     log_connect: String, log_service: String, log_status: String, log_bytes: String)

object LogAnalyser {

  def main(args: Array[String]): Unit = {

    //Just to make sure the log file has been taken from the command line (you can give hdfs url also)
    if(args.length < 1){
      println("Usage: sbt 'run /path/to/log/file'")
      System.exit(0)
    }

    //Comment the following lines if you want to see Whats happening!!
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val log_file = args(0)

    println("\n\n\n\n\nGiven log file: " + log_file)

    //Conf for Creating SparkContext
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Log Analyser")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress", "true")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    println("\nCreated SparkContext, Starting Processing...")

    //Lets parse the log file
    val parsed_rdd : RDD[Logschema] = sc.textFile(log_file).map(_.split(" ")).map(cols => {
      try {
        //val cols = line.split(" ")
        Logschema(cols(0), cols(1).replace(":", ""), cols(2).split("=")(1), cols(3).split("=")(1),
          cols(4).split("=")(1), cols(5).split("=")(1), cols(6).split("=")(1).replaceAll("\"", ""),
          cols(7).split("=")(1).replaceAll("web.", ""), cols(8).split("=")(1).replaceAll("ms", ""),
          cols(9).split("=")(1).replaceAll("ms", ""), cols(10).split("=")(1), cols(11).split("=")(1)  )

      }catch{ case e: Exception => {
        println("Exception!! => " + e)
        Logschema("", "", "", "", "", "", "", "", "", "", "", "")
      } }

    })

    //Lets get the data into memory
    parsed_rdd.registerTempTable("logs_table")

    //Yay! Now we can query :D

    val total_rdd = sqlContext.sql("SELECT count(*) FROM logs_table")
    val uniq_url_rdd = sqlContext.sql("SELECT DISTINCT(log_path) FROM logs_table")
    val avg_rdd = sqlContext.sql("SELECT AVG(log_connect+log_service) FROM logs_table")
    val dyno_rdd = sqlContext.sql("SELECT count(log_dyno) c, log_dyno FROM logs_table group by log_dyno order by c desc")

    //Now lets get those results in console!
    total_rdd.map(t => "\n\n\n\nTotal Number of log lines : " + (t(0).toString.toInt - 1)).take(1).foreach(println)
    println("The number of times the URL was called (DISTINCT): " + uniq_url_rdd.count())
    avg_rdd.map(t => "Average Response Time: " + t(0)).take(1).foreach(println)
    dyno_rdd.map(t => "Most Responded Dyno: web." + t(1) + " Total Response :" + t(0) + "\n\n").take(1).foreach(println)

  }

}
