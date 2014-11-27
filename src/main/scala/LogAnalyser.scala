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
    total_rdd.map(t => "\nOverview\n========\n\n\nTotal Number of log lines : " + (t(0).toString.toInt - 1)).take(1).foreach(println)
    println("The number of times the URL was called (DISTINCT): " + uniq_url_rdd.count())
    avg_rdd.map(t => "Average Response Time: " + t(0)).take(1).foreach(println)
    dyno_rdd.map(t => "Most Responded Dyno: web." + t(1) + " Total Response :" + t(0) + "\n\n").take(1).foreach(println)

    val pending_msgs = sqlContext.sql("SELECT count(*) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/count_pending_messages'")
    val pending_uniq_url_rdd = sqlContext.sql("SELECT DISTINCT(log_path) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/count_pending_messages'")
    val pending_avg_rdd = sqlContext.sql("SELECT AVG(log_connect+log_service) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/count_pending_messages'")
    val pending_dyno_rdd = sqlContext.sql("SELECT count(log_dyno) c, log_dyno FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/count_pending_messages' group by log_dyno order by c desc")


    pending_msgs.map(t => "\nOverview for GET /api/users/{user_id}/count_pending_messages\n" +
                            "============================================================\n\n\nTotal Number of log lines : " + t(0)).take(1).foreach(println)
    println("The number of times the URL was called (DISTINCT): " + pending_uniq_url_rdd.count())
    pending_avg_rdd.map(t => "Average Response Time: " + t(0)).take(1).foreach(println)
    pending_dyno_rdd.map(t => "Most Responded Dyno: web." + t(1) + " Total Response :" + t(0) + "\n\n").take(1).foreach(println)


    val msgs = sqlContext.sql("SELECT count(*) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_messages'")
    val msgs_uniq_url_rdd = sqlContext.sql("SELECT DISTINCT(log_path) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_messages'")
    val msgs_avg_rdd = sqlContext.sql("SELECT AVG(log_connect+log_service) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_messages'")
    val msgs_dyno_rdd = sqlContext.sql("SELECT count(log_dyno) c, log_dyno FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_messages' group by log_dyno order by c desc")


    msgs.map(t => "\nOverview for GET /api/users/{user_id}/get_messages\n" +
      "============================================================\n\n\nTotal Number of log lines : " + t(0)).take(1).foreach(println)
    println("The number of times the URL was called (DISTINCT): " + msgs_uniq_url_rdd.count())
    msgs_avg_rdd.map(t => "Average Response Time: " + t(0)).take(1).foreach(println)
    msgs_dyno_rdd.map(t => "Most Responded Dyno: web." + t(1) + " Total Response :" + t(0) + "\n\n").take(1).foreach(println)


    val friends_progress = sqlContext.sql("SELECT count(*) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_friends_progress'")
    val friends_progress_uniq_url_rdd = sqlContext.sql("SELECT DISTINCT(log_path) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_friends_progress'")
    val friends_progress_avg_rdd = sqlContext.sql("SELECT AVG(log_connect+log_service) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_friends_progress'")
    val friends_progress_rdd = sqlContext.sql("SELECT count(log_dyno) c, log_dyno FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_friends_progress' group by log_dyno order by c desc")


    friends_progress.map(t => "\nOverview for GET /api/users/{user_id}/get_friends_progress\n" +
      "============================================================\n\n\nTotal Number of log lines : " + t(0)).take(1).foreach(println)
    println("The number of times the URL was called (DISTINCT): " + friends_progress_uniq_url_rdd.count())
    friends_progress_avg_rdd.map(t => "Average Response Time: " + t(0)).take(1).foreach(println)
    friends_progress_rdd.map(t => "Most Responded Dyno: web." + t(1) + " Total Response :" + t(0) + "\n\n").take(1).foreach(println)


    val friends_score = sqlContext.sql("SELECT count(*) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_friends_score'")
    val friends_score_uniq_url_rdd = sqlContext.sql("SELECT DISTINCT(log_path) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_friends_score'")
    val friends_score_avg_rdd = sqlContext.sql("SELECT AVG(log_connect+log_service) FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_friends_score'")
    val friends_score_rdd = sqlContext.sql("SELECT count(log_dyno) c, log_dyno FROM logs_table WHERE log_method='GET' AND log_path LIKE '/api/users/%/get_friends_score' group by log_dyno order by c desc")


    friends_score.map(t => "\nOverview for GET /api/users/{user_id}/get_friends_score\n" +
      "============================================================\n\n\nTotal Number of log lines : " + t(0)).take(1).foreach(println)
    println("The number of times the URL was called (DISTINCT): " + friends_score_uniq_url_rdd.count())
    friends_score_avg_rdd.map(t => "Average Response Time: " + t(0)).take(1).foreach(println)
    friends_score_rdd.map(t => "Most Responded Dyno: web." + t(1) + " Total Response :" + t(0) + "\n\n").take(1).foreach(println)


    val get_user = sqlContext.sql("SELECT count(*) FROM logs_table WHERE log_method='GET' AND NOT (log_path RLIKE '/.*/.*/.*/') AND log_path LIKE '/api/users/%'")
    val get_user_uniq_url_rdd = sqlContext.sql("SELECT DISTINCT(log_path) FROM logs_table WHERE log_method='GET' AND NOT (log_path RLIKE '/.*/.*/.*/') AND log_path LIKE '/api/users/%'")
    val get_user_score_avg_rdd = sqlContext.sql("SELECT AVG(log_connect+log_service) FROM logs_table WHERE log_method='GET' AND NOT (log_path RLIKE '/.*/.*/.*/') AND log_path LIKE '/api/users/%'")
    val get_user_score_rdd = sqlContext.sql("SELECT count(log_dyno) c, log_dyno FROM logs_table WHERE log_method='GET' AND NOT (log_path RLIKE '/.*/.*/.*/') AND log_path LIKE '/api/users/%' group by log_dyno order by c desc")


    get_user.map(t => "\nOverview for GET /api/users/{user_id}\n" +
      "============================================================\n\n\nTotal Number of log lines : " + t(0)).take(1).foreach(println)
    println("The number of times the URL was called (DISTINCT): " + get_user_uniq_url_rdd.count())
    get_user_score_avg_rdd.map(t => "Average Response Time: " + t(0)).take(1).foreach(println)
    get_user_score_rdd.map(t => "Most Responded Dyno: web." + t(1) + " Total Response :" + t(0) + "\n\n").take(1).foreach(println)


    val post_user = sqlContext.sql("SELECT count(*) FROM logs_table WHERE log_method='POST' AND NOT (log_path RLIKE '/.*/.*/.*/') AND log_path LIKE '/api/users/%'")
    val post_user_uniq_url_rdd = sqlContext.sql("SELECT DISTINCT(log_path) FROM logs_table WHERE log_method='POST' AND NOT (log_path RLIKE '/.*/.*/.*/') AND log_path LIKE '/api/users/%'")
    val post_user_score_avg_rdd = sqlContext.sql("SELECT AVG(log_connect+log_service) FROM logs_table WHERE log_method='POST' AND NOT (log_path RLIKE '/.*/.*/.*/') AND log_path LIKE '/api/users/%'")
    val post_user_score_rdd = sqlContext.sql("SELECT count(log_dyno) c, log_dyno FROM logs_table WHERE log_method='POST' AND NOT (log_path RLIKE '/.*/.*/.*/') AND log_path LIKE '/api/users/%' group by log_dyno order by c desc")

    post_user.map(t => "\nOverview for POST /api/users/{user_id}\n" +
      "============================================================\n\n\nTotal Number of log lines : " + t(0)).take(1).foreach(println)
    println("The number of times the URL was called (DISTINCT): " + post_user_uniq_url_rdd.count())
    post_user_score_avg_rdd.map(t => "Average Response Time: " + t(0)).take(1).foreach(println)
    post_user_score_rdd.map(t => "Most Responded Dyno: web." + t(1) + " Total Response :" + t(0) + "\n\n").take(1).foreach(println)

  }

}
