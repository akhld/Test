import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by akhld on 10/10/14.
 */
object TestMain {

  def main(args: Array[String]): Unit ={

    //Create SparkContext
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Sigmoid Spark")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction","1")

    val sc = new SparkContext(conf)

    val data = sc.parallelize(1 to 10000000).collect().filter(_<1000)
    data.foreach(println)

  }


}
