package beyondthewall.spark

import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.runtime.ScalaRunTime._
import org.apache.spark.SparkContext._

case class KeyValue(key: String, value: Int)

object LocalSpark {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val array = Seq(
      new KeyValue("user1", 19),
      new KeyValue("user2", 29),
      new KeyValue("user1", 45),
      new KeyValue("user3", 229),
      new KeyValue("user2", 9));
    val myKVRdd: RDD[KeyValue] = sc.parallelize(array, 5)
    val multipliedby5 = myKVRdd.map(t => {
      (t.key, t.value * 5)
    })
    val groupByKey = multipliedby5.reduceByKey(_ + _)
    val collectedRDD = groupByKey.collect
    println("New Array is " + stringOf(collectedRDD))
  }

}
