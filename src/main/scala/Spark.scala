package main.scala
import org.apache._
import org.apache.spark.rdd.RDD
/**
  * Created by Louis on 02/12/2015.
  */
 object spark {
  def main (args: Array[String]) = {

     val conf new SparkConf().setAppName("Assignement 3")
     val sc = new SparkContext(conf)



  }
}
