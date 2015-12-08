import org.apache._
import org.apache.spark.rdd
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql.SQLContext._
/**
  * Created by Louis on 02/12/2015.
  */
 object spark {
  def main (args: Array[String]) = {
	 val conf = new SparkConf().setMaster("local").setAppName("My app")
         val sc = new SparkContext(conf)
	 val sqlContext = new org.apache.spark.sql.SQLContext(sc)
         import sqlContext.implicits._
	 
         
	 case class CrimeAverage(name: String, value: Float)


         val file = sc.textFile("SacramentocrimeJanuary2006.csv").mapPartitionsWithIndex{ (idx, iter) => if(idx == 0) iter.drop(1) else iter}
	 val crimeTypes = file.map (line => { 
						val l = line.split(",")
						l(5)
					})
        val crimeDate = file.map(line => {
						val l = line.split(",")
						l(0).substring(0,9)
				})
        val threeHighestCrimeDays = crimeDate.groupBy(w => w).mapValues(_.size).takeOrdered(3)(Ordering[Int].reverse.on(_._2))
    	val mostRecurrentCrime  = crimeTypes.groupBy(w => w).mapValues(_.size).takeOrdered(1)(Ordering[Int].reverse.on(_._2))
	val crimeAveragePerDay = crimeTypes.groupBy(w => w).mapValues(_.size).map(item => CrimeAverage(item._1,(item._2.toFloat)/31)).sortBy(_.value)
	       

     
	println("Most recurrent crime is :")
	mostRecurrentCrime.foreach(println)
	println("The three days with highest crime rate are:")
    threeHighestCrimeDays.foreach(println)
	println("Crime average per day")
	crimeAveragePerDay.foreach(println)

	val crimeTypesDF = crimeTypes.toDF
	val crimeDateDF = crimeDate.toDF

    val mostReccurentCrimeDF = crimeTypesDF.groupBy('_1).count().sort('count.desc).first
    val threeHighestCrimeDaysDF = crimeDateDF.groupBy('_1).count().sort('count.desc).take(3)
    val crimeAveragePerDayDF = crimeTypesDF.groupBy('_1).count().select('_1,'count /31f)


    println("Most recurrent crime is :")
    println(mostReccurentCrimeDF)
    println("The three days with highest crime rate are:")
    threeHighestCrimeDaysDF.foreach(println)
    println("Crime average per day")
    crimeAveragePerDayDF.foreach(println)
    
  }
}

