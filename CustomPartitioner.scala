package sparkUtilityCodes
import org.apache.spark.Partitioner

class CustomPartitioner(override val numPartitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = key match {
		case s: String => {
			if (s(0).toUpper == 'A')  1 
			else if (s(0).toUpper == 'B')  2
			else if (s(0).toUpper == 'C')  3
			else if (s(0).toUpper == 'D')  4
			else if (s(0).toUpper == 'E') 5
			else if (s(0).toUpper == 'F')  6
			else if (s(0).toUpper == 'G')  7
			else if (s(0).toUpper == 'H')  8
			else if (s(0).toUpper == 'I')  9
			else if (s(0).toUpper == 'J')  10
			else if (s(0).toUpper == 'K')  11
			else if (s(0).toUpper == 'L')  12
			else if (s(0).toUpper == 'M')  13
			else if (s(0).toUpper == 'N')  14
			else if (s(0).toUpper == 'O')  15
			else if (s(0).toUpper == 'P')  16
			else if (s(0).toUpper == 'Q')  17
			else if (s(0).toUpper == 'R')  18
			else if (s(0).toUpper == 'S')  19
			else if (s(0).toUpper == 'T')  20
			else if (s(0).toUpper == 'U')  21
			else if (s(0).toUpper == 'V')  22
			else if (s(0).toUpper == 'W')  23
			else if (s(0).toUpper == 'X')  24
			else if (s(0).toUpper == 'Y')  25
			else  26
		}   
	}
}
//usage example
def createSparkSession():SparkSession=
  {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate;
    
    spark
  }
  
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.types.IntegerType

object readDataFromOnePartition {
  def main(args: Array[String]) {
   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("akka").setLevel(Level.OFF)
   val sparkObj= createSparkSession()
   val spark=sparkObj.createSparkSession()
   import spark.implicits._


   
   case class entity(name:String,km_travel:Int,date:Int)
   val data=spark.read.textFile("distanceTravelledData.txt").map(f => f.split(',')).map{case Array(a,b,c) => (a,b.toInt,c)}.toDF("name","km_travel","date")//.rdd.flatMap(x => x.split(","))//.map(f=>f.toInt) 
   val dat=data.repartitionByRange(26,$"name").rdd.mapPartitionsWithIndex((i,rows) => Iterator((i,rows.size))).toDF("partition_number","number_of_records").show
   
  }
