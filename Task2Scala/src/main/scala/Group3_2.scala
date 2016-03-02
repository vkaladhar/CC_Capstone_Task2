import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import capstone.Task2.RefineData
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraRDD


class Group3_2(sc: SparkContext) {
  
    def refineData(t: String) : RDD[(Any)] = {
      
       val values1 =sc.textFile(t,1).repartition(1).flatMap { x => x.toString().split("\n").filterNot { x => x.contains("null") } }.map { 
        x => val values = x.split(",")
            (values(3),values(4).replaceAll("\"","")+"-"+values(5).replaceAll("\"",""),
                values(1).replaceAll("\"","")+"-"+values(2).replaceAll("\"",""),
                values(6).replaceAll("\"","").toInt,
                  values(8).replaceAll("\"","").toInt, values(7).toDouble, values(9).toDouble)
            
                      
      }
      
       return values1.filter(_._6 >= -30).filter(_._7 >= -30).filter(_._6 <= 30).filter(_._7 <= 30)
               .map{
                   case (a,b,c,d,e,f,g) => a+","+b+","+c+","+d+","+e+","+f+","+g
                     
               }
  
   }
   
    def getDataInTuple(t: RDD[String]) : RDD[(String,String,String,Int,Double,Int,Double)] = {
      
       val values1 =t.flatMap { x => x.toString().split("\n")}.map { 
        x => val values = x.split(",")
            (values(3),values(4).replaceAll("\"","")+"-"+values(5).replaceAll("\"",""),
                values(1).replaceAll("\"","")+"-"+values(2).replaceAll("\"",""),
                values(6).replaceAll("\"","").toInt,
                  values(7).toDouble, values(8).replaceAll("\"","").toInt, values(9).toDouble)
              
      }
      
       return values1
   } 
 
}  
  
object Group3_2 {
    def main(args: Array[String]) {
    
    var winOpath = "C:/MyCourses/CloudComputing/capstone/data/Output/";
    var unixOpath =  "file:/usr/local/spark/data/Output/Scala/";
    
    var winIpath = "C:/MyCourses/CloudComputing/capstone/data/CleanedData/files";
    var unixIpath =  "file:/usr/local/spark/data/Input-small";
  
    val conf = new SparkConf()
              .setAppName("RefineData")
              .setMaster("local")
              .set("spark.cassandra.connection.host", "172.31.28.69")
    val context = new SparkContext(conf)
    val job = new Group3_2(context)
    
    val refinedData = job.getDataInTuple(RefineData.run(context, unixIpath))
    
    //val refinedData = refinedData.refineData("C:\\MyCourses\\CloudComputing\\capstone\\data\\CleanedData\\files")
    refinedData.saveAsTextFile(unixOpath+"Group3_2")
    refinedData.saveToCassandra("capstone", "group3_2", SomeColumns("flightdate", "airports", 
                        "airlines", "deptime", "depdelayminutes", "arrtime", "arrdelayminutes"))
     
    context.stop()
  }
}
  
