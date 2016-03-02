import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraRDD

//import capstone.Task2.RefineData;


class Group1_2(sc: SparkContext) {
  
  
   def topAirports(t: RDD[String]) : RDD[(Int,String)] = {
    
      val values = t.flatMap { x => x.toString().split("\n") }.map { 
        x => val values = x.toString().split(",")
            (values(4),1)
            (values(5),1)
          
      }
       //return values.reduceByKey((v1,v2)=> v1+v2).sortBy(_._2, false, 1).map{case (x,y) => x+","+y.toString()}
       
     return sc.parallelize(values.reduceByKey((v1,v2)=> v1+v2).sortBy(_._2, false, 1).map(x => x.swap).top(10))
                   
    } 
   
   def topAirlines(t: RDD[String]) : RDD[(Double,String)] = {
      val values1 = t.flatMap { x => x.toString().split("\n") }.map { 
        x => val values = x.toString().split(",")
            (values(1),1)
                      
      }
      val values1R = values1.reduceByKey((v1,v2)=> v1+v2);
      //val values2 = t.flatMap { x => x.toString().split("\n").filterNot { x => x.contains("null") } }.map { 
      
      val values2 = t.flatMap { x => x.toString().split("\n") }.map { 
      //val values2 = t.flatMap { x => x.toString().split("\n").map { 
        x => val values = x.toString().split(",")
            (values(1),values(9).toDouble)
      }
      val values2R = values2.reduceByKey((v1,v2)=> v1+v2);
      val value3 = values1R.join(values2R).map{ case (x, (y,z)) => (x,BigDecimal(z/y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}
                                          .sortBy(_._2, true, 1);
      //return value3.map{case (x,y) => x+","+y.toString()};
      return sc.parallelize(value3.map(x => x.swap).top(10).reverse);                                    
  
    } 
   
   def AirlinesRanking(t: RDD[String]) : RDD[(String,String,Double)] = {
      
     //val values1 = t.flatMap { x => x.toString().split("\n").filterNot { x => x.contains("null") } }.map { 
     val values1 = t.flatMap { x => x.toString().split("\n") }.map {
     //val values1 = t.flatMap { x => x.toString().split("\n").map {   
           x => val values = x.split(",")
            (values(1)+"-"+values(4),(1,values(7).toDouble))
             
      }
      //BigDecimal(v1._2+v2._2/v1._1+v2._1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
      val values1R = values1.reduceByKey{(v1,v2)=> (v1._1+v2._1, v1._2+v2._2) };
      
      val value3 = values1R.map{ case (x, (y,z)) => (x,
                                           BigDecimal(z/y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}
                                          .sortBy(_._1, true, 1);
             
      return value3.map{case (x,y) => (x.split("-")(1),x.split("-")(0),y)};
      
      //return value3.map(x => (x.));
  
    }
   
   def AirportRanking(t: RDD[String]) : RDD[(String,String,Double)] = {
      
     //val values1 = t.flatMap { x => x.toString().split("\n").filterNot { x => x.contains("null") } }.map {
     val values1 = t.flatMap { x => x.toString().split("\n") }.map {
        x => val values = x.split(",")
            (values(4)+"-"+values(5),(1,values(7).toDouble))
            
                      
      }
      //BigDecimal(v1._2+v2._2/v1._1+v2._1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
      val values1R = values1.reduceByKey{(v1,v2)=> (v1._1+v2._1, v1._2+v2._2) };
      
      val value3 = values1R.map{ case (x, (y,z)) => (x,
                                           BigDecimal(z/y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}
                                          .sortBy(_._1, true, 1);
             
      return value3.map{case (x,y) => (x.split("-")(1),x.split("-")(0),y)};
  
    } 
   
   def AirlinesXYRanking(t: RDD[String]) : RDD[(String,String,Double)] = {
      
     val values1 = t.flatMap { x => x.toString().split("\n") }.map { 
        x => val values = x.split(",")
            (values(4)+"-"+values(5)+"-"+values(1),(1,values(7).toDouble))
            
                      
      }
      //BigDecimal(v1._2+v2._2/v1._1+v2._1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
      val values1R = values1.reduceByKey{(v1,v2)=> (v1._1+v2._1, v1._2+v2._2) };
      
      val value3 = values1R.map{ case (x, (y,z)) => (x,
                                           BigDecimal(z/y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}
                                          .sortBy(_._1, true, 1);
             
      return value3.map{case (x,y) => (x.split("-")(0)+"-"+x.split("-")(1),x.split("-")(2),y)};
  
    } 
   
   def refineData(t: String) : RDD[(String)] = {
      val values = sc.textFile(t,1).repartition(1).flatMap { x => x.split("\n")}
                
       return values;             
      }
     
  
 
   
}  
  
object Group1_2 {
    def main(args: Array[String]) {
  
    val conf = new SparkConf(true)
    .setAppName("RefineData")
    .setMaster("local")
    .set("spark.cassandra.connection.host", "172.31.28.69")
    
    var winOpath = "C:/MyCourses/CloudComputing/capstone/data/Output/";
    var unixOpath =  "file:/usr/local/spark/data/Output/Scala/";
    
    var winIpath = "C:/MyCourses/CloudComputing/capstone/data/CleanedData/files";
    var unixIpath =  "/data/spark/Input";
    
    val sc = new SparkContext(conf)
    val job = new Group1_2(sc)
    //val refinedData = RefineData.run(sc, unixIpath)
    
    //val refinedData = job.refineData2("C:/MyCourses/CloudComputing/capstone/data/CleanedData/files")
    val refinedData = job.refineData(unixIpath)
    refinedData.saveAsTextFile(unixOpath+"RefinedData")
    
        
    val topAirports = job.topAirports(refinedData);
    topAirports.saveAsTextFile(unixOpath+"Group1_1")
    topAirports.saveToCassandra("capstone", "group1_1", SomeColumns("airport_code", "cnt"))
    
    
    val topAirlines = job.topAirlines(refinedData);
    topAirlines.saveAsTextFile(unixOpath+"Group1_2")
    
    val AirlinesRanking = job.AirlinesRanking(refinedData);
    AirlinesRanking.saveAsTextFile(unixOpath+"Group2_1")
    AirlinesRanking.saveToCassandra("capstone", "group2_1", SomeColumns("orig_airport", "carrier", "cnt"))
    
    val AirportRanking = job.AirportRanking(refinedData);
    AirportRanking.saveAsTextFile(unixOpath+"Group2_2")
    AirportRanking.saveToCassandra("capstone", "group2_2", SomeColumns("orig_airport", "dest_airport", "cnt"))
    
    val AirlinesXYRanking = job.AirlinesXYRanking(refinedData);
    AirlinesXYRanking.saveAsTextFile(unixOpath+"Group2_3")
    AirlinesXYRanking.saveToCassandra("capstone", "group2_3", SomeColumns("airportx_y", "carrier", "cnt"))
    
    
    sc.stop()
  }
}
  
