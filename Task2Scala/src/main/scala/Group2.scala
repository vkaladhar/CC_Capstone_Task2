package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraRDD

object TopAirports {
     val conf = new SparkConf(true)
    .setAppName("RefineData")
    .setMaster("local")
    .set("spark.cassandra.connection.host", "172.31.28.69")
    val sc = new SparkContext(conf)
    var unixIpath = "/data/spark/Input";
    var unixOpath = "/data/spark/Output/"; 
     
  
  def main(args: Array[String]) {
    val data = refineData(unixIpath)
     val values = data.flatMap { x => x.toString().split("\n") }.map { 
        x => val values = x.toString().split(",")
            (values(4),1)
            (values(5),1)
          
      }
       //return values.reduceByKey((v1,v2)=> v1+v2).sortBy(_._2, false, 1).map{case (x,y) => x+","+y.toString()}
       
     val topAirports = sc.parallelize(values.reduceByKey((v1,v2)=> v1+v2).sortBy(_._2, false, 1).map(x => x.swap).top(10))
     topAirports.saveAsTextFile(unixOpath+"Group1_1")              
    
  }
  
  def refineData(t: String) : RDD[(String)] = {
      val values = sc.textFile(t,1).repartition(1).flatMap { x => x.split("\n")}
                
       return values;             
      }
}

object TopAirlines {
     val conf = new SparkConf(true)
    .setAppName("RefineData")
    .setMaster("local")
    .set("spark.cassandra.connection.host", "172.31.28.69")
    val sc = new SparkContext(conf)
    var unixIpath = "/data/spark/Input";
    var unixOpath = "/data/spark/Output"; 
     
  
  def main(args: Array[String]) {
    val data = refineData(unixIpath)
     val values1 = data.flatMap { x => x.toString().split("\n") }.map { 
        x => val values = x.toString().split(",")
            (values(1),1)
                      
      }
      val values1R = values1.reduceByKey((v1,v2)=> v1+v2);
     
      val values2 = data.flatMap { x => x.toString().split("\n") }.map { 
      
        x => val values = x.toString().split(",")
            (values(1),values(9).toDouble)
      }
      val values2R = values2.reduceByKey((v1,v2)=> v1+v2);
      val value3 = values1R.join(values2R).map{ case (x, (y,z)) => (x,BigDecimal(z/y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}
                                          .sortBy(_._2, true, 1);
      //return value3.map{case (x,y) => x+","+y.toString()};
     sc.parallelize(value3.map(x => x.swap).top(10).reverse).saveAsTextFile(unixOpath+"Group1_2") 
     
  }
  
  def refineData(t: String) : RDD[(String)] = {
      val values = sc.textFile(t,1).repartition(1).flatMap { x => x.split("\n")}
                
       return values;             
      }
}

object AirlinesRanking {
     val conf = new SparkConf(true)
    .setAppName("RefineData")
    .setMaster("local")
    .set("spark.cassandra.connection.host", "172.31.28.69")
    val sc = new SparkContext(conf)
    var unixIpath = "/data/spark/Input";
    var unixOpath = "/data/spark/Output"; 
     
  
  def main(args: Array[String]) {
    val data = refineData(unixIpath)
     val values1 = data.flatMap { x => x.toString().split("\n") }.map {
     //val values1 = t.flatMap { x => x.toString().split("\n").map {   
           x => val values = x.split(",")
            (values(1)+"-"+values(4),(1,values(7).toDouble))
             
      }
      //BigDecimal(v1._2+v2._2/v1._1+v2._1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
      val values1R = values1.reduceByKey{(v1,v2)=> (v1._1+v2._1, v1._2+v2._2) };
      
      val value3 = values1R.map{ case (x, (y,z)) => (x,
                                           BigDecimal(z/y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}
                                          .sortBy(_._1, true, 1);
             
      value3.map{case (x,y) => (x.split("-")(1),x.split("-")(0),y)}
        .saveToCassandra("capstone", "group2_1", SomeColumns("orig_airport", "carrier", "cnt"))
     
  }
  
  def refineData(t: String) : RDD[(String)] = {
      val values = sc.textFile(t,1).repartition(1).flatMap { x => x.split("\n")}
                
       return values;             
      }
}


object AirportsRanking {
     val conf = new SparkConf(true)
    .setAppName("RefineData")
    .setMaster("local")
    .set("spark.cassandra.connection.host", "172.31.28.69")
    val sc = new SparkContext(conf)
    var unixIpath = "/data/spark/Input";
    var unixOpath = "/data/spark/Output"; 
     
  
  def main(args: Array[String]) {
    val data = refineData(unixIpath)
    val values1 = data.flatMap { x => x.toString().split("\n") }.map {
     x => val values = x.split(",")
            (values(4)+"-"+values(5),(1,values(7).toDouble))
            
                      
      }
      //BigDecimal(v1._2+v2._2/v1._1+v2._1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
      val values1R = values1.reduceByKey{(v1,v2)=> (v1._1+v2._1, v1._2+v2._2) };
      
      val value3 = values1R.map{ case (x, (y,z)) => (x,
                                           BigDecimal(z/y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}
                                          .sortBy(_._1, true, 1);
             
      value3.map{case (x,y) => (x.split("-")(1),x.split("-")(0),y)}
        .saveToCassandra("capstone", "group2_2", SomeColumns("orig_airport", "dest_airport", "cnt"))
     
  }
  
  def refineData(t: String) : RDD[(String)] = {
      val values = sc.textFile(t,1).repartition(1).flatMap { x => x.split("\n")}
                
       return values;             
      }
}

object AirlinesXYRanking {
     val conf = new SparkConf(true)
    .setAppName("RefineData")
    .setMaster("local")
    .set("spark.cassandra.connection.host", "172.31.28.69")
    val sc = new SparkContext(conf)
    var unixIpath = "/data/spark/Input";
    var unixOpath = "/data/spark/Output"; 
     
  
  def main(args: Array[String]) {
    val data = refineData(unixIpath)
    val values1 = data.flatMap { x => x.toString().split("\n") }.map { 
        x => val values = x.split(",")
            (values(4)+"-"+values(5)+"-"+values(1),(1,values(7).toDouble))
            
                      
      }
      //BigDecimal(v1._2+v2._2/v1._1+v2._1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
      val values1R = values1.reduceByKey{(v1,v2)=> (v1._1+v2._1, v1._2+v2._2) };
      
      val value3 = values1R.map{ case (x, (y,z)) => (x,
                                           BigDecimal(z/y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}
                                          .sortBy(_._1, true, 1);
             
     value3.map{case (x,y) => (x.split("-")(0)+"-"+x.split("-")(1),x.split("-")(2),y)}
       .saveToCassandra("capstone", "group2_3", SomeColumns("airportx_y", "carrier", "cnt"))
     
  }
  
  def refineData(t: String) : RDD[(String)] = {
      val values = sc.textFile(t,1).repartition(1).flatMap { x => x.split("\n")}
                
       return values;             
      }
}

object Group3_2 {
     val conf = new SparkConf(true)
    .setAppName("RefineData")
    .setMaster("local")
    .set("spark.cassandra.connection.host", "172.31.28.69")
    val sc = new SparkContext(conf)
    var unixIpath = "/data/spark/Input/2008";
    var unixOpath = "/data/spark/Output/"; 
     
  
  def main(args: Array[String]) {
    val data = refineData("/data/spark/Input/2008")
    val values1 =data.flatMap { x => x.toString().split("\n")}.map { 
        x => val values = x.split(",")
            (values(3),values(4).replaceAll("\"","")+"-"+values(5).replaceAll("\"",""),
                values(1).replaceAll("\"","")+"-"+values(2).replaceAll("\"",""),
                values(6).replaceAll("\"","").toInt,
                  values(7).toDouble, values(8).replaceAll("\"","").toInt, values(9).toDouble)
              
      }
    values1.saveToCassandra("capstone", "group3_2", SomeColumns("flightdate", "airports", 
                        "airlines", "deptime", "depdelayminutes", "arrtime", "arrdelayminutes"))
     
     
  }
  
  def refineData(t: String) : RDD[(String)] = {
      val values = sc.textFile(t,1).repartition(1).flatMap { x => x.split("\n")}
                
       return values;             
      }
}