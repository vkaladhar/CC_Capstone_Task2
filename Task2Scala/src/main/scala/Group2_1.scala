
package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import scala.BigDecimal
import scala.reflect.runtime.universe
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.SomeColumns

object Group2_1 {
  
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: Group2_1 <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("Group2_1")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines.print()
    
     val values1 = lines.map { x => x.toString().split("\n")
                 val values = x.split(",") 
                 (values(1)+"-"+values(4),(1,values(7).toDouble))   
              }
      values1.print()
    
    val values1R = values1.reduceByKey{(v1,v2)=> (v1._1+v2._1, v1._2+v2._2) };
    values1R.print()
      
    val value3 = values1R.map{ case (x, (y,z)) => (x,
                                           BigDecimal(z/y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}
                                          //.sortBy(_._1, true, 1)
    value3.print();
    val value4 = value3.map{case (x,y) => (x.split("-")(1),x.split("-")(0),y)}
    
    value4.print();
            
    value4.saveToCassandra("capstone", "group2_1", SomeColumns("orig_airport", "carrier", "cnt"))
    
    ssc.start()
    ssc.awaitTermination()
  }
  
}

object Group2_2 {
  
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: Group2_2 <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("Group2_2")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines.print()
        
     val values1 = lines.map { x => x.toString().split("\n")
                 val values = x.split(",") 
                 (values(4)+"-"+values(5),(1,values(7).toDouble))   
              }
      values1.print()
    
    val values1R = values1.reduceByKey{(v1,v2)=> (v1._1+v2._1, v1._2+v2._2) };
    values1R.print()
      
    val value3 = values1R.map{ case (x, (y,z)) => (x,
                                           BigDecimal(z/y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}
                                          //.sortBy(_._1, true, 1)
    value3.print();
    val value4 = value3.map{case (x,y) => (x.split("-")(1),x.split("-")(0),y)}
    
    value4.print();
            
    value4.saveToCassandra("capstone", "group2_2", SomeColumns("orig_airport", "dest_airport", "cnt"))
    
    ssc.start()
    ssc.awaitTermination()
  }
}
  
object Group2_3 {
  
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: Group2_3 <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("Group2_3")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines.print()
        
     val values1 = lines.map { x => x.toString().split("\n")
                 val values = x.split(",") 
                 (values(4)+"-"+values(5)+"-"+values(1),(1,values(7).toDouble))   
              }
      values1.print()
    
    val values1R = values1.reduceByKey{(v1,v2)=> (v1._1+v2._1, v1._2+v2._2) };
      
    val value3 = values1R.map{ case (x, (y,z)) => (x,
                                           BigDecimal(z/y).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}
                                          //.sortBy(_._1, true, 1);
             
    val value4 = value3.map{case (x,y) => (x.split("-")(0)+"-"+x.split("-")(1),x.split("-")(2),y)};
    
    value4.saveToCassandra("capstone", "group2_3", SomeColumns("airportx_y", "carrier", "cnt"))
    
    ssc.start()
    ssc.awaitTermination()
  }
  
}