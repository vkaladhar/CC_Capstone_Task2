
package main.scala

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._


object KafkaFileProducer {
  
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaFileProducer <metadataBrokerList> <topic> " +
        "<filename> <linesMessage>")
      System.exit(1)
    }
    
    val Array(brokers, topic, filename, linesPerMessage) = args

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    //read portions of file and send as messages
    
    
  }

  
}