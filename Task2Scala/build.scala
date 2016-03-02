//import AssemblyKeys._ 


//seq(assemblySettings: _*)

name := "Task2Scala"

version := "1.0"

scalaVersion := "2.10.4"

unmanagedJars in Compile += file("lib/refinedata")



libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "1.4.1" % "provided", 
"com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.4.1",
"com.datastax.cassandra" % "cassandra-driver-core" % "2.1.7.1",
"org.apache.cassandra" % "cassandra-clientutil" % "2.1.9",
"org.apache.cassandra" % "cassandra-thrift" % "2.0.1",
"com.google.guava" % "guava" % "14.0.1",
"org.joda" % "joda-convert" % "1.2",
"joda-time" % "joda-time" % "2.0", 
"org.apache.kafka" % "kafka_2.10" % "0.8.0",
"org.apache.kafka" % "kafka-clients" % "0.8.2.1",
"org.apache.thrift" % "libthrift" % "0.9.1"
//exclude("javax.jms", "jms") 
//exclude("com.sun.jdmk", "jmxtools") 
//exclude("com.sun.jmx", "jmxri")
//excludeAll ExclusionRule(organization = "io.netty")
).map(_.exclude("com.sun.jmx", "jmxri"))




