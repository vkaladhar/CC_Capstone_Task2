//import AssemblyKeys._ 


//seq(assemblySettings: _*)

name := "Task2Scala"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "1.4.1" % "provided", 
"com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.4.1",
"com.datastax.cassandra" % "cassandra-driver-core" % "2.1.7.1",
"org.apache.cassandra" % "cassandra-clientutil" % "2.1.9",
"org.apache.cassandra" % "cassandra-thrift" % "2.0.1",
"com.google.guava" % "guava" % "14.0.1",
"org.joda" % "joda-convert" % "1.2",
"joda-time" % "joda-time" % "2.0", 
"org.apache.thrift" % "libthrift" % "0.9.1"
excludeAll ExclusionRule(organization = "io.netty")
)




