name := "Big Shipper"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"
libraryDependencies += "org.tsers.zeison" %% "zeison" % "0.7.0"
libraryDependencies += "log4j" % "log4j" % "1.2.14"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0" % "provided"
