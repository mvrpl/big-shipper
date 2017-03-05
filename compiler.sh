#!/bin/bash

if ! type sbt &> /dev/null; then
	echo "Install SBT"
	exit 1
fi

sparkVer=$(spark-submit --version 2>&1 | awk '{if(match($0, /version ([0-9\.]+$)/, v)){print v[1]}}')
if [ $? -ne 0 ];then echo "Check GAWK installed in your system!";exit 1;fi

cat <<EOF > script.scala
util.Properties.versionString
System.exit(0)
EOF

scalaVer=$(spark-shell -i script.scala 2>&1 | awk '{if(match($0,/^res0.*version ([0-9\.]+)/,s)){print s[1]}}')

bigShipperVer="0.1"

rm -f script.scala

echo "Creating build.sbt with Spark version ${sparkVer} and Scala version ${scalaVer}"

cat <<EOF > build.sbt
name := "BigShipper"

version := "$bigShipperVer"

scalaVersion := "$scalaVer"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"
libraryDependencies += "org.tsers.zeison" %% "zeison" % "0.7.0"
libraryDependencies += "log4j" % "log4j" % "1.2.14"
libraryDependencies += "org.apache.spark" %% "spark-core" % "$sparkVer" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "$sparkVer" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "$sparkVer" % "provided"
EOF

echo "JAR making..."

sbt assembly
if [ $? -eq 0 ];then
	echo -e "\nJAR compiled.\nRun example: spark-submit --class main.Shipper target/scala-${scalaVer%.*}/BigShipper-assembly-${bigShipperVer}.jar -c /path/config.json --loglevel error"
else
	echo "Failed on build .jar"
fi
