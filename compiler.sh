#!/bin/bash

ask() {
	# http://djm.me/ask
	local prompt default REPLY

	while true; do

		if [ "${2:-}" = "Y" ]; then
			prompt="Y/n"
			default=Y
		elif [ "${2:-}" = "N" ]; then
			prompt="y/N"
			default=N
		else
			prompt="y/n"
			default=
		fi

		# Ask the question (not using "read -p" as it uses stderr not stdout)
		echo -n "$1 [$prompt] "

		# Read the answer (use /dev/tty in case stdin is redirected from somewhere else)
		read REPLY </dev/tty

		# Default?
		if [ -z "$REPLY" ]; then
			REPLY=$default
		fi

		# Check if the reply is valid
		case "$REPLY" in
			Y*|y*) return 0 ;;
			N*|n*) return 1 ;;
		esac

	done
}

if ! type sbt &> /dev/null; then
	echo "Install SBT"
	exit 1
fi

cat <<EOF > script.scala
util.Properties.versionString.replace("version ", "")
"^[0-9]+\\\.[0-9]+\\\.[0-9]+".r.findFirstIn(sc.version).get
System.exit(0)
EOF

spark-shell -i script.scala 2> /dev/null | awk '{if($0 ~ /^res0/){split($0, ver, /= /);print ver[2]};if($0 ~ /^res1/){split($0, ver, /= /);print ver[2]}}' > versions.txt
mapfile -t ver < versions.txt

bigShipperVer="0.1"
scalaVer=${ver[0]}
sparkVer=${ver[1]}

rm -f script.scala versions.txt

echo "Creating build.sbt with Spark version ${sparkVer:?Error while getting spark version} and Scala version ${scalaVer:?Error while getting scala version}"

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
libraryDependencies += "com.databricks" % "spark-csv_${scalaVer%.*}" % "1.5.0"
libraryDependencies += "org.scalatest" % "scalatest_${scalaVer%.*}" % "1.9.1" % "test"
EOF

echo "JAR making..."

sbt 'set test in assembly := {}' clean assembly
if [ $? -eq 0 ];then
	echo -e "\nJAR compiled.\nRun example: spark-submit --class main.Shipper target/scala-${scalaVer%.*}/BigShipper-assembly-${bigShipperVer}.jar -c /path/config.json --loglevel error\n"
else
	echo "Failed on build .jar"
	exit 1
fi

if ask "Analyze Scala code?"; then
	sbt scalastyle
else
	exit 0
fi
