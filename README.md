[![Issue Count](https://lima.codeclimate.com/github/mvrpl/big-shipper/badges/issue_count.svg)](https://lima.codeclimate.com/github/mvrpl/big-shipper)
[![Build Status](https://travis-ci.org/mvrpl/big-shipper.svg?branch=master)](https://travis-ci.org/mvrpl/big-shipper)

# Big Shipper
Spark ingestion tool for Big Data written in Scala

## Mandatory
Installed build tool: [SBT](http://www.scala-sbt.org)

Spark
## Beta version
0.1 - under development
## Compilation
Run script compiler.sh
```sh
cd big-shipper
./compiler.sh
```
This script generate **target/scala-[version, like: 2.10]/BigShipper-assembly-0.1.jar** file with dependencies embedded.
## Usage
```sh
spark-submit --class main.Shipper target/scala-[version, like: 2.10]/BigShipper-assembly-0.1.jar -c /path_to/config.json --loglevel debug
```
####Config example:
```json
{
	"SOURCE":{
		"TYPE": "delimitedfile",
		"FIELDS": [
			{
				"NAME": "field1",
				"TYPE": "int"
			},
			{
				"NAME": "field2",
				"TYPE": "string"
			},
			{
				"NAME": "field3",
				"TYPE": "decimal"
			}
		],
		"DELIMITER_RAW": "|",
		"DIR_RAW_FILES": "/user/NAME/data_201703{2[7-9],3[0-1]}.txt"
	},
	"TARGET":{
		"TYPE": "hive",
		"HIVE_TABLE": "table_name_here"
	}
}
```
***SOURCE.TYPE:*** Only delimited file implemented. Values: [delimitedfile]

***SOURCE.FIELDS.TYPE:*** Data types for fields. Values: [bigint, int, smallint, tinyint, double, decimal, float, byte,  string, date, timestamp and boolean]

***SOURCE.DIR_RAW_FILES:*** HDFS path with REGEX pattern to grab files or local path started with: [file://].
## License
MIT License
