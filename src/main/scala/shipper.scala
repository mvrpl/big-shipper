package shipper

import org.tsers.zeison.Zeison
import utils.{Utils, Spark, Logs}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class Loader extends Logs {

	val utils = new Utils
	val spark = new Spark

	def delimitedFiles(configs: Zeison.JValue) : Boolean = {
		val fields = configs.SOURCE.FIELDS.map(f => "%s:%s".format(f.NAME.toStr, f.TYPE.toStr.toLowerCase)).mkString(",")
		val cSchema = utils.makeSchema(fields)
		val dirRawFiles = configs.SOURCE.DIR_RAW_FILES.toStr
		val delimiter = configs.SOURCE.DELIMITER_RAW.toStr
		var headerFile = ""
		if (configs.SOURCE.HEADER.isBool) {
			headerFile = configs.SOURCE.HEADER.toBool.toString
		} else {
			error("The option SOURCE.HEADER need a boolean type.")
			System.exit(1)
		}
		val dataFrame = spark.hiveContext.read.format("com.databricks.spark.csv").option("header", headerFile).option("delimiter", delimiter).schema(cSchema).load(dirRawFiles)
		if (configs.TARGET.ACTION.toStr.toLowerCase == "update"){
			val targetTable = configs.TARGET.HIVE_TABLE.toStr
			val targetDF = spark.hiveContext.sql(s"select * from $targetTable").toDF
			val updateDF = spark.updateDF(targetDF, dataFrame, configs.TARGET.UPDATEKEY.toStr)
			spark.writeDFInTarget(updateDF, configs)
		} else {
			spark.writeDFInTarget(dataFrame, configs)
		}
		return true
	}

	def jsonFiles(configs: Zeison.JValue) : Boolean = {
		val sourceSchema = configs.SOURCE.SCHEMA.toStr
		val jsonFiles = configs.SOURCE.DIR_RAW_FILES.toStr
		val schemaSeq = spark.sc.parallelize(Seq(sourceSchema))
		val schema = spark.hiveContext.read.json(schemaSeq).schema
		val dataFrame = spark.hiveContext.jsonFile(jsonFiles, schema)
		if (configs.TARGET.ACTION.toStr.toLowerCase == "update"){
			val targetTable = configs.TARGET.HIVE_TABLE.toStr
			val targetDF = spark.hiveContext.sql(s"select * from $targetTable").toDF
			val updateDF = spark.updateDF(targetDF, dataFrame, configs.TARGET.UPDATEKEY.toStr)
			spark.writeDFInTarget(updateDF, configs)
		} else {
			spark.writeDFInTarget(dataFrame, configs)
		}
		return true
	}
}
