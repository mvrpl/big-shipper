package shipper

import org.tsers.zeison.Zeison
import utils.{Utils, Spark}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class Loader {

	val utils = new Utils
	val spark = new Spark

	def delimitedFiles(configs: Zeison.JValue) : Boolean = {
		val fields = configs.SOURCE.FIELDS.map(f => "%s:%s".format(f.NAME.toStr, f.TYPE.toStr.toLowerCase)).mkString(",")
		val schema = utils.makeSchema(fields)
		val dirRawFiles = configs.SOURCE.DIR_RAW_FILES.toStr
		val delimiter = configs.SOURCE.DELIMITER_RAW.toStr
		val rawdata = spark.sc.textFile(dirRawFiles).mapPartitions(_.drop(1))
		val rowsRDD = rawdata.map(p => {
			var list: collection.mutable.Seq[Any] = collection.mutable.Seq.empty[Any]
			var index = 0
			var fields = p.split(delimiter.charAt(0))
			fields.foreach(value => {
				var valType = schema.fields(index).dataType
				var returnVal: Any = null
				valType match {
					case IntegerType => returnVal = value.toString.toInt
					case ShortType => returnVal = value.toString.toInt
					case DoubleType => returnVal = value.toString.toDouble
					case LongType => returnVal = value.toString.toLong
					case FloatType => returnVal = value.toString.toFloat
					case ByteType => returnVal = value.toString.toByte
					case StringType => returnVal = value.toString
					case TimestampType => returnVal = value.toString
				}
				list = list :+ returnVal
				index += 1
			})
			Row.fromSeq(list)
		})
		val dataFrame = spark.makeDF(rowsRDD, schema)
		spark.writeDFInTarget(dataFrame, configs)
		return true
	}

	def jsonFiles(configs: Zeison.JValue) : Boolean = {
		val sourceSchema = configs.SOURCE.SCHEMA.toStr
		val jsonFiles = configs.SOURCE.DIR_RAW_FILES.toStr
		val schemaSeq = spark.sc.parallelize(Seq(sourceSchema))
		val schema = spark.hiveContext.read.json(schemaSeq).schema
		val dataFrame = spark.hiveContext.jsonFile(jsonFiles, schema)
		spark.writeDFInTarget(dataFrame, configs)
		return true
	}
}
