package shipper

import org.tsers.zeison.Zeison
import utils.Utils
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.types._

class Loader {

	val utils = new Utils

	def delimitedFiles(configs: Zeison.JValue) : Boolean = {
		val fields = configs.FIELDS.map(f => "%s:%s".format(f.NAME.toStr, f.TYPE.toStr.toLowerCase)).mkString(",")
		val schema = utils.makeSchema(fields)
		val dirRawFiles = configs.DIR_RAW_FILES.toStr
		val delimiter = configs.DELIMITER_RAW.toStr
		val conf = new SparkConf().setAppName("Big Shipper")
		val sc = new SparkContext(conf)
		val rawdata = sc.textFile(dirRawFiles).mapPartitions(_.drop(1))
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
		val hiveContext = new hive.HiveContext(sc)
		val dataFrame = hiveContext.createDataFrame(rowsRDD, schema)
		val targetTable = configs.TARGET_HIVE_TABLE.toStr
		val fieldsStage = configs.FIELDS.map(_.NAME.toStr).mkString(",")
		dataFrame.registerTempTable("stage")
		hiveContext.setConf("hive.exec.dynamic.partition", "true")
		hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
		hiveContext.sql(s"INSERT INTO TABLE $targetTable SELECT $fieldsStage FROM stage")
		return true
	}
}
