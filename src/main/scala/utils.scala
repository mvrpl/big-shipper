package utils

import org.apache.log4j.{BasicConfigurator, PatternLayout, ConsoleAppender, RollingFileAppender, LogManager, Logger, Level}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.tsers.zeison.Zeison

trait Logs {

	private[this] val logger = Logger.getLogger(getClass().getName())

	def debug(message: => String) = if (logger.isEnabledFor(Level.DEBUG)) logger.debug(message)
	def debug(message: => String, ex:Throwable) = if (logger.isEnabledFor(Level.DEBUG)) logger.debug(message,ex)
	def debugValue[T](valueName: String, value: => T):T = {
		val result:T = value
		debug(valueName + " == " + result.toString)
		result
	}

	def info(message: => String) = if (logger.isEnabledFor(Level.INFO)) logger.info(message)
	def info(message: => String, ex:Throwable) = if (logger.isEnabledFor(Level.INFO)) logger.info(message,ex)

	def warn(message: => String) = if (logger.isEnabledFor(Level.WARN)) logger.warn(message)
	def warn(message: => String, ex:Throwable) = if (logger.isEnabledFor(Level.WARN)) logger.warn(message,ex)

	def error(ex:Throwable) = if (logger.isEnabledFor(Level.ERROR)) logger.error(ex.toString,ex)
	def error(message: => String) = if (logger.isEnabledFor(Level.ERROR)) logger.error(message)
	def error(message: => String, ex:Throwable) = if (logger.isEnabledFor(Level.ERROR)) logger.error(message,ex)

	def fatal(ex:Throwable) = if (logger.isEnabledFor(Level.FATAL)) logger.fatal(ex.toString,ex)
	def fatal(message: => String) = if (logger.isEnabledFor(Level.FATAL)) logger.fatal(message)
	def fatal(message: => String, ex:Throwable) = if (logger.isEnabledFor(Level.FATAL)) logger.fatal(message,ex)
}

class Utils extends Logs {

	def fieldTypeInSchema(ftype: String): DataType = {
		ftype match {
			case "bigint" => return LongType
			case "int" => return IntegerType
			case "smallint" => return IntegerType
			case "tinyint" => return ShortType
			case "double" => return DoubleType
			case "decimal" => return DoubleType
			case "float" => return FloatType
			case "byte" => return ByteType
			case "string" => return StringType
			case "date" => return TimestampType
			case "timestamp" => return StringType
			case "boolean" => return BooleanType
			case _ => return StringType
		}
	}

	def makeSchema(schemaString: String): StructType = {
		debug("Schema constructed: "+schemaString)
		try {
			val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName.split(":")(0), fieldTypeInSchema(fieldName.split(":")(1)), true)))
			return schema
		} catch {
			case e: ArrayIndexOutOfBoundsException => { error("Error in making schema, verify your JSON config file.", e);System.exit(1) }
		}
		return new StructType()
	}

	def logLevel(logLevel: String): Boolean = {
		val log = LogManager.getRootLogger()
		LogManager.resetConfiguration()
		val layout = new PatternLayout("%d{yyyy/MM/dd HH:mm:ss} [%p]: %m%n")
		log.addAppender(new ConsoleAppender(layout))
		try {
			val fileAppender = new RollingFileAppender(layout, "/tmp/bigshipper.log")
			log.addAppender(fileAppender)
		} catch {
			case e: java.io.IOException => { println("Error write in log file.") }
		}
		System.setProperty("loglevel", logLevel.toUpperCase)
		logLevel.toLowerCase match {
			case "debug" => log.setLevel(Level.DEBUG)
			case "info" => log.setLevel(Level.INFO)
			case "warn" => log.setLevel(Level.WARN)
			case "error" => log.setLevel(Level.ERROR)
			case "fatal" => log.setLevel(Level.FATAL)
			case _ => throw new Exception("Log level not exists!")
		}
		return true
	}
}

class Spark extends Logs {

	val conf = new SparkConf().setAppName("Big Shipper")
	val sc = new SparkContext(conf)
	val sparkVer = "^[0-9]{1,2}\\.[0-9]{1,2}".r.findFirstIn(sc.version).get.toDouble
	sc.setLogLevel(System.getProperty("loglevel"))
	val hiveContext = new hive.HiveContext(sc)
	hiveContext.setConf("hive.exec.dynamic.partition", "true")
	hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

	def writePartitionsDF(dataFrame: DataFrame, partitions: Seq[String], targetTable: String, saveModeDF: SaveMode): Boolean = {
		if (sparkVer >= 2.0) {
			val reOrder = dataFrame.columns.filter(!partitions.contains(_)) ++ partitions 
			dataFrame.select(reOrder.map(functions.col):_*).write.format("orc").mode(saveModeDF).insertInto(targetTable)
		} else if (sparkVer >= 1.4) {
			dataFrame.write.partitionBy(partitions:_*).format("orc").mode(saveModeDF).saveAsTable(targetTable)
		}
		return true
	}

	def writeSelectedColsDF(dataFrame: DataFrame, partitionName: String, targetTable: String, saveModeDF: SaveMode): Boolean = {
		val reOrder = dataFrame.columns.filter(_ != partitionName) :+ partitionName
		dataFrame.select(reOrder.map(functions.col):_*).write.format("orc").mode(saveModeDF).insertInto(targetTable)
		return true
	}

	def updateDF(left: DataFrame, right: DataFrame, key: String): DataFrame = {
		left.alias("l").join(right.alias("r"), functions.col(s"l.$key") === functions.col(s"r.$key"), "fullouter").select(left.columns.map( colName => functions.coalesce( functions.col(s"r.$colName"), functions.col(s"l.$colName") ).alias(s"$colName")):_*)
	}

	def writeDFInTarget(dataFrame: DataFrame, configs: Zeison.JValue): Boolean = {
		val targetTable = configs.TARGET.HIVE_TABLE.toStr
		val saveModeDF = if (configs.TARGET.ACTION.toStr.toLowerCase == "update") SaveMode.Overwrite else SaveMode.Append
		if (configs.TARGET.PARTITION.isDefined) {
			if (configs.TARGET.PARTITION.isArray) {
				val partitionsList = configs.TARGET.PARTITION.map(_.toStr).toSeq
				writePartitionsDF(dataFrame, partitionsList, targetTable, saveModeDF)
			} else {
				val partitionName = configs.TARGET.PARTITION.toStr
				if (sparkVer >= 2.0) {
					writeSelectedColsDF(dataFrame, partitionName, targetTable, saveModeDF)
				} else if (sparkVer >= 1.4) {
					dataFrame.write.partitionBy(partitionName).format("orc").mode(saveModeDF).saveAsTable(targetTable)
				}
			}
		} else if (configs.TARGET.CUSTOMPARTITION.isDefined) {
			val partitions = configs.TARGET.CUSTOMPARTITION.map(_.map(_.toStr).toSeq).toSeq
			var dataDF: DataFrame = dataFrame
			partitions.foreach(values => {
				if (values.size != 2) {
					error("The custom partitions need 2 values every item: [NAME, VALUE]")
					System.exit(1)
				}
				if (values(1).startsWith("D{") && values(1).endsWith("}")) {
					val formatD = values(1).stripPrefix("D{").stripSuffix("}")
					val format = new java.text.SimpleDateFormat(formatD)
					dataDF = dataDF.withColumn(values(0), functions.lit(format.format(new java.util.Date()))) 
				} else {
					dataDF = dataDF.withColumn(values(0), functions.lit(values(1))) 
				}
			})
			if (sparkVer >= 2.0) {
				dataDF.write.format("orc").mode(saveModeDF).insertInto(targetTable)
			} else if (sparkVer >= 1.4) {
				val partitionsDF = partitions.map(_(0))
				writePartitionsDF(dataDF, partitionsDF, targetTable, saveModeDF)
			}
		} else {
			if (sparkVer >= 2.0) {
				dataFrame.write.format("orc").mode(saveModeDF).insertInto(targetTable)
			} else if (sparkVer >= 1.4) {
				dataFrame.write.format("orc").mode(saveModeDF).saveAsTable(targetTable)
			}
		}
		return true
	}
}
