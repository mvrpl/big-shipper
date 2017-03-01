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
	sc.setLogLevel(System.getProperty("loglevel"))
	val hiveContext = new hive.HiveContext(sc)

	def makeDF(rows: RDD[Row], schema: StructType): DataFrame = {
		val dataFrame = hiveContext.createDataFrame(rows, schema)
		return dataFrame
	}

	def insertTarget(dataFrame: DataFrame, configs: Zeison.JValue): Boolean = {
		val targetTable = configs.TARGET.HIVE_TABLE.toStr
		val fieldsStage = configs.SOURCE.FIELDS.map(_.NAME.toStr).mkString(",")
		dataFrame.registerTempTable("stage")
		hiveContext.setConf("hive.exec.dynamic.partition", "true")
		hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
		hiveContext.sql(s"INSERT INTO TABLE $targetTable SELECT $fieldsStage FROM stage")
		return true
	}
}
