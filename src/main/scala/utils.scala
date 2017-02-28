package utils

import org.apache.log4j.{LogManager, Logger, Level}
import org.apache.spark.sql.types._

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
		return StructType(schemaString.split(",").map(fieldName => StructField(fieldName.split(":")(0), fieldTypeInSchema(fieldName.split(":")(1)), true)))
	}

	def logLevel(logLevel: String): Boolean = {
		val log = LogManager.getRootLogger()
		val loglevels: List[String] = List("debug", "info", "warn", "error", "fatal")
		if (loglevels.contains(logLevel.toLowerCase) == false)
			throw new Exception("Log level not exists!")
		System.setProperty("loglevel", logLevel.toUpperCase)
		logLevel.toLowerCase match {
			case "debug" => log.setLevel(Level.DEBUG)
			case "info" => log.setLevel(Level.INFO)
			case "warn" => log.setLevel(Level.WARN)
			case "error" => log.setLevel(Level.ERROR)
			case "fatal" => log.setLevel(Level.FATAL)
		}
		return true
	}

}
