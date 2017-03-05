package main

import scopt.OptionParser
import org.tsers.zeison.Zeison
import java.lang.Boolean
import shipper.Loader
import utils.{Utils, Logs}
import scala.collection.JavaConversions._

case class Args(config: String = "", loglevel: String = "info")

object Shipper extends App with Logs {
	def execution(fileName:String) : Boolean = {
		try {
			val json_project = scala.io.Source.fromFile(fileName).getLines.mkString
			val json_configs = Zeison.parse(json_project)
			if (json_configs.SOURCE.TYPE.toStr == "delimitedfile"){
				new Loader delimitedFiles(json_configs)
			} else if (json_configs.SOURCE.TYPE.toStr == "json"){
				new Loader jsonFiles(json_configs)
			} else if (json_configs.SOURCE.TYPE.toStr == "rdbms"){
				info("Source type not implemented")
			} else {
				error(s"Wrong SOURCE.TYPE in $fileName")
				System.exit(1)
			}
		} catch {
			case e: Zeison.ZeisonException => { error("Error on read JSON config file.", e);System.exit(1) }
			case e: java.io.FileNotFoundException => { error(s"$fileName not exists.", e);System.exit(1) }
		}
		return true
	}

	val parser = new scopt.OptionParser[Args]("scopt") {
		opt[String]('c', "config").required().action( (x, c) => c.copy(config = x) ).text("Send config file with absolute path.")
		opt[String]("loglevel").action( (x, c) => c.copy(loglevel = x) ).text("Set log level [debug, info, warn, error or fatal].")
	}

	parser.parse(args, Args()) map { config =>
		new Utils logLevel(config.loglevel)
		val fileConfig = config.config
		info(s"Execution started with config file: $fileConfig")
		execution(fileConfig)
	} getOrElse {
		System.exit(1)
	}
}
