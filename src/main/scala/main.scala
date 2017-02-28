package main

import scopt.OptionParser
import org.tsers.zeison.Zeison
import java.lang.Boolean
import shipper.Loader
import utils.Utils
import scala.collection.JavaConversions._

case class Args(config: String = "", loglevel: String = "info")

object Shipper extends App {
	def execution(fileName:String) : Boolean = {
		val json_project = scala.io.Source.fromFile(fileName).getLines.mkString
		val json_configs = Zeison.parse(json_project)
		new Loader delimitedFiles(json_configs)
		return true
	}

	val parser = new scopt.OptionParser[Args]("scopt") {
		opt[String]('c', "config").required().action( (x, c) => c.copy(config = x) ).text("Send config file with absolute path.")
		opt[String]("loglevel").action( (x, c) => c.copy(loglevel = x) ).text("Set log level [debug, info, warn, error or fatal].")
	}

	parser.parse(args, Args()) map { config =>
		new Utils logLevel(config.loglevel)
		val fileConfig = config.config
		execution(fileConfig)
	} getOrElse {
		System.exit(1)
	}
}
