package pl.wisniewskimic.spark.task

import scopt.OParser

object ArgumentsExtractor {

  val builder = OParser.builder[FilePaths]
  val parser = {
    import builder._
    OParser.sequence(
      opt[String]("urls")
        .action((value, config) => config.copy(urls = value))
        .text("Input file containing urls"),
      opt[String]("countries")
        .action((value, config) => config.copy(countries = value))
        .text("Input file containing countries"),
      opt[String]("output")
        .action((value, config) => config.copy(output = value))
        .text("Output file")
    )
  }

  def parseArguments(args: Array[String]): Option[FilePaths] = {
    OParser.parse(parser, args, FilePaths()) match {
      case Some(config) => config match {
        case _ if config.isValid() => Some(config)
        case _ => None
      }
      case _ => None
    }
  }
}

case class FilePaths(urls: String = "", countries: String = "", output: String = "") {
  def isValid(): Boolean = {
    urls.nonEmpty && countries.nonEmpty && output.nonEmpty
  }
}
