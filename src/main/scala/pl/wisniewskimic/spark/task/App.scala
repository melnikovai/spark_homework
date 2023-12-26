package pl.wisniewskimic.spark.task

import ArgumentsExtractor.parseArguments
import org.apache.spark.sql.functions.{column, date_format, from_unixtime, udf}
import org.apache.spark.sql.SparkSession
import pl.wisniewskimic.spark.task.UrlExtractor.extractUrl

object App {

  def main(args: Array[String]) {
    val filesPathsOpt = parseArguments(args)
    filesPathsOpt match {
      case None => println("Invalid arguments. Provide --urls --countries and --output file paths parameters.")
      case Some(filesPaths) =>
        val session = createSession()

        val extractUrlUdf = udf( x => extractUrl(x) )

        val countriesDF = session
          .read
          .option("header", "true")
          .option("delimiter", ",")
          .csv(filesPaths.countries)
        val desiredCountriesDF = countriesDF
          .filter(column("active") === 1 )

        val urlsDF = session
          .read
          .option("header", "true")
          .option("delimiter", ",")
          .csv(filesPaths.urls)
        val desiredUrlsDF =
          urlsDF.join(desiredCountriesDF, Seq("country_id"), "leftsemi")

        val answer = desiredUrlsDF
          .withColumn("extracted_url", extractUrlUdf(column("url")))
          .selectExpr("*","extracted_url.*")
          .drop(column("extracted_url"))
          .withColumn(
            "v_length",
            date_format(from_unixtime(column("v_length")), "mm:ss") )
          .na.fill(default_values)

        answer
          .write
          .mode("overwrite")
          .option("header", "true")
          .option("delimiter", ",")
          .csv(filesPaths.output)

        session.stop()
    }
  }

  def createSession(): SparkSession = {
    SparkSession
      .builder
      .appName("Task1")
      .getOrCreate()
  }

  val default_values = Map(
    "id" -> null.asInstanceOf[Int],
    "views" -> null.asInstanceOf[Int],
    "country_id" -> "unknown",
    "url" -> "unknown",
    "channel_desc" -> "unknown",
    "domain" -> "unknown",
    "subpath" -> "unknown",
    "videoId" -> "unknown",
    "contentType" -> "unknown", //redundant
    "v_length"  -> "unknown"
  )
}
