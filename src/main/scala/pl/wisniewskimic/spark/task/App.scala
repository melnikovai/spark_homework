package pl.wisniewskimic.spark.task

import ArgumentsExtractor.parseArguments
import org.apache.spark.sql.catalyst.expressions.codegen.FalseLiteral
import org.apache.spark.sql.functions.{column, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object App {

  def main(args: Array[String]) {
    val inputFiles = parseArguments(args)
    inputFiles match {
      case None => println("Invalid arguments. Provide --urls --countries and --output file paths parameters.")
      case Some(inputFiles) =>
        val session = createSession()

        val countriesDF = session
          .read
          .option("header", "true")
          .option("delimiter", ",")
          .csv(inputFiles.countries)
        val desiredCountriesDF = session
          .read
          .option("header", "true")
          .option("delimiter", ",")
          .csv(inputFiles.countries)

        val urlsDF = session
          .read
          .option("header", "true")
          .option("delimiter", ",")
          .csv(inputFiles.urls)
        val desiredUrlsDF =
          urlsDF.join(desiredCountriesDF, Seq("country_id"), "leftsemi")

        desiredUrlsDF
          .withColumn("extracted_url", UrlExtractor.extractUrlUdf(column("url")))
          .show(truncate = false)

        session.stop()
    }
  }

  def createSession(): SparkSession = {
    SparkSession
      .builder
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .appName("Task1")
      .getOrCreate()
  }
}
