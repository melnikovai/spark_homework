package pl.wisniewskimic.spark.task

import org.apache.spark.sql.types.{DataTypes, StringType}
import io.lemonlabs.uri.Url
import org.apache.spark.sql.functions.udf

object UrlExtractor {

  case class UrlStruct(domain: Option[String],
                       subpath: Option[String],
                       videoId: Option[String],
                       contentType : String)

  val unparsableUrlStruct = UrlStruct(None, None, None, Video.toString)

  private val idSupportedKeys = Seq("v", "jumpid")
  sealed trait ContentTypeEnum {
    override def toString: String = this match {
      case Video => "video"
      case Short => "short"
    }
  }
  case object Video extends ContentTypeEnum
  case object Short extends ContentTypeEnum


//  val urlStructType = DataTypes.createStructType(Array(
//    DataTypes.createStructField("domain", StringType, true),
//    DataTypes.createStructField("subpath", StringType, true),
//    DataTypes.createStructField("videoId", StringType, true),
//    DataTypes.createStructField("contentType", StringType, true),
//  ))


  def extractUrl(url: String): UrlStruct = {
    Url.parseOption(url) match {
      case Some(parsedUrl) =>
        UrlStruct(
          extractHost(parsedUrl),
          extractSubpath(parsedUrl),
          extractVideoId(parsedUrl),
          extractContentType(parsedUrl).toString )
      case None => unparsableUrlStruct
    }
  }

  val extractUrlUdf = udf( x => extractUrl(x) )

  private def extractHost(parsedUrl: Url): Option[String] = {
    parsedUrl.hostOption match {
      case Some(x) if !x.value.isEmpty => Some(x.value)
      case _ => None
    }
  }

  private def extractSubpath(parsedUrl: Url): Option[String] = {
    parsedUrl.path.toString() match {
      case x if !x.isEmpty => Some(x)
      case _ => None
    }
  }

  private def extractVideoId(parsedUrl: Url): Option[String] = {
    //  TODO: what if we have both params set ?
    parsedUrl.query.params.find( x => idSupportedKeys.contains( x._1 ) ) match {
      case Some((_, value)) => value
      case _ => None
    }
  }

  private def extractContentType(parsedUrl: Url): ContentTypeEnum = {
    parsedUrl.path.parts.contains("short") match {
      case true => Short
      case false => Video
    }
  }

}
