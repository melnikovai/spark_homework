package pl.wisniewskimic.spark.task

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.json4s.JsonAST.{JNull, JString}
import org.json4s.{CustomSerializer, DefaultFormats}

object FormatterUtils {

  implicit val formats = DefaultFormats + JLocalDateTimeSerializer
}

case object JLocalDateTimeSerializer extends CustomSerializer[LocalDateTime](format => {
  val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  ({
    case JString(s) => LocalDateTime.parse(s, dtf)
    case JNull => null
  }, {
    case ldt: LocalDateTime => JString(dtf.format(ldt))
  })
}
)


