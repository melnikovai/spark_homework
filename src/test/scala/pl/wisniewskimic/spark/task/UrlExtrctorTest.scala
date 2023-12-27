package pl.wisniewskimic.spark.task

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UrlExtrctorTest extends AnyFlatSpec with Matchers {

 "valid url" should "parse correctly" in {
   val result = UrlExtractor.extractUrl("https://www.youtube.com/watch?v=1234")
   result.domain shouldBe Some("www.youtube.com")
   result.videoId shouldBe  Some("1234")
   result.subpath shouldBe Some("/watch")
   result.contentType shouldBe UrlExtractor.Video.toString
 }

  "valid url with jumpId" should "parse correctly" in {
    val result = UrlExtractor.extractUrl("https://www.hpe.com/h22228/video-gallery/us/en/36c66060-05a4-482f-9704-2e707d82c85b/video?jumpId=in_videogallery_7dfc5298-206f-4de9-9965-605f2b89d659_gaiw")
    result.domain shouldBe Some("www.hpe.com")
    result.videoId shouldBe Some("in_videogallery_7dfc5298-206f-4de9-9965-605f2b89d659_gaiw")
    result.subpath shouldBe Some("/h22228/video-gallery/us/en/36c66060-05a4-482f-9704-2e707d82c85b/video")
    result.contentType shouldBe UrlExtractor.Video.toString
  }

//  TODD: is this the right choice ?
  "valid url with both video ids" should "parse correctly" in {
    val result = UrlExtractor.extractUrl("https://www.hpe.com/h22228/video-gallery/us/en/36c66060-05a4-482f-9704-2e707d82c85b/video?v=1234&jumpId=in_videogallery_7dfc5298-206f-4de9-9965-605f2b89d659_gaiw")
    result.domain shouldBe Some("www.hpe.com")
    result.videoId.isDefined shouldBe true
    result.videoId.get should (equal ("1234") or equal ("in_videogallery_7dfc5298-206f-4de9-9965-605f2b89d659_gaiw"))
    result.subpath shouldBe Some("/h22228/video-gallery/us/en/36c66060-05a4-482f-9704-2e707d82c85b/video")
    result.contentType shouldBe UrlExtractor.Video.toString
  }

  "valid url with shorts in path" should "parse correctly as short" in {
    val result = UrlExtractor.extractUrl("https://www.wp.pl/shorts/video-gallery/us/en/36c66060-05a4-482f-9704-2e707d82c85b/video?v=12")
    result.domain shouldBe Some("www.wp.pl")
    result.videoId shouldBe Some("12")
    result.subpath shouldBe Some("/shorts/video-gallery/us/en/36c66060-05a4-482f-9704-2e707d82c85b/video")
    result.contentType shouldBe UrlExtractor.Short.toString
  }

  "valid url without subpath" should "parse correctly" in {
    val result = UrlExtractor.extractUrl("https://www.wp.pl?v=12")
    result.domain shouldBe Some("www.wp.pl")
    result.videoId shouldBe Some("12")
    result.subpath shouldBe None
    result.contentType shouldBe UrlExtractor.Video.toString
  }

  "valid url without parameters" should "parse correctly" in {
    val result = UrlExtractor.extractUrl("https://www.wp.pl/wiadomosci")
    result.domain shouldBe Some("www.wp.pl")
    result.videoId shouldBe None
    result.subpath shouldBe Some("/wiadomosci")
    result.contentType shouldBe UrlExtractor.Video.toString
  }

  "valid url without domain" should "parse correctly" in {
    val result = UrlExtractor.extractUrl("https:///wiadomosci")
    result.domain shouldBe None
    result.videoId shouldBe None
    result.subpath shouldBe Some("/wiadomosci")
    result.contentType shouldBe UrlExtractor.Video.toString
  }

  "null" should "parse as default" in {
    val result = UrlExtractor.extractUrl(null.asInstanceOf[String])
    result shouldBe UrlExtractor.unparsableUrlStruct
  }

}
