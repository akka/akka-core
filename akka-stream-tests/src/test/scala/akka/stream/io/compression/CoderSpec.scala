/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io.compression

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream }
import java.util.concurrent.ThreadLocalRandom
import java.util.zip.DataFormatException

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import org.scalatest.Inspectors
import org.scalatest.wordspec.AnyWordSpec

import akka.NotUsed
import akka.stream.impl.io.compression.Compressor
import akka.stream.scaladsl.{ Compression, Flow, Sink, Source }
import akka.util.ByteString

abstract class CoderSpec(codecName: String) extends AnyWordSpec with CodecSpecSupport with Inspectors {
  import CompressionTestingTools._

  protected def newCompressor(): Compressor
  protected def encoderFlow: Flow[ByteString, ByteString, Any]
  protected def decoderFlow(
      maxBytesPerChunk: Int = Compression.MaxBytesPerChunkDefault): Flow[ByteString, ByteString, Any]

  protected def newDecodedInputStream(underlying: InputStream): InputStream
  protected def newEncodedOutputStream(underlying: OutputStream): OutputStream

  case object AllDataAllowed extends Exception with NoStackTrace
  protected def corruptInputCheck: Boolean = true

  def extraTests(): Unit = {}

  s"The $codecName codec" should {
    "produce valid data on immediate finish" in {
      streamDecode(newCompressor().finish()) should readAs(emptyText)
    }
    "properly encode an empty string" in {
      streamDecode(ourEncode(emptyTextBytes)) should readAs(emptyText)
    }
    "properly decode an empty string" in {
      ourDecode(streamEncode(emptyTextBytes)) should readAs(emptyText)
    }
    "properly round-trip encode/decode an empty string" in {
      ourDecode(ourEncode(emptyTextBytes)) should readAs(emptyText)
    }
    "properly encode a small string" in {
      streamDecode(ourEncode(smallTextBytes)) should readAs(smallText)
    }
    "properly decode a small string" in {
      ourDecode(streamEncode(smallTextBytes)) should readAs(smallText)
    }
    "properly round-trip encode/decode a small string" in {
      ourDecode(ourEncode(smallTextBytes)) should readAs(smallText)
    }
    "properly encode a large string" in {
      streamDecode(ourEncode(largeTextBytes)) should readAs(largeText)
    }
    "properly decode a large string" in {
      ourDecode(streamEncode(largeTextBytes)) should readAs(largeText)
    }
    "properly round-trip encode/decode a large string" in {
      ourDecode(ourEncode(largeTextBytes)) should readAs(largeText)
    }

    if (corruptInputCheck) {
      "throw an error on corrupt input" in {
        (the[RuntimeException] thrownBy {
          ourDecode(corruptContent)
        }).ultimateCause should be(a[DataFormatException])
      }
    }

    "not throw an error if a subsequent block is corrupt" in {
      pending // FIXME: should we read as long as possible and only then report an error, that seems somewhat arbitrary
      ourDecode(Seq(encode("Hello,"), encode(" dear "), corruptContent).join) should readAs("Hello, dear ")
    }
    "decompress in very small chunks" in {
      val compressed = encode("Hello")

      decodeChunks(Source(Vector(compressed.take(10), compressed.drop(10)))) should readAs("Hello")
    }
    "support chunked round-trip encoding/decoding" in {
      val chunks = largeTextBytes.grouped(512).toVector
      val comp = newCompressor()
      val compressedChunks = chunks.map { chunk =>
          comp.compressAndFlush(chunk)
        } :+ comp.finish()
      val uncompressed = decodeFromIterator(() => compressedChunks.iterator)

      uncompressed should readAs(largeText)
    }
    "works for any split in prefix + suffix" in {
      val compressed = streamEncode(smallTextBytes)
      def tryWithPrefixOfSize(prefixSize: Int): Unit = {
        val prefix = compressed.take(prefixSize)
        val suffix = compressed.drop(prefixSize)

        decodeChunks(Source(prefix :: suffix :: Nil)) should readAs(smallText)
      }
      (0 to compressed.size).foreach(tryWithPrefixOfSize)
    }
    "works for chunked compressed data of sizes just above 1024" in {
      val comp = newCompressor()
      val inputBytes = ByteString(
        """{"baseServiceURL":"http://www.acme.com","endpoints":{"assetSearchURL":"/search","showsURL":"/shows","mediaContainerDetailURL":"/container","featuredTapeURL":"/tape","assetDetailURL":"/asset","moviesURL":"/movies","recentlyAddedURL":"/recent","topicsURL":"/topics","scheduleURL":"/schedule"},"urls":{"aboutAweURL":"www.foobar.com"},"channelName":"Cool Stuff","networkId":"netId","slotProfile":"slot_1","brag":{"launchesUntilPrompt":10,"daysUntilPrompt":5,"launchesUntilReminder":5,"daysUntilReminder":2},"feedbackEmailAddress":"feedback@acme.com","feedbackEmailSubject":"Commends from User","splashSponsor":[],"adProvider":{"adProviderProfile":"","adProviderProfileAndroid":"","adProviderNetworkID":0,"adProviderSiteSectionNetworkID":0,"adProviderVideoAssetNetworkID":0,"adProviderSiteSectionCustomID":{},"adProviderServerURL":"","adProviderLiveVideoAssetID":""},"update":[{"forPlatform":"ios","store":{"iTunes":"www.something.com"},"minVer":"1.2.3","notificationVer":"1.2.5"},{"forPlatform":"android","store":{"amazon":"www.something.com","play":"www.something.com"},"minVer":"1.2.3","notificationVer":"1.2.5"}],"tvRatingPolicies":[{"type":"sometype","imageKey":"tv_rating_small","durationMS":15000,"precedence":1},{"type":"someothertype","imageKey":"tv_rating_big","durationMS":15000,"precedence":2}],"exts":{"adConfig":{"globals":{"#{adNetworkID}":"2620","#{ssid}":"usa_tveapp"},"iPad":{"showlist":{"adMobAdUnitID":"/2620/usa_tveapp_ipad/shows","adSize":[{"#{height}":90,"#{width}":728}]},"launch":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_ipad&sz=1x1&t=&c=#{doubleclickrandom}"},"watchwithshowtile":{"adMobAdUnitID":"/2620/usa_tveapp_ipad/watchwithshowtile","adSize":[{"#{height}":120,"#{width}":240}]},"showpage":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_ipad/shows/#{SHOW_NAME}&sz=1x1&t=&c=#{doubleclickrandom}"}},"iPadRetina":{"showlist":{"adMobAdUnitID":"/2620/usa_tveapp_ipad/shows","adSize":[{"#{height}":90,"#{width}":728}]},"launch":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_ipad&sz=1x1&t=&c=#{doubleclickrandom}"},"watchwithshowtile":{"adMobAdUnitID":"/2620/usa_tveapp_ipad/watchwithshowtile","adSize":[{"#{height}":120,"#{width}":240}]},"showpage":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_ipad/shows/#{SHOW_NAME}&sz=1x1&t=&c=#{doubleclickrandom}"}},"iPhone":{"home":{"adMobAdUnitID":"/2620/usa_tveapp_iphone/home","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"showlist":{"adMobAdUnitID":"/2620/usa_tveapp_iphone/shows","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"episodepage":{"adMobAdUnitID":"/2620/usa_tveapp_iphone/shows/#{SHOW_NAME}","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"launch":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_iphone&sz=1x1&t=&c=#{doubleclickrandom}"},"showpage":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_iphone/shows/#{SHOW_NAME}&sz=1x1&t=&c=#{doubleclickrandom}"}},"iPhoneRetina":{"home":{"adMobAdUnitID":"/2620/usa_tveapp_iphone/home","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"showlist":{"adMobAdUnitID":"/2620/usa_tveapp_iphone/shows","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"episodepage":{"adMobAdUnitID":"/2620/usa_tveapp_iphone/shows/#{SHOW_NAME}","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"launch":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_iphone&sz=1x1&t=&c=#{doubleclickrandom}"},"showpage":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_iphone/shows/#{SHOW_NAME}&sz=1x1&t=&c=#{doubleclickrandom}"}},"Tablet":{"home":{"adMobAdUnitID":"/2620/usa_tveapp_androidtab/home","adSize":[{"#{height}":90,"#{width}":728},{"#{height}":50,"#{width}":320},{"#{height}":50,"#{width}":300}]},"showlist":{"adMobAdUnitID":"/2620/usa_tveapp_androidtab/shows","adSize":[{"#{height}":90,"#{width}":728},{"#{height}":50,"#{width}":320},{"#{height}":50,"#{width}":300}]},"episodepage":{"adMobAdUnitID":"/2620/usa_tveapp_androidtab/shows/#{SHOW_NAME}","adSize":[{"#{height}":90,"#{width}":728},{"#{height}":50,"#{width}":320},{"#{height}":50,"#{width}":300}]},"launch":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_androidtab&sz=1x1&t=&c=#{doubleclickrandom}"},"showpage":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_androidtab/shows/#{SHOW_NAME}&sz=1x1&t=&c=#{doubleclickrandom}"}},"TabletHD":{"home":{"adMobAdUnitID":"/2620/usa_tveapp_androidtab/home","adSize":[{"#{height}":90,"#{width}":728},{"#{height}":50,"#{width}":320},{"#{height}":50,"#{width}":300}]},"showlist":{"adMobAdUnitID":"/2620/usa_tveapp_androidtab/shows","adSize":[{"#{height}":90,"#{width}":728},{"#{height}":50,"#{width}":320},{"#{height}":50,"#{width}":300}]},"episodepage":{"adMobAdUnitID":"/2620/usa_tveapp_androidtab/shows/#{SHOW_NAME}","adSize":[{"#{height}":90,"#{width}":728},{"#{height}":50,"#{width}":320},{"#{height}":50,"#{width}":300}]},"launch":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_androidtab&sz=1x1&t=&c=#{doubleclickrandom}"},"showpage":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_androidtab/shows/#{SHOW_NAME}&sz=1x1&t=&c=#{doubleclickrandom}"}},"Phone":{"home":{"adMobAdUnitID":"/2620/usa_tveapp_android/home","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"showlist":{"adMobAdUnitID":"/2620/usa_tveapp_android/shows","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"episodepage":{"adMobAdUnitID":"/2620/usa_tveapp_android/shows/#{SHOW_NAME}","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"launch":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_android&sz=1x1&t=&c=#{doubleclickrandom}"},"showpage":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_android/shows/#{SHOW_NAME}&sz=1x1&t=&c=#{doubleclickrandom}"}},"PhoneHD":{"home":{"adMobAdUnitID":"/2620/usa_tveapp_android/home","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"showlist":{"adMobAdUnitID":"/2620/usa_tveapp_android/shows","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"episodepage":{"adMobAdUnitID":"/2620/usa_tveapp_android/shows/#{SHOW_NAME}","adSize":[{"#{height}":50,"#{width}":300},{"#{height}":50,"#{width}":320}]},"launch":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_android&sz=1x1&t=&c=#{doubleclickrandom}"},"showpage":{"doubleClickCallbackURL":"http://pubads.g.doubleclick.net/gampad/ad?iu=/2620/usa_tveapp_android/shows/#{SHOW_NAME}&sz=1x1&t=&c=#{doubleclickrandom}"}}}}}""",
        "utf8")
      val compressed = comp.compressAndFinish(inputBytes)

      ourDecode(compressed) should equal(inputBytes)
    }

    "shouldn't produce huge ByteStrings for some input" in {
      val array = Array.fill(10)(1.toByte)
      val compressed = streamEncode(ByteString(array))
      val limit = 10000
      val resultBs =
        Source
          .single(compressed)
          .via(decoderFlow(maxBytesPerChunk = limit))
          .limit(4200)
          .runWith(Sink.seq)
          .awaitResult(3.seconds)

      forAll(resultBs) { bs =>
        bs.length should be < limit
        bs.forall(_ == 1) should equal(true)
      }
    }

    "be able to decode chunk-by-chunk (depending on input chunks)" in {
      val minLength = 100
      val maxLength = 1000
      val numElements = 1000

      val random = ThreadLocalRandom.current()
      val sizes = Seq.fill(numElements)(random.nextInt(minLength, maxLength))
      def createByteString(size: Int): ByteString =
        ByteString(Array.fill(size)(1.toByte))

      val sizesAfterRoundtrip =
        Source
          .fromIterator(() => sizes.iterator.map(createByteString))
          .via(encoderFlow)
          .via(decoderFlow())
          .runFold(Seq.empty[Int])(_ :+ _.size)

      sizesAfterRoundtrip.awaitResult(3.seconds) shouldEqual sizes
    }

    extraTests()
  }

  def encode(s: String) = ourEncode(ByteString(s, "UTF8"))
  def ourEncode(bytes: ByteString): ByteString = newCompressor().compressAndFinish(bytes)
  def ourDecode(bytes: ByteString): ByteString =
    Source.single(bytes).via(decoderFlow()).join.awaitResult(3.seconds)

  lazy val corruptContent = {
    val content = encode(largeText).toArray
    content(14) = 36.toByte
    ByteString(content)
  }

  def streamEncode(bytes: ByteString): ByteString = {
    val output = new ByteArrayOutputStream()
    val gos = newEncodedOutputStream(output); gos.write(bytes.toArray); gos.close()
    ByteString(output.toByteArray)
  }

  def streamDecode(bytes: ByteString): ByteString = {
    val output = new ByteArrayOutputStream()
    val input = newDecodedInputStream(new ByteArrayInputStream(bytes.toArray))

    val buffer = new Array[Byte](500)
    @tailrec def copy(from: InputStream, to: OutputStream): Unit = {
      val read = from.read(buffer)
      if (read >= 0) {
        to.write(buffer, 0, read)
        copy(from, to)
      }
    }

    copy(input, output)
    ByteString(output.toByteArray)
  }

  def decodeChunks(input: Source[ByteString, NotUsed]): ByteString =
    input.via(decoderFlow()).join.awaitResult(3.seconds) // TODO make it use remaining?

  def decodeFromIterator(iterator: () => Iterator[ByteString]): ByteString =
    Await.result(Source.fromIterator(iterator).via(decoderFlow()).join, 3.seconds)
}
