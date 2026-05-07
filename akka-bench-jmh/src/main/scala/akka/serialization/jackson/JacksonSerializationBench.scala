/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson

import java.time.{ Duration => JDuration }
import java.time.Instant
import java.time.LocalDateTime
import java.util
import java.util.concurrent.TimeUnit

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

import akka.actor._
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest

object JacksonSerializationBench {
  trait TestMessage

  final case class Small(name: String, num: Int) extends TestMessage

  final case class Medium(
      field1: String,
      field2: String,
      field3: String,
      num1: Int,
      num2: Int,
      num3: Int,
      flag1: Boolean,
      flag2: Boolean,
      duration: FiniteDuration,
      date: LocalDateTime,
      instant: Instant,
      nested1: Small,
      nested2: Small,
      nested3: Small)
      extends TestMessage

  final case class Large(
      nested1: Medium,
      nested2: Medium,
      nested3: Medium,
      vector: Vector[Medium],
      map: Map[String, Medium])
      extends TestMessage

  final class TimeMessage(val duration: FiniteDuration, val date: LocalDateTime, val instant: Instant)
      extends TestMessage

}

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(2)
@Warmup(iterations = 4)
@Measurement(iterations = 5)
class JacksonSerializationBench {
  import JacksonSerializationBench._

  val smallMsg1 = Small("abc", 17)
  val smallMsg2 = Small("def", 18)
  val smallMsg3 = Small("ghi", 19)
  val mediumMsg1 = Medium(
    "abc",
    "def",
    "ghi",
    1,
    2,
    3,
    flag1 = false,
    flag2 = true,
    5.seconds,
    LocalDateTime.of(2019, 4, 29, 23, 15, 3, 12345),
    Instant.now(),
    smallMsg1,
    smallMsg2,
    smallMsg3)
  val mediumMsg2 = Medium(
    "ABC",
    "DEF",
    "GHI",
    10,
    20,
    30,
    flag1 = true,
    flag2 = false,
    5.millis,
    LocalDateTime.of(2019, 4, 29, 23, 15, 4, 12345),
    Instant.now(),
    smallMsg1,
    smallMsg2,
    smallMsg3)
  val mediumMsg3 = Medium(
    "abcABC",
    "defDEF",
    "ghiGHI",
    100,
    200,
    300,
    flag1 = true,
    flag2 = true,
    200.millis,
    LocalDateTime.of(2019, 4, 29, 23, 15, 5, 12345),
    Instant.now(),
    smallMsg1,
    smallMsg2,
    smallMsg3)
  val largeMsg = Large(
    mediumMsg1,
    mediumMsg2,
    mediumMsg3,
    Vector(mediumMsg1, mediumMsg2, mediumMsg3),
    Map("a" -> mediumMsg1, "b" -> mediumMsg2, "c" -> mediumMsg3))

  val timeMsg = new TimeMessage(5.seconds, LocalDateTime.of(2019, 4, 29, 23, 15, 3, 12345), Instant.now())

  import JavaMessages._
  val jSmallMsg1 = new JSmall("abc", 17)
  val jSmallMsg2 = new JSmall("def", 18)
  val jSmallMsg3 = new JSmall("ghi", 19)
  val jMediumMsg1 = new JMedium(
    "abc",
    "def",
    "ghi",
    1,
    2,
    3,
    false,
    true,
    JDuration.ofSeconds(5),
    LocalDateTime.of(2019, 4, 29, 23, 15, 3, 12345),
    Instant.now(),
    jSmallMsg1,
    jSmallMsg2,
    jSmallMsg3)
  val jMediumMsg2 = new JMedium(
    "ABC",
    "DEF",
    "GHI",
    10,
    20,
    30,
    true,
    false,
    JDuration.ofMillis(5),
    LocalDateTime.of(2019, 4, 29, 23, 15, 4, 12345),
    Instant.now(),
    jSmallMsg1,
    jSmallMsg2,
    jSmallMsg3)
  val jMediumMsg3 = new JMedium(
    "abcABC",
    "defDEF",
    "ghiGHI",
    100,
    200,
    300,
    true,
    true,
    JDuration.ofMillis(200),
    LocalDateTime.of(2019, 4, 29, 23, 15, 5, 12345),
    Instant.now(),
    jSmallMsg1,
    jSmallMsg2,
    jSmallMsg3)
  val jMap = new util.HashMap[String, JMedium]()
  jMap.put("a", jMediumMsg1)
  jMap.put("b", jMediumMsg2)
  jMap.put("c", jMediumMsg3)
  val jLargeMsg = new JLarge(
    jMediumMsg1,
    jMediumMsg2,
    jMediumMsg3,
    java.util.Arrays.asList(jMediumMsg1, jMediumMsg2, jMediumMsg3),
    jMap)

  var system: ActorSystem = _
  var serialization: Serialization = _

  @nowarn("msg=immutable val") // JMH updates this via reflection
  @Param(Array("jackson-json", "jackson-cbor")) // "java"
  private var serializerName: String = _

  @nowarn("msg=immutable val")
  @Param(Array("off", "gzip", "lz4"))
  private var compression: String = _

  @Setup(Level.Trial)
  def setupTrial(): Unit = {
    val config = ConfigFactory.parseString(s"""
        akka {
          loglevel = WARNING
          actor {
            serialization-bindings {
              "${classOf[TestMessage].getName}" = $serializerName
              "${classOf[JTestMessage].getName}" = $serializerName
            }
          }
          serialization.jackson {
            serialization-features {
              #WRITE_DATES_AS_TIMESTAMPS = off
            }
          }
        }
        akka.serialization.jackson.jackson-json.compression {
          algorithm = $compression
          compress-larger-than = 100 b
        }
      """)

    system = ActorSystem("JacksonSerializationBench", config)
    serialization = SerializationExtension(system)
  }

  @TearDown(Level.Trial)
  def tearDownTrial(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  private var size = 0L

  private def serializeDeserialize[T <: AnyRef](msg: T): T = {
    serialization.findSerializerFor(msg) match {
      case serializer: SerializerWithStringManifest =>
        val blob = serializer.toBinary(msg)
        if (size != blob.length) {
          size = blob.length
          println(
            s"# Size is $size of ${msg.getClass.getName} with " +
            s"${system.settings.config.getString("akka.serialization.jackson.jackson-json.compression.algorithm")}")
        }
        serializer.fromBinary(blob, serializer.manifest(msg)).asInstanceOf[T]
      case serializer =>
        val blob = serializer.toBinary(msg)
        if (serializer.includeManifest)
          serializer.fromBinary(blob, Some(msg.getClass)).asInstanceOf[T]
        else
          serializer.fromBinary(blob, None).asInstanceOf[T]
    }

  }

  @Benchmark
  def small(): Small = {
    serializeDeserialize(smallMsg1)
  }

  @Benchmark
  def medium(): Medium = {
    serializeDeserialize(mediumMsg1)
  }

  @Benchmark
  def large(): Large = {
    serializeDeserialize(largeMsg)
  }

  @Benchmark
  def jSmall(): JSmall = {
    serializeDeserialize(jSmallMsg1)
  }

  @Benchmark
  def jMedium(): JMedium = {
    serializeDeserialize(jMediumMsg1)
  }

  @Benchmark
  def jLarge(): JLarge = {
    serializeDeserialize(jLargeMsg)
  }

  @Benchmark
  def timeMessage(): TimeMessage = {
    serializeDeserialize(timeMsg)
  }

}
