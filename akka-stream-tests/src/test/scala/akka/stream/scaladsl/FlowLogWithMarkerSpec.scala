/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import akka.NotUsed
import akka.event.{ DummyClassForStringSources, LogMarker, Logging }
import akka.stream._
import akka.stream.ActorAttributes._
import akka.stream.Attributes.LogLevels
import akka.stream.Supervision._
import akka.stream.testkit.{ ScriptedTest, StreamSpec }
import akka.testkit.TestProbe

class FlowLogWithMarkerSpec extends StreamSpec("""
     akka.loglevel = DEBUG # test verifies logging
     """) with ScriptedTest {

  val logProbe = {
    val p = TestProbe()
    system.eventStream.subscribe(p.ref, classOf[Logging.LogEvent])
    p
  }

  "A LogWithMarker" must {

    val supervisorPath = SystemMaterializer(system).materializer.supervisor.path
    val LogSrc = s"akka.stream.LogWithMarker($supervisorPath)"
    val LogClazz = classOf[Materializer]
    val mdc = Logging.emptyMDC

    "on Flow" must {

      "debug each element" in {
        val debugging = Flow[Int].logWithMarker("my-debug", _ => LogMarker("my-marker"))
        Source(1 to 2).via(debugging).runWith(Sink.ignore)

        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-debug] Element: 1", mdc, LogMarker("my-marker")))
        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-debug] Element: 2", mdc, LogMarker("my-marker")))
        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-debug] Upstream finished."))
      }

      "allow disabling element logging" in {
        val disableElementLogging =
          Attributes.logLevels(onElement = LogLevels.Off, onFinish = Logging.DebugLevel, onFailure = Logging.DebugLevel)

        val debugging = Flow[Int].logWithMarker("my-debug", _ => LogMarker("my-marker"))
        Source(1 to 2).via(debugging).withAttributes(disableElementLogging).runWith(Sink.ignore)

        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-debug] Upstream finished."))
      }

    }

    "on javadsl.Flow" must {
      "debug each element" in {
        val log = Logging.withMarker(system, "com.example.ImportantLogger")

        val debugging: javadsl.Flow[Integer, Integer, NotUsed] = javadsl.Flow
          .of(classOf[Integer])
          .logWithMarker("log-1", _ => LogMarker("marker-1"))
          .logWithMarker("log-2", _ => LogMarker("marker-2"), new akka.japi.function.Function[Integer, Integer] {
            def apply(i: Integer) = i
          })
          .logWithMarker("log-3", _ => LogMarker("marker-3"), new akka.japi.function.Function[Integer, Integer] {
            def apply(i: Integer) = i
          }, log)
          .logWithMarker("log-4", _ => LogMarker("marker-4"), log)

        javadsl.Source.single[Integer](1).via(debugging).runWith(javadsl.Sink.ignore[Integer](), system)

        var counter = 0
        var finishCounter = 0
        import scala.concurrent.duration._
        logProbe.fishForMessage(3.seconds) {
          case Logging.Debug(_, _, msg: String) if msg contains "Element: 1" =>
            counter += 1
            counter == 4 && finishCounter == 4

          case Logging.Debug(_, _, msg: String) if msg contains "Upstream finished" =>
            finishCounter += 1
            counter == 4 && finishCounter == 4
        }
      }
    }

    "on Source" must {
      "debug each element" in {
        Source(1 to 2).logWithMarker("flow-s2", _ => LogMarker("marker-s2")).runWith(Sink.ignore)

        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[flow-s2] Element: 1"))
        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[flow-s2] Element: 2"))
        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[flow-s2] Upstream finished."))
      }

      "allow extracting value to be logged" in {
        case class Complex(a: Int, b: String)
        Source.single(Complex(1, "42")).logWithMarker("flow-s3", _ => LogMarker("marker-s3"), _.b).runWith(Sink.ignore)

        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[flow-s3] Element: 42"))
        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[flow-s3] Upstream finished."))
      }

      "log upstream failure" in {
        val cause = new TestException
        Source.failed(cause).logWithMarker("flow-4", (_: Any) => LogMarker("marker-4")).runWith(Sink.ignore)
        logProbe.expectMsg(Logging.Error(cause, LogSrc, LogClazz, "[flow-4] Upstream failed."))
      }

      "allow passing in custom LoggingAdapter" in {
        val log = Logging.withMarker(system, "com.example.ImportantLogger")
        val marker = LogMarker("marker-5")

        Source.single(42).logWithMarker("flow-5", _ => marker)(log).runWith(Sink.ignore)

        val src = "com.example.ImportantLogger(akka://FlowLogWithMarkerSpec)"
        val clazz = classOf[DummyClassForStringSources]
        logProbe.expectMsg(Logging.Debug(src, clazz, "[flow-5] Element: 42", mdc, marker))
        logProbe.expectMsg(Logging.Debug(src, clazz, "[flow-5] Upstream finished."))
      }

      "allow configuring log levels via Attributes" in {
        val logAttrs = Attributes.logLevels(
          onElement = Logging.WarningLevel,
          onFinish = Logging.InfoLevel,
          onFailure = Logging.DebugLevel)

        Source
          .single(42)
          .logWithMarker("flow-6", _ => LogMarker("marker-6"))
          .withAttributes(Attributes
            .logLevels(onElement = Logging.WarningLevel, onFinish = Logging.InfoLevel, onFailure = Logging.DebugLevel))
          .runWith(Sink.ignore)

        logProbe.expectMsg(Logging.Warning(LogSrc, LogClazz, "[flow-6] Element: 42", mdc, LogMarker("marker-6")))
        logProbe.expectMsg(Logging.Info(LogSrc, LogClazz, "[flow-6] Upstream finished."))

        val cause = new TestException
        Source
          .failed(cause)
          .logWithMarker("flow-6e", (_: Any) => LogMarker("marker-6e"))
          .withAttributes(logAttrs)
          .runWith(Sink.ignore)
        logProbe.expectMsg(
          Logging
            .Debug(LogSrc, LogClazz, "[flow-6e] Upstream failed, cause: FlowLogWithMarkerSpec$TestException: Boom!"))
      }

      "follow supervision strategy when exception thrown" in {
        val ex = new RuntimeException() with NoStackTrace
        val future = Source(1 to 5)
          .logWithMarker("hi", _ => LogMarker("marker-hi"), _ => throw ex)
          .withAttributes(supervisionStrategy(resumingDecider))
          .runWith(Sink.fold(0)(_ + _))
        Await.result(future, 500.millis) shouldEqual 0
      }
    }

    "on javadsl.Source" must {
      "debug each element" in {
        val log = Logging.withMarker(system, "com.example.ImportantLogger")

        javadsl.Source
          .single[Integer](1)
          .logWithMarker("log-1", _ => LogMarker("marker-1"))
          .logWithMarker("log-2", _ => LogMarker("marker-2"), new akka.japi.function.Function[Integer, Integer] {
            def apply(i: Integer) = i
          })
          .logWithMarker("log-3", _ => LogMarker("marker-3"), new akka.japi.function.Function[Integer, Integer] {
            def apply(i: Integer) = i
          }, log)
          .logWithMarker("log-4", _ => LogMarker("marker-4"), log)
          .runWith(javadsl.Sink.ignore[Integer](), system)

        var counter = 1
        import scala.concurrent.duration._
        logProbe.fishForMessage(3.seconds) {
          case Logging.Debug(_, _, msg: String) if msg contains "Element: 1" =>
            counter += 1
            counter == 4

          case Logging.Debug(_, _, msg: String) if msg contains "Upstream finished" =>
            false
        }
      }
    }

  }

  final class TestException extends RuntimeException("Boom!") with NoStackTrace

}
