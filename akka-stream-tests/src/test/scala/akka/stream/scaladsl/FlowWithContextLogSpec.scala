/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.event.Logging
import akka.stream._
import akka.stream.Attributes.LogLevels
import akka.stream.testkit.ScriptedTest
import akka.stream.testkit.StreamSpec
import akka.testkit.TestProbe

class FlowWithContextLogSpec extends StreamSpec("""
     akka.loglevel = DEBUG # test verifies logging
     """) with ScriptedTest {

  val logProbe = {
    val p = TestProbe()
    system.eventStream.subscribe(p.ref, classOf[Logging.LogEvent])
    p
  }

  "log() from FlowWithContextOps" must {

    val supervisorPath = (SystemMaterializer(system).materializer).supervisor.path
    val LogSrc = s"akka.stream.Log($supervisorPath)"
    val LogClazz = classOf[Materializer]

    "on FlowWithContext" must {

      "log each element" in {
        val logging = FlowWithContext[Message, Long].log("my-log")
        Source(List(Message("a", 1L), Message("b", 2L)))
          .asSourceWithContext(m => m.offset)
          .via(logging)
          .asSource
          .runWith(Sink.ignore)

        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log] Element: Message(a,1)"))
        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log] Element: Message(b,2)"))
        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log] Upstream finished."))
      }

      "allow extracting value to be logged" in {
        val logging = FlowWithContext[Message, Long].log("my-log2", m => m.data)
        Source(List(Message("a", 1L))).asSourceWithContext(m => m.offset).via(logging).asSource.runWith(Sink.ignore)

        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log2] Element: a"))
        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log2] Upstream finished."))
      }

      "allow disabling element logging" in {
        val disableElementLogging =
          Attributes.logLevels(onElement = LogLevels.Off, onFinish = Logging.DebugLevel, onFailure = Logging.DebugLevel)

        val logging = FlowWithContext[Message, Long].log("my-log3").withAttributes(disableElementLogging)
        Source(List(Message("a", 1L), Message("b", 2L)))
          .asSourceWithContext(m => m.offset)
          .via(logging)
          .asSource
          .runWith(Sink.ignore)

        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log3] Upstream finished."))
      }

    }

    "on SourceWithContext" must {

      "log each element" in {
        Source(List(Message("a", 1L), Message("b", 2L)))
          .asSourceWithContext(m => m.offset)
          .log("my-log4")
          .asSource
          .runWith(Sink.ignore)

        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log4] Element: Message(a,1)"))
        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log4] Element: Message(b,2)"))
        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log4] Upstream finished."))
      }

      "allow extracting value to be logged" in {
        Source(List(Message("a", 1L)))
          .asSourceWithContext(m => m.offset)
          .log("my-log5", m => m.data)
          .asSource
          .runWith(Sink.ignore)

        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log5] Element: a"))
        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log5] Upstream finished."))
      }

      "allow disabling element logging" in {
        val disableElementLogging =
          Attributes.logLevels(onElement = LogLevels.Off, onFinish = Logging.DebugLevel, onFailure = Logging.DebugLevel)

        Source(List(Message("a", 1L), Message("b", 2L)))
          .asSourceWithContext(m => m.offset)
          .log("my-log6")
          .withAttributes(disableElementLogging)
          .asSource
          .runWith(Sink.ignore)

        logProbe.expectMsg(Logging.Debug(LogSrc, LogClazz, "[my-log6] Upstream finished."))
      }

    }
  }

}
