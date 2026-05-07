/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import java.nio.charset.StandardCharsets

import scala.collection.immutable
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.annotation.InternalApi
import akka.persistence.PersistentActor
import akka.persistence.journal.JournalPerfSpec.BenchActor
import akka.persistence.journal.JournalPerfSpec.Cmd
import akka.persistence.journal.JournalPerfSpec.ResetCounter
import akka.serialization.SerializerWithStringManifest
import akka.testkit.TestProbe

object JournalPerfSpec {
  class BenchActor(override val persistenceId: String, replyTo: ActorRef, replyAfter: Int)
      extends PersistentActor
      with ActorLogging {

    var counter = 0

    override def receiveCommand: Receive = {
      case c @ Cmd("p", _) =>
        persist(c) { d =>
          counter += 1
          require(d.payload == counter, s"Expected to receive [$counter] yet got: [${d.payload}]")
          if (counter == replyAfter) replyTo ! d.payload
        }

      case c @ Cmd("pa", _) =>
        persistAsync(c) { d =>
          counter += 1
          require(d.payload == counter, s"Expected to receive [$counter] yet got: [${d.payload}]")
          if (counter == replyAfter) replyTo ! d.payload
        }

      case c @ Cmd("par", payload) =>
        counter += 1
        persistAsync(c) { d =>
          require(d.payload == counter, s"Expected to receive [$counter] yet got: [${d.payload}]")
        }
        if (counter == replyAfter) replyTo ! payload

      case Cmd("n", payload) =>
        counter += 1
        require(payload == counter, s"Expected to receive [$counter] yet got: [${payload}]")
        if (counter == replyAfter) replyTo ! payload

      case ResetCounter =>
        counter = 0
    }

    override def receiveRecover: Receive = {
      case Cmd(_, payload) =>
        counter += 1
        require(payload == counter, s"Expected to receive [$counter] yet got: [${payload}]")
        if (counter == replyAfter) replyTo ! payload
    }

  }

  case object ResetCounter
  case class Cmd(mode: String, payload: Int)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] class CmdSerializer extends SerializerWithStringManifest {
    override def identifier: Int = 293562

    override def manifest(o: AnyRef): String = ""

    override def toBinary(o: AnyRef): Array[Byte] =
      o match {
        case Cmd(mode, payload) =>
          s"$mode|$payload".getBytes(StandardCharsets.UTF_8)
        case _ =>
          throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
      }

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
      val str = new String(bytes, StandardCharsets.UTF_8)
      val i = str.indexOf('|')
      Cmd(str.substring(0, i), str.substring(i + 1).toInt)
    }
  }

  private val cmdSerializerConfig = ConfigFactory.parseString(s"""
  akka.actor {
    serializers {
      JournalPerfSpec = "${classOf[CmdSerializer].getName}"
    }
    serialization-bindings {
      "${classOf[Cmd].getName}" = JournalPerfSpec
    }
  }  
  """)
}

/**
 * This spec measures execution times of the basic operations that an [[akka.persistence.PersistentActor]] provides,
 * using the provided Journal (plugin).
 *
 * It is *NOT* meant to be a comprehensive benchmark, but rather aims to help plugin developers to easily determine
 * if their plugin's performance is roughly as expected. It also validates the plugin still works under "more messages" scenarios.
 *
 * In case your journal plugin needs some kind of setup or teardown, override the `beforeAll` or `afterAll`
 * methods (don't forget to call `super` in your overridden methods).
 *
 * For a Java and JUnit consumable version of the TCK please refer to [[akka.persistence.japi.journal.JavaJournalPerfSpec]].
 *
 * @see [[akka.persistence.journal.JournalSpec]]
 */
abstract class JournalPerfSpec(config: Config)
    extends JournalSpec(config.withFallback(JournalPerfSpec.cmdSerializerConfig)) {

  private val testProbe = TestProbe()

  def benchActor(replyAfter: Int): ActorRef =
    system.actorOf(Props(new BenchActor(pid, testProbe.ref, replyAfter)))

  def feedAndExpectLast(actor: ActorRef, mode: String, cmnds: immutable.Seq[Int]): Unit = {
    cmnds.foreach { c =>
      actor ! Cmd(mode, c)
    }
    testProbe.expectMsg(awaitDuration, cmnds.last)
  }

  /** Executes a block of code multiple times (no warm-up) */
  def measure(msg: Duration => String)(block: => Unit): Unit = {
    val measurements = new Array[Duration](measurementIterations)
    var i = 0
    while (i < measurementIterations) {
      val start = System.nanoTime()

      block

      val stop = System.nanoTime()
      val d = (stop - start).nanos
      measurements(i) = d
      info(msg(d))

      i += 1
    }
    info(s"Average time: ${(measurements.map(_.toNanos).sum / measurementIterations).nanos.toMillis} ms")
  }

  /** Override in order to customize timeouts used for expectMsg, in order to tune the awaits to your journal's perf */
  def awaitDurationMillis: Long = 10.seconds.toMillis

  /** Override in order to customize timeouts used for expectMsg, in order to tune the awaits to your journal's perf */
  private def awaitDuration: FiniteDuration = awaitDurationMillis.millis

  /** Number of messages sent to the PersistentActor under test for each test iteration */
  def eventsCount: Int = 10 * 1000

  /** Number of measurement iterations each test will be run. */
  def measurementIterations: Int = 10

  private val commands = Vector(1 to eventsCount: _*)

  "A PersistentActor's performance" must {
    s"measure: persistAsync()-ing $eventsCount events" in {
      val p1 = benchActor(eventsCount)

      measure(d => s"PersistAsync()-ing $eventsCount took ${d.toMillis} ms") {
        feedAndExpectLast(p1, "pa", commands)
        p1 ! ResetCounter
      }
    }
    s"measure: persist()-ing $eventsCount events" in {
      val p1 = benchActor(eventsCount)

      measure(d => s"Persist()-ing $eventsCount took ${d.toMillis} ms") {
        feedAndExpectLast(p1, "p", commands)
        p1 ! ResetCounter
      }
    }
    s"measure: recovering $eventsCount events" in {
      val p1 = benchActor(eventsCount)
      feedAndExpectLast(p1, "p", commands)

      measure(d => s"Recovering $eventsCount took ${d.toMillis} ms") {
        benchActor(eventsCount)
        testProbe.expectMsg(max = awaitDuration, commands.last)
      }

    }
  }

}
