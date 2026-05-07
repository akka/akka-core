/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import java.util.UUID

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.PersistenceTestKitSnapshotPlugin
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.scaladsl.EventSourcedBehavior.CommandHandler

object PerformanceSpec {

  val config =
    """
      akka.persistence.performance.cycles.load = 100
      # more accurate throughput measurements
      #akka.persistence.performance.cycles.load = 10000
      # no stash capacity limit
      akka.persistence.typed.stash-capacity = 1000000
    """

  sealed trait Command

  case object StopMeasure extends Command with Reply

  case class FailAt(sequence: Long) extends Command

  case class CommandWithEvent(evt: String) extends Command

  sealed trait Reply

  case object ExpectedFail extends Reply

  class Measure(numberOfMessages: Int) {
    private val NanoToSecond = 1000.0 * 1000 * 1000

    private var startTime: Long = 0L
    private var stopTime: Long = 0L

    def startMeasure(): Unit = {
      startTime = System.nanoTime
    }

    def stopMeasure(): Double = {
      stopTime = System.nanoTime
      NanoToSecond * numberOfMessages / (stopTime - startTime)
    }
  }

  case class Parameters(var persistCalls: Long = 0L, var failAt: Long = -1) {
    def every(num: Long): Boolean = persistCalls % num == 0

    def shouldFail: Boolean =
      failAt != -1 && persistCalls % failAt == 0

    def failureWasDefined: Boolean = failAt != -1L
  }

  def behavior(name: String, probe: TestProbe[Reply])(other: (Command, Parameters) => Effect[String, String]) = {
    Behaviors
      .supervise({
        val parameters = Parameters()
        EventSourcedBehavior[Command, String, String](
          persistenceId = PersistenceId.ofUniqueId(name),
          "",
          commandHandler = CommandHandler.command {
            case StopMeasure =>
              Effect.none.thenRun(_ => probe.ref ! StopMeasure)
            case FailAt(sequence) =>
              Effect.none.thenRun(_ => parameters.failAt = sequence)
            case command => other(command, parameters)
          },
          eventHandler = {
            case (state, _) => state
          }).receiveSignal {
          case (_, RecoveryCompleted) =>
            if (parameters.every(1000)) print("r")
        }
      })
      .onFailure(SupervisorStrategy.restart.withLoggingEnabled(false))
  }

  def eventSourcedTestPersistenceBehavior(name: String, probe: TestProbe[Reply]) =
    behavior(name, probe) {
      case (CommandWithEvent(evt), parameters) =>
        Effect
          .persist(evt)
          .thenRun(_ => {
            parameters.persistCalls += 1
            if (parameters.every(1000)) print(".")
            if (parameters.shouldFail) {
              probe.ref ! ExpectedFail
              throw TestException("boom")
            }
          })
      case _ => Effect.none
    }
}

class PerformanceSpec
    extends ScalaTestWithActorTestKit(
      PersistenceTestKitPlugin.config
        .withFallback(PersistenceTestKitSnapshotPlugin.config)
        .withFallback(ConfigFactory.parseString("""
      akka.persistence.publish-plugin-commands = on
      akka.actor.testkit.typed.single-expect-default = 10s
      """))
        .withFallback(ConfigFactory.parseString(PerformanceSpec.config)))
    with AnyWordSpecLike
    with LogCapturing {

  import PerformanceSpec._

  val loadCycles = system.settings.config.getInt("akka.persistence.performance.cycles.load")

  def stressPersistentActor(
      persistentActor: ActorRef[Command],
      probe: TestProbe[Reply],
      failAt: Option[Long],
      description: String): Unit = {
    failAt.foreach { persistentActor ! FailAt(_) }
    val m = new Measure(loadCycles)
    m.startMeasure()
    val parameters = Parameters(0, failAt = failAt.getOrElse(-1))
    (1 to loadCycles).foreach { n =>
      parameters.persistCalls += 1
      persistentActor ! CommandWithEvent(s"msg$n")
      // stash is cleared when exception is thrown so have to wait before sending more commands
      if (parameters.shouldFail)
        probe.expectMessage(ExpectedFail)
    }
    persistentActor ! StopMeasure
    probe.expectMessage(100.seconds, StopMeasure)
    println(f"\nthroughput = ${m.stopMeasure()}%.2f $description per second")
  }

  def stressEventSourcedPersistentActor(failAt: Option[Long]): Unit = {
    val probe = TestProbe[Reply]()
    val name = s"${this.getClass.getSimpleName}-${UUID.randomUUID().toString}"
    val persistentActor = spawn(eventSourcedTestPersistenceBehavior(name, probe), name)
    stressPersistentActor(persistentActor, probe, failAt, "persistent events")
  }

  "An event sourced persistent actor" should {
    "have some reasonable throughput" in {
      stressEventSourcedPersistentActor(None)
    }
    "have some reasonable throughput under failure conditions" in {
      stressEventSourcedPersistentActor(Some((loadCycles / 10).toLong))
    }
  }
}
