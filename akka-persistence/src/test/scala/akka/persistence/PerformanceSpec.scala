/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.testkit._

object PerformanceSpec {
  val config =
    """
      akka.persistence.performance.cycles.load = 100
      # more accurate throughput measurements
      #akka.persistence.performance.cycles.load = 200000
    """

  case object StopMeasure
  final case class FailAt(sequenceNr: Long)

  class Measure(numberOfMessages: Int) {
    private val NanoToSecond = 1000.0 * 1000 * 1000

    private var startTime: Long = 0L
    private var stopTime: Long = 0L

    def startMeasure(): Unit = {
      startTime = System.nanoTime
    }

    def stopMeasure(): Double = {
      stopTime = System.nanoTime
      (NanoToSecond * numberOfMessages / (stopTime - startTime))
    }
  }

  abstract class PerformanceTestPersistentActor(name: String) extends NamedPersistentActor(name) {
    var failAt: Long = -1

    override val receiveRecover: Receive = {
      case _ => if (lastSequenceNr % 1000 == 0) print("r")
    }

    val controlBehavior: Receive = {
      case StopMeasure        => deferAsync(StopMeasure)(_ => sender() ! StopMeasure)
      case FailAt(sequenceNr) => failAt = sequenceNr
    }

  }

  class CommandsourcedTestPersistentActor(name: String) extends PerformanceTestPersistentActor(name) {

    override val receiveCommand: Receive = controlBehavior.orElse {
      case cmd =>
        persistAsync(cmd) { _ =>
          if (lastSequenceNr % 1000 == 0) print(".")
          if (lastSequenceNr == failAt) throw new TestException("boom")
        }
    }
  }

  class EventsourcedTestPersistentActor(name: String) extends PerformanceTestPersistentActor(name) {

    override val receiveCommand: Receive = controlBehavior.orElse {
      case cmd =>
        persist(cmd) { _ =>
          if (lastSequenceNr % 1000 == 0) print(".")
          if (lastSequenceNr == failAt) throw new TestException("boom")
        }
    }
  }

  /**
   * `persist` every 10th message, otherwise `persistAsync`
   */
  class MixedTestPersistentActor(name: String) extends PerformanceTestPersistentActor(name) {
    var counter = 0

    val handler: Any => Unit = { _ =>
      if (lastSequenceNr % 1000 == 0) print(".")
      if (lastSequenceNr == failAt) throw new TestException("boom")
    }

    val receiveCommand: Receive = controlBehavior.orElse {
      case cmd =>
        counter += 1
        if (counter % 10 == 0) persist(cmd)(handler)
        else persistAsync(cmd)(handler)
    }
  }

  class StashingEventsourcedTestPersistentActor(name: String) extends PerformanceTestPersistentActor(name) {

    val printProgress: PartialFunction[Any, Any] = {
      case m => if (lastSequenceNr % 1000 == 0) print("."); m
    }

    val receiveCommand: Receive = printProgress.andThen(controlBehavior.orElse {
      case "a" => persist("a")(_ => context.become(processC))
      case "b" => persist("b")(_ => ())
    })

    val processC: Receive = printProgress.andThen {
      case "c" =>
        persist("c")(_ => context.unbecome())
        unstashAll()
      case _ => stash()
    }
  }
}

class PerformanceSpec
    extends PersistenceSpec(
      PersistenceSpec
        .config("inmem", "PerformanceSpec", serialization = "off")
        .withFallback(ConfigFactory.parseString(PerformanceSpec.config)))
    with ImplicitSender {
  import PerformanceSpec._

  val loadCycles = system.settings.config.getInt("akka.persistence.performance.cycles.load")

  def stressPersistentActor(persistentActor: ActorRef, failAt: Option[Long], description: String): Unit = {
    failAt.foreach { persistentActor ! FailAt(_) }
    val m = new Measure(loadCycles)
    m.startMeasure()
    (1 to loadCycles).foreach { i =>
      persistentActor ! s"msg${i}"
    }
    persistentActor ! StopMeasure
    expectMsg(100.seconds, StopMeasure)
    println(f"\nthroughput = ${m.stopMeasure()}%.2f $description per second")
  }

  def stressCommandsourcedPersistentActor(failAt: Option[Long]): Unit = {
    val persistentActor = namedPersistentActor[CommandsourcedTestPersistentActor]
    stressPersistentActor(persistentActor, failAt, "persistent commands")
  }

  def stressEventSourcedPersistentActor(failAt: Option[Long]): Unit = {
    val persistentActor = namedPersistentActor[EventsourcedTestPersistentActor]
    stressPersistentActor(persistentActor, failAt, "persistent events")
  }

  def stressMixedPersistentActor(failAt: Option[Long]): Unit = {
    val persistentActor = namedPersistentActor[MixedTestPersistentActor]
    stressPersistentActor(persistentActor, failAt, "persistent events & commands")
  }

  def stressStashingPersistentActor(): Unit = {
    val persistentActor = namedPersistentActor[StashingEventsourcedTestPersistentActor]
    val m = new Measure(loadCycles)
    m.startMeasure()
    val cmds = (1 to (loadCycles / 3)).flatMap(_ => List("a", "b", "c"))
    cmds.foreach(persistentActor ! _)
    persistentActor ! StopMeasure
    expectMsg(100.seconds, StopMeasure)
    println(f"\nthroughput = ${m.stopMeasure()}%.2f persistent events per second")
  }

  "Warmup persistent actor" should {
    "exercise" in {
      stressCommandsourcedPersistentActor(None)
    }
    "exercise some more" in {
      stressCommandsourcedPersistentActor(None)
    }
  }

  "A command sourced persistent actor" should {
    "have some reasonable throughput" in {
      stressCommandsourcedPersistentActor(None)
    }
  }

  "An event sourced persistent actor" should {
    "have some reasonable throughput" in {
      stressEventSourcedPersistentActor(None)
    }
    "have some reasonable throughput under failure conditions" in {
      stressEventSourcedPersistentActor(Some(loadCycles / 10))
    }
    "have some reasonable throughput with stashing and unstashing every 3rd command" in {
      stressStashingPersistentActor()
    }
  }

  "A mixed command and event sourced persistent actor" should {
    "have some reasonable throughput" in {
      stressMixedPersistentActor(None)
    }
  }

}
