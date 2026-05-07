/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.concurrent.duration._
import scala.language.postfixOps

import com.codahale.metrics.Histogram
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

import akka.testkit.{ AkkaSpec, ImplicitSender, PerformanceTest }
import akka.testkit.metrics._
import akka.testkit.metrics.HeapMemoryUsage

object ActorCreationPerfSpec {

  val config = ConfigFactory.parseString("""
    akka.test.actor.ActorPerfSpec {
      warmUp = 5
      numberOfActors = 10
      numberOfRepeats = 1
      force-gc = off
      report-metrics = off
      # For serious measurements use something like the following values
      #warmUp = 50000
      #numberOfActors = 100000
      #numberOfRepeats = 3
      #force-gc = on
      #report-metrics = on
    }
    """)

  final case class Create(number: Int, props: () => Props)
  case object Created
  case object IsAlive
  case object Alive
  case object WaitForChildren
  case object Waited

  class EmptyActor extends Actor {
    def receive = {
      case IsAlive => sender() ! Alive
    }
  }

  class EmptyArgsActor(val foo: Int, val bar: Int) extends Actor {
    def receive = {
      case IsAlive => sender() ! Alive
    }
  }

  class TimingDriver(hist: Histogram) extends Actor {

    def receive = {
      case IsAlive =>
        sender() ! Alive
      case Create(number, propsCreator) =>
        for (_ <- 1 to number) {
          val start = System.nanoTime()
          context.actorOf(propsCreator.apply())
          // yes, we are aware of this being skewed
          val stop = System.nanoTime()
          hist.update(stop - start)
        }

        sender() ! Created
      case WaitForChildren =>
        context.children.foreach(_ ! IsAlive)
        context.become(waiting(context.children.size, sender()), discardOld = false)
    }

    def waiting(number: Int, replyTo: ActorRef): Receive = {
      var current = number

      {
        case Alive =>
          current -= 1
          if (current == 0) {
            replyTo ! Waited
            context.unbecome()
          }
      }
    }
  }

  class Driver extends Actor {

    def receive = {
      case IsAlive =>
        sender() ! Alive
      case Create(number, propsCreator) =>
        for (_ <- 1 to number) {
          context.actorOf(propsCreator.apply())
        }
        sender() ! Created
      case WaitForChildren =>
        context.children.foreach(_ ! IsAlive)
        context.become(waiting(context.children.size, sender()), discardOld = false)
    }

    def waiting(number: Int, replyTo: ActorRef): Receive = {
      var current = number

      {
        case Alive =>
          current -= 1
          if (current == 0) {
            replyTo ! Waited
            context.unbecome()
          }
      }
    }
  }
}

class ActorCreationPerfSpec
    extends AkkaSpec(ActorCreationPerfSpec.config)
    with ImplicitSender
    with MetricsKit
    with BeforeAndAfterAll {

  import ActorCreationPerfSpec._

  def metricsConfig = system.settings.config
  val ActorCreationKey = MetricKey.fromString("actor-creation")
  val BlockingTimeKey = ActorCreationKey / "synchronous-part"
  val TotalTimeKey = ActorCreationKey / "total"

  val warmUp = metricsConfig.getInt("akka.test.actor.ActorPerfSpec.warmUp")
  val nrOfActors = metricsConfig.getInt("akka.test.actor.ActorPerfSpec.numberOfActors")
  val nrOfRepeats = metricsConfig.getInt("akka.test.actor.ActorPerfSpec.numberOfRepeats")
  override val reportMetricsEnabled = metricsConfig.getBoolean("akka.test.actor.ActorPerfSpec.report-metrics")
  override val forceGcEnabled = metricsConfig.getBoolean("akka.test.actor.ActorPerfSpec.force-gc")

  def runWithCounterInside(metricName: String, scenarioName: String, number: Int, propsCreator: () => Props): Unit = {
    val hist = histogram(BlockingTimeKey / metricName)

    val driver = system.actorOf(Props(classOf[TimingDriver], hist), scenarioName)
    driver ! IsAlive
    expectMsg(Alive)

    driver ! Create(number, propsCreator)
    expectMsgPF(15 seconds, s"$scenarioName waiting for Created") { case Created => }

    driver ! WaitForChildren
    expectMsgPF(15 seconds, s"$scenarioName waiting for Waited") { case Waited => }

    driver ! PoisonPill
    watch(driver)
    expectTerminated(driver, 15.seconds)
    gc()
  }

  def runWithoutCounter(scenarioName: String, number: Int, propsCreator: () => Props): HeapMemoryUsage = {
    val mem = measureMemory(TotalTimeKey / scenarioName)

    val driver = system.actorOf(Props(classOf[Driver]), scenarioName)
    driver ! IsAlive
    expectMsg(Alive)

    gc()
    val before = mem.getHeapSnapshot

    driver ! Create(number, propsCreator)
    expectMsgPF(15 seconds, s"$scenarioName waiting for Created") { case Created => }

    driver ! WaitForChildren
    expectMsgPF(15 seconds, s"$scenarioName waiting for Waited") { case Waited => }

    gc()
    val after = mem.getHeapSnapshot

    driver ! PoisonPill
    watch(driver)
    expectTerminated(driver, 15.seconds)

    after.diff(before)
  }

  def registerTests(name: String, propsCreator: () => Props): Unit = {
    val scenarioName = name.replaceAll("""[^\w]""", "")

    s"warm-up before: $name" taggedAs PerformanceTest in {
      if (warmUp > 0) {
        runWithoutCounter(s"${scenarioName}_warmup", warmUp, propsCreator)
      }

      clearMetrics()
    }

    s"measure synchronous blocked time for $name" taggedAs PerformanceTest in {
      // note: measuring per-actor-memory-use in this scenario is skewed as the Actor contains references to counters etc!
      //       for measuring actor size use refer to the `runWithoutCounter` method
      for (i <- 1 to nrOfRepeats) {
        runWithCounterInside(name, s"${scenarioName}_driver_inside_$i", nrOfActors, propsCreator)
      }

      reportAndClearMetrics()
    }

    s"measure total creation time for $name" taggedAs PerformanceTest in {
      val avgMem = averageGauge(ActorCreationKey / name / "avg-mem-per-actor")

      for (i <- 1 to nrOfRepeats) {
        val heapUsed = timedWithKnownOps(TotalTimeKey / s"creating-$nrOfActors-actors" / name, ops = nrOfActors) {
          runWithoutCounter(s"${scenarioName}_driver_outside_$i", nrOfActors, propsCreator)
        }

        avgMem.add(heapUsed.used / nrOfActors) // average actor size, over nrOfRepeats
        // time is handled by the histogram already
      }

      reportAndClearMetrics()
    }
  }

  "Actor creation with actorOf" must {

    registerTests("Props[EmptyActor] with new Props", () => Props[EmptyActor]())

    val props1 = Props[EmptyActor]()
    registerTests("Props[EmptyActor] with same Props", () => props1)

    registerTests("Props(new EmptyActor) new", () => { Props(new EmptyActor) })

    val props2 = Props(new EmptyActor)
    registerTests("Props(new EmptyActor) same", () => { props2 })

    registerTests("Props(classOf[EmptyArgsActor], ...) new", () => { Props(classOf[EmptyArgsActor], 4711, 1729) })

    val props3 = Props(classOf[EmptyArgsActor], 4711, 1729)
    registerTests("Props(classOf[EmptyArgsActor], ...) same", () => { props3 })

    registerTests("Props(new EmptyArgsActor(...)) new", () => { Props(new EmptyArgsActor(4711, 1729)) })

    val props4 = Props(new EmptyArgsActor(4711, 1729))
    registerTests("Props(new EmptyArgsActor(...)) same", () => { props4 })
  }

  override def afterTermination() = shutdownMetrics()

  override def expectedTestDuration = 5 minutes
}
