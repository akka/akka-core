/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.testkit

import java.util.concurrent.ThreadLocalRandom

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.concurrent.duration._

import org.reactivestreams.Publisher
import org.scalatest.matchers.should.Matchers

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializerSettings
import akka.stream.Materializer
import akka.stream.SystemMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher._
import akka.stream.testkit.TestSubscriber._

trait ScriptedTest extends Matchers {

  class ScriptException(msg: String) extends RuntimeException(msg)

  def toPublisher[In, Out]: (Source[Out, _], Materializer) => Publisher[Out] =
    (f, m) => f.runWith(Sink.asPublisher(false))(m)

  object Script {
    def apply[In, Out](phases: (Seq[In], Seq[Out])*): Script[In, Out] = {
      var providedInputs = Vector[In]()
      var expectedOutputs = Vector[Out]()
      var jumps = Vector[Int]()

      for ((ins, outs) <- phases) {
        providedInputs ++= ins
        expectedOutputs ++= outs
        jumps ++= Vector.fill(ins.size - 1)(0) ++ Vector(outs.size)
      }

      new Script(
        providedInputs,
        expectedOutputs,
        jumps,
        inputCursor = 0,
        outputCursor = 0,
        outputEndCursor = 0,
        completed = false)
    }
  }

  final class Script[In, Out](
      val providedInputs: Vector[In],
      val expectedOutputs: Vector[Out],
      val jumps: Vector[Int],
      val inputCursor: Int,
      val outputCursor: Int,
      val outputEndCursor: Int,
      val completed: Boolean) {
    require(jumps.size == providedInputs.size)

    def provideInput: (In, Script[In, Out]) =
      if (noInsPending)
        throw new ScriptException("Script cannot provide more input.")
      else
        (
          providedInputs(inputCursor),
          new Script(
            providedInputs,
            expectedOutputs,
            jumps,
            inputCursor = inputCursor + 1,
            outputCursor,
            outputEndCursor = outputEndCursor + jumps(inputCursor),
            completed))

    def consumeOutput(out: Out): Script[In, Out] = {
      if (noOutsPending)
        throw new ScriptException(s"Tried to produce element ${out} but no elements should be produced right now.")
      out should be(expectedOutputs(outputCursor))
      new Script(
        providedInputs,
        expectedOutputs,
        jumps,
        inputCursor,
        outputCursor = outputCursor + 1,
        outputEndCursor,
        completed)
    }

    def complete(): Script[In, Out] = {
      if (finished)
        new Script(
          providedInputs,
          expectedOutputs,
          jumps,
          inputCursor,
          outputCursor = outputCursor + 1,
          outputEndCursor,
          completed = true)
      else fail("received onComplete prematurely")
    }

    def finished: Boolean = outputCursor == expectedOutputs.size

    def error(e: Throwable): Script[In, Out] = throw e

    def pendingOuts: Int = outputEndCursor - outputCursor
    def noOutsPending: Boolean = pendingOuts == 0
    def someOutsPending: Boolean = !noOutsPending

    def pendingIns: Int = providedInputs.size - inputCursor
    def noInsPending: Boolean = pendingIns == 0
    def someInsPending: Boolean = !noInsPending

    def debug: String =
      s"Script(pending=($pendingIns in, $pendingOuts out), remainingIns=${providedInputs
        .drop(inputCursor)
        .mkString("/")}, remainingOuts=${expectedOutputs.drop(outputCursor).mkString("/")})"
  }

  class ScriptRunner[In, Out, M](
      op: Flow[In, In, NotUsed] => Flow[In, Out, M],
      settings: ActorMaterializerSettings,
      script: Script[In, Out],
      maximumOverrun: Int,
      maximumRequest: Int,
      maximumBuffer: Int)(implicit _system: ActorSystem)
      extends ChainSetup(op, settings, toPublisher) {

    var _debugLog = Vector.empty[String]
    var currentScript = script
    var remainingDemand = script.expectedOutputs.size + ThreadLocalRandom.current().nextInt(1, maximumOverrun)
    debugLog(s"starting with remainingDemand=$remainingDemand")
    var pendingRequests = 0L
    var outstandingDemand = 0L
    var completed = false

    def getNextDemand(): Int = {
      val max = Math.min(remainingDemand, maximumRequest)
      if (max == 1) {
        remainingDemand = 0
        1
      } else {
        val demand = ThreadLocalRandom.current().nextInt(1, max)
        remainingDemand -= demand
        demand
      }
    }

    def debugLog(msg: String): Unit = _debugLog :+= msg

    def request(demand: Int): Unit = {
      debugLog(s"test environment requests $demand")
      downstreamSubscription.request(demand)
      outstandingDemand += demand
    }

    def mayProvideInput: Boolean =
      currentScript.someInsPending && (pendingRequests > 0) && (currentScript.pendingOuts <= maximumBuffer)
    def mayRequestMore: Boolean = remainingDemand > 0

    def shakeIt(): Boolean = {
      val u = upstream.receiveWhile(1.milliseconds) {
        case RequestMore(_, n) =>
          debugLog(s"operation requests $n")
          pendingRequests += n
          true
        case _ => false // Ignore
      }
      val d = downstream.receiveWhile(1.milliseconds) {
        case OnNext(elem: Out @unchecked) =>
          debugLog(s"operation produces [$elem]")
          if (outstandingDemand == 0) fail("operation produced while there was no demand")
          outstandingDemand -= 1
          currentScript = currentScript.consumeOutput(elem)
          true
        case OnComplete =>
          currentScript = currentScript.complete()
          true
        case OnError(e) =>
          currentScript = currentScript.error(e)
          true
        case _ => false // Ignore
      }
      (u ++ d).exists(x => x)
    }

    def run(): Unit = {

      @tailrec def doRun(idleRounds: Int): Unit = {
        if (idleRounds > 250) fail("too many idle rounds")
        if (!currentScript.completed) {
          val nextIdle = if (shakeIt()) 0 else idleRounds + 1

          val tieBreak = ThreadLocalRandom.current().nextBoolean()
          if (mayProvideInput && (!mayRequestMore || tieBreak)) {
            val (input, nextScript) = currentScript.provideInput
            debugLog(s"test environment produces [${input}]")
            pendingRequests -= 1
            currentScript = nextScript
            upstreamSubscription.sendNext(input)
            doRun(nextIdle)
          } else if (mayRequestMore && (!mayProvideInput || !tieBreak)) {
            request(getNextDemand())
            doRun(nextIdle)
          } else {
            if (currentScript.noInsPending && !completed) {
              debugLog("test environment completes")
              upstreamSubscription.sendComplete()
              completed = true
            }
            doRun(nextIdle)
          }

        }
      }

      try {
        debugLog(s"running $script")
        request(getNextDemand())
        doRun(0)
      } catch {
        case e: Throwable =>
          println(_debugLog.mkString("Steps leading to failure:\n", "\n", "\nCurrentScript: " + currentScript.debug))
          throw e
      }

    }

  }

  @nowarn("msg=deprecated")
  def runScript[In, Out, M](script: Script[In, Out])(op: Flow[In, In, NotUsed] => Flow[In, Out, M])(
      implicit system: ActorSystem): Unit =
    runScript(script, SystemMaterializer(system).materializer.settings)(op)(system)

  def runScript[In, Out, M](
      script: Script[In, Out],
      settings: ActorMaterializerSettings,
      maximumOverrun: Int = 3,
      maximumRequest: Int = 3,
      maximumBuffer: Int = 3)(op: Flow[In, In, NotUsed] => Flow[In, Out, M])(implicit system: ActorSystem): Unit = {
    new ScriptRunner(op, settings, script, maximumOverrun, maximumRequest, maximumBuffer).run()
  }

}
