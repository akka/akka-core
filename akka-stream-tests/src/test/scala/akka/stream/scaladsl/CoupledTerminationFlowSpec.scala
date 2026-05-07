/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.xml.Node

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.scalatest.Assertion

import akka.Done
import akka.NotUsed
import akka.stream._
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestProbe

class CoupledTerminationFlowSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) with ScriptedTest {

  import system.dispatcher

  /**
   * The table of effects is exactly the same one as in the scaladoc of the class,
   * so we're able to directly copy that table to check we wrote correct docs.
   */
  val effectsTable =
    <table>
      <tr>
        <td>flow <i>cause:</i> upstream (sink-side) receives completion</td>
        <td>sink <i>effect:</i> receives completion</td>
        <td>source <i>effect:</i> receives cancel</td>
      </tr>
      <tr>
        <td>flow <i>cause:</i> upstream (sink-side) receives error</td>
        <td>sink <i>effect:</i> receives error</td>
        <td>source <i>effect:</i> receives cancel</td>
      </tr>
      <tr>
        <td>flow <i>cause:</i> downstream (source-side) receives cancel</td>
        <td>sink <i>effect:</i> completes</td>
        <td>source <i>effect:</i> receives cancel</td>
      </tr>
      <tr>
        <td>flow <i>effect:</i> cancels upstream, completes downstream</td>
        <td>sink <i>effect:</i> completes</td>
        <td>source <i>cause:</i> signals complete</td>
      </tr>
      <tr>
        <td>flow <i>effect:</i> cancels upstream, errors downstream</td>
        <td>sink <i>effect:</i> receives error</td>
        <td>source <i>cause:</i> signals error or throws</td>
      </tr>
      <tr>
        <td>flow <i>effect:</i> cancels upstream, completes downstream</td>
        <td>sink <i>cause:</i> cancels</td>
        <td>source <i>effect:</i> receives cancel</td>
      </tr>
    </table>

  "Completion" must {
    (effectsTable \ "tr").foreach { testCase =>
      val rules = testCase \\ "td"
      val outerRule = rules.head.toString()
      val innerSinkRule = rules.drop(1).head.toString()
      val innerSourceRule = rules.drop(2).head.toString()

      s"test interpreter: ${testName(testCase)}" in {

        val (outerSource, outerSink, outerAssertions) = interpretOuter(outerRule)
        val (innerSink, innerSinkAssertion) = interpretInnerSink(innerSinkRule)
        val (innerSource, innerSourceAssertion) = interpretInnerSource(innerSourceRule)

        val flow = Flow.fromSinkAndSourceCoupledMat(innerSink, innerSource)(Keep.none)
        outerSource.via(flow).to(outerSink).run()

        outerAssertions()
        innerSinkAssertion()
        innerSourceAssertion()
      }
    }

    "completed out:Source => complete in:Sink" in {
      val probe = TestProbe()
      val f = Flow.fromSinkAndSourceCoupledMat(Sink.onComplete(_ => probe.ref ! "done"), Source.empty)(Keep.none) // completes right away, should complete the sink as well

      f.runWith(Source.maybe, Sink.ignore) // these do nothing.

      probe.expectMsg("done")
    }

    "cancel in:Sink => cancel out:Source" in {
      val probe = TestProbe()
      val f = Flow.fromSinkAndSourceCoupledMat(Sink.cancelled, Source.fromPublisher(new Publisher[String] {
        override def subscribe(subscriber: Subscriber[_ >: String]): Unit = {
          subscriber.onSubscribe(new Subscription {
            override def cancel(): Unit = probe.ref ! "cancelled"

            override def request(l: Long): Unit = () // do nothing
          })
        }
      }))(Keep.none) // completes right away, should complete the sink as well

      f.runWith(Source.maybe, Sink.ignore) // these do nothing.

      probe.expectMsg("cancelled")
    }

    "error wrapped Sink when wrapped Source errors " in {
      val probe = TestProbe()
      val f = Flow.fromSinkAndSourceCoupledMat(
        Sink.onComplete(e => probe.ref ! e.failed.get.getMessage),
        Source.failed(new Exception("BOOM!")))(Keep.none) // completes right away, should complete the sink as well

      f.runWith(Source.maybe, Sink.ignore) // these do nothing.

      probe.expectMsg("BOOM!")
    }

    "support usage with Graphs" in {
      val source: Graph[SourceShape[Int], TestPublisher.Probe[Int]] = TestSource[Int]()
      val sink: Graph[SinkShape[Any], Future[Done]] = Sink.ignore

      val flow = Flow.fromSinkAndSourceCoupledMat(sink, source)(Keep.right)

      val (source1, source2) = TestSource[Int]().viaMat(flow)(Keep.both).toMat(Sink.ignore)(Keep.left).run()

      source1.sendComplete()
      source2.expectCancellation()
    }

  }

  def interpretOuter(rule: String): (Source[String, NotUsed], Sink[String, NotUsed], () => Any) = {
    val probe = TestProbe()
    val causeUpstreamCompletes = Source.empty[String]
    val causeUpstreamErrors = Source.failed(new Exception("Boom"))
    val causeDownstreamCancels = Sink.cancelled[String]

    val downstreamEffect = Sink.onComplete(s => probe.ref ! s)
    val upstreamEffect = Source.fromPublisher(new Publisher[String] {
      override def subscribe(s: Subscriber[_ >: String]): Unit =
        s.onSubscribe(new Subscription {
          override def cancel(): Unit = probe.ref ! "cancel-received"

          override def request(n: Long): Unit = ()
        })
    })
    val assertCancel = () => {
      val m = probe.expectMsgType[String]
      m should ===("cancel-received")
    }
    val assertComplete = () => {
      val m = probe.expectMsgType[Try[Done]]
      m.isFailure should ===(false)
    }
    val assertCompleteAndCancel = () => {
      probe.expectMsgPF() {
        case Success(_)        => // good
        case "cancel-received" => // good
      }
      probe.expectMsgPF() {
        case Success(_)        => // good
        case "cancel-received" => // good
      }
    }
    val assertError = () => {
      val m = probe.expectMsgType[Try[Done]]
      m.isFailure should ===(true)
      m.failed.get.getMessage should include("Boom")
    }
    val assertErrorAndCancel = () => {
      probe.expectMsgPF() {
        case Failure(_)        => // good
        case "cancel-received" => // good
      }
      probe.expectMsgPF() {
        case Failure(_)        => // good
        case "cancel-received" => // good
      }
    }

    if (rule contains "cause") {
      if (rule.contains("upstream") && (rule.contains("completion") || rule.contains("complete")))
        (causeUpstreamCompletes, downstreamEffect, assertComplete)
      else if (rule.contains("upstream") && rule.contains("error"))
        (causeUpstreamErrors, downstreamEffect, assertError)
      else if (rule.contains("downstream") && rule.contains("cancel"))
        (upstreamEffect, causeDownstreamCancels, assertCancel)
      else
        throw UnableToInterpretRule(rule)
    } else if (rule contains "effect") {
      if (rule.contains("cancel") && rule.contains("complete"))
        (upstreamEffect, downstreamEffect, assertCompleteAndCancel)
      else if (rule.contains("cancel") && rule.contains("error"))
        (upstreamEffect, downstreamEffect, assertErrorAndCancel)
      else throw UnableToInterpretRule(rule)
    } else throw UnableToInterpretRule(rule)
  }

  def interpretInnerSink(rule: String): (Sink[String, NotUsed], () => Assertion) = {
    val probe = TestProbe()
    val causeCancel = Sink.cancelled[String]

    val catchEffect = Sink.onComplete(s => probe.ref ! s)
    val assertComplete = () => {
      val m = probe.expectMsgType[Try[Done]]
      m.isFailure should ===(false)
    }
    val assertError = () => {
      val m = probe.expectMsgType[Try[Done]]
      m.isFailure should ===(true)
      m.failed.get.getMessage should include("Boom")
    }
    val assertionOK = () => 1 should ===(1)

    if (rule contains "cause") {
      if (rule.contains("cancels")) (causeCancel, assertionOK)
      else throw UnableToInterpretRule(rule)
    } else if (rule contains "effect") {
      if (rule.contains("complete") || rule.contains("completion")) (catchEffect, assertComplete)
      else if (rule.contains("error")) (catchEffect, assertError)
      else throw UnableToInterpretRule(rule)
    } else throw UnableToInterpretRule(rule)
  }

  def interpretInnerSource(rule: String): (Source[String, NotUsed], () => Assertion) = {
    val probe = TestProbe()
    val causeComplete = Source.empty[String]
    val causeError = Source.failed(new Exception("Boom"))

    val catchEffect = Source
      .maybe[String]
      .mapMaterializedValue(p => {
        p.future.onComplete(t => probe.ref ! t)
        NotUsed
      })
    val assertCancel = () => {
      val m = probe.expectMsgType[Try[Option[String]]]
      m.isFailure should ===(false)
      m.get should ===(None) // downstream cancelled
    }
    val assertionOK = () => 1 should ===(1)

    if (rule contains "cause") {
      if (rule.contains("complete")) (causeComplete, assertionOK)
      else if (rule.contains("error")) (causeError, assertionOK)
      else ???
    } else if (rule contains "effect") {
      (catchEffect, assertCancel)
    } else ???
  }

  def testName(n: Node): String =
    n.text.split("\n").drop(1).map(_.trim).filterNot(_.isEmpty).mkString(", ")

  case class UnableToInterpretRule(msg: String) extends AssertionError("Unable to interpret rule: " + msg)

}
