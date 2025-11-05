/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

import org.scalatest.concurrent.ScalaFutures

import akka.Done
import akka.testkit.AkkaSpec
import akka.testkit.TestException
import akka.testkit.TestProbe
import akka.util.Timeout

class StatusReplySpec extends AkkaSpec with ScalaFutures {

  "StatusReply" should {
    "pattern match success" in {
      // like in a classic actor receive Any => ...
      (StatusReply.Success("woho!"): Any) match {
        case StatusReply.Success(_: Int)                          => fail()
        case StatusReply.Success(text: String) if text == "woho!" =>
        case _                                                    => fail()
      }
    }
    "pattern match success (Ack)" in {
      // like in a classic actor receive Any => ...
      (StatusReply.Ack: Any) match {
        case StatusReply.Ack =>
        case _               => fail()
      }
    }
    "not throw exception if null" in {
      (null: StatusReply[_]) match {
        case StatusReply.Success(_) => fail()
        case StatusReply.Error(_)   => fail()
        case _                      =>
      }
    }
    "pattern match error with text" in {
      StatusReply.Error("boho!") match {
        case StatusReply.Error(_) =>
        case _                    => fail()
      }
    }

    "pattern match error with exception" in {
      StatusReply.Error(TestException("boho!")) match {
        case StatusReply.Error(_) =>
        case _                    => fail()
      }
    }

    "include exception type in toString for non text-error" in {
      StatusReply.Error(TestException("boho!")).toString should include("TestException")
    }

    "transform scala.util.Try" in {
      StatusReply.fromTry(scala.util.Success("woho")) should matchPattern {
        case StatusReply.Success("woho") =>
      }
      StatusReply.fromTry(scala.util.Failure(TestException("boho"))) should matchPattern {
        case StatusReply.Error(StatusReply.ErrorMessage("boho")) =>
      }
      StatusReply.fromTryKeepException(scala.util.Success("woho")) should matchPattern {
        case StatusReply.Success("woho") =>
      }
      StatusReply.fromTryKeepException(scala.util.Failure(TestException("boho"))) should matchPattern {
        case StatusReply.Error(TestException("boho")) =>
      }
    }

    "flatten a Future[StatusReply]" in {
      import system.dispatcher
      StatusReply.flattenStatusFuture(Future(StatusReply.Success("woho"))).futureValue should ===("woho")
      StatusReply.flattenStatusFuture(Future(StatusReply.Ack)).futureValue should ===(Done)
      StatusReply.flattenStatusFuture(Future(StatusReply.Error("boo"))).failed.futureValue should ===(
        StatusReply.ErrorMessage("boo"))
      StatusReply.flattenStatusFuture(Future(StatusReply.Error(TestException("boo")))).failed.futureValue should ===(
        TestException("boo"))

    }
  }

  "askWithStatus" should {
    implicit val timeout: Timeout = 3.seconds

    "unwrap success" in {
      val probe = TestProbe()
      val result = probe.ref.askWithStatus("request")
      probe.expectMsg("request")
      probe.lastSender ! StatusReply.Success("woho")
      Await.result(result, timeout.duration) should ===("woho")
    }

    "unwrap Error with message" in {
      val probe = TestProbe()
      val result = probe.ref.askWithStatus("request")
      probe.expectMsg("request")
      probe.lastSender ! StatusReply.Error("boho")
      result.failed.futureValue should ===(StatusReply.ErrorMessage("boho"))
    }

    "unwrap Error with exception" in {
      val probe = TestProbe()
      val result = probe.ref.askWithStatus("request")
      probe.expectMsg("request")
      probe.lastSender ! StatusReply.Error(TestException("boho"))
      result.failed.futureValue should ===(TestException("boho"))
    }

  }

}
