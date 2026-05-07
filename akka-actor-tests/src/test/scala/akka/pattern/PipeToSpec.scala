/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import scala.concurrent.Future

import akka.actor.Status
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe

class PipeToSpec extends AkkaSpec {

  import system.dispatcher

  "PipeTo" must {

    "work" in {
      val p = TestProbe()
      Future(42).pipeTo(p.ref)
      p.expectMsg(42)
    }

    "signal failure" in {
      val p = TestProbe()
      Future.failed(new Exception("failed")).pipeTo(p.ref)
      p.expectMsgType[Status.Failure].cause.getMessage should ===("failed")
    }

    "pick up an implicit sender()" in {
      val p = TestProbe()
      implicit val s = testActor
      Future(42).pipeTo(p.ref)
      p.expectMsg(42)
      p.lastSender should ===(s)
    }

    "work in Java form" in {
      val p = TestProbe()
      pipe(Future(42)) to p.ref
      p.expectMsg(42)
    }

    "work in Java form with sender()" in {
      val p = TestProbe()
      pipe(Future(42)).to(p.ref, testActor)
      p.expectMsg(42)
      p.lastSender should ===(testActor)
    }

  }

  "PipeToSelection" must {

    "work" in {
      val p = TestProbe()
      val sel = system.actorSelection(p.ref.path)
      Future(42).pipeToSelection(sel)
      p.expectMsg(42)
    }

    "signal failure" in {
      val p = TestProbe()
      val sel = system.actorSelection(p.ref.path)
      Future.failed(new Exception("failed")).pipeToSelection(sel)
      p.expectMsgType[Status.Failure].cause.getMessage should ===("failed")
    }

    "pick up an implicit sender()" in {
      val p = TestProbe()
      val sel = system.actorSelection(p.ref.path)
      implicit val s = testActor
      Future(42).pipeToSelection(sel)
      p.expectMsg(42)
      p.lastSender should ===(s)
    }

    "work in Java form" in {
      val p = TestProbe()
      val sel = system.actorSelection(p.ref.path)
      pipe(Future(42)) to sel
      p.expectMsg(42)
    }

    "work in Java form with sender()" in {
      val p = TestProbe()
      val sel = system.actorSelection(p.ref.path)
      pipe(Future(42)).to(sel, testActor)
      p.expectMsg(42)
      p.lastSender should ===(testActor)
    }

  }

}
