/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.testkit

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import akka.testkit.TestKitBase
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.testkit.TestProbe
import akka.actor.ActorRefFactory
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll

import scala.annotation.nowarn

/**
 * Parent-Child examples
 */
//#test-example
class Parent extends Actor {
  val child = context.actorOf(Props[Child](), "child")
  var ponged = false

  def receive = {
    case "pingit" => child ! "ping"
    case "pong"   => ponged = true
  }
}

class Child extends Actor {
  def receive = {
    case "ping" => context.parent ! "pong"
  }
}
//#test-example

//#test-dependentchild
class DependentChild(parent: ActorRef) extends Actor {
  def receive = {
    case "ping" => parent ! "pong"
  }
}
//#test-dependentchild

//#test-dependentparent
class DependentParent(childProps: Props, probe: ActorRef) extends Actor {
  val child = context.actorOf(childProps, "child")

  def receive = {
    case "pingit" => child ! "ping"
    case "pong"   => probe ! "ponged"
  }
}

class GenericDependentParent(childMaker: ActorRefFactory => ActorRef) extends Actor {
  val child = childMaker(context)
  var ponged = false

  def receive = {
    case "pingit" => child ! "ping"
    case "pong"   => ponged = true
  }
}
//#test-dependentparent

/**
 * Test specification
 */
class MockedChild extends Actor {
  def receive = {
    case "ping" => sender() ! "pong"
  }
}

@nowarn("msg=never used") // sample snippets
class ParentChildSpec extends AnyWordSpec with Matchers with TestKitBase with BeforeAndAfterAll {
  implicit lazy val system: ActorSystem = ActorSystem("ParentChildSpec")

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A DependentChild" should {
    "be tested without its parent" in {
      val probe = TestProbe()
      val child = system.actorOf(Props(new DependentChild(probe.ref)))
      probe.send(child, "ping")
      probe.expectMsg("pong")
    }
  }

  "A DependentParent" should {
    "be tested with custom props" in {
      val probe = TestProbe()
      val childProps = Props(new MockedChild())
      val parent = system.actorOf(Props(new DependentParent(childProps, probe.ref)))
      probe.send(parent, "pingit")
      // test some parent state change
      probe.expectMsg("ponged")
    }
  }

  "A GenericDependentParent" should {
    "be tested with a child probe" in {
      val probe = TestProbe()
      //#child-maker-test
      val maker = (_: ActorRefFactory) => probe.ref
      val parent = system.actorOf(Props(new GenericDependentParent(maker)))
      //#child-maker-test
      probe.send(parent, "pingit")
      probe.expectMsg("ping")
    }

    "demonstrate production version of child creator" in {
      //#child-maker-prod
      val maker = (f: ActorRefFactory) => f.actorOf(Props(new Child))
      val parent = system.actorOf(Props(new GenericDependentParent(maker)))
      //#child-maker-prod
    }
  }

  //#test-TestProbe-parent
  "A TestProbe serving as parent" should {
    "test its child responses" in {
      val parent = TestProbe()
      val child = parent.childActorOf(Props(new Child))
      parent.send(child, "ping")
      parent.expectMsg("pong")
    }
  }
  //#test-TestProbe-parent

  //#test-fabricated-parent
  "A fabricated parent" should {
    "test its child responses" in {
      val proxy = TestProbe()
      val parent = system.actorOf(Props(new Actor {
        val child = context.actorOf(Props(new Child), "child")
        def receive = {
          case x if sender() == child => proxy.ref.forward(x)
          case x                      => child.forward(x)
        }
      }))

      proxy.send(parent, "ping")
      proxy.expectMsg("pong")
    }
  }
  //#test-fabricated-parent
}
