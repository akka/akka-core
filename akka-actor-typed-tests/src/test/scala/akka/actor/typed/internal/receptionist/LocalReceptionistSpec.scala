/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.receptionist

import scala.concurrent.Future

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{ AnyWordSpec, AnyWordSpecLike }

import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed._
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.Receptionist._
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors

object LocalReceptionistSpec {
  trait ServiceA
  val ServiceKeyA = ServiceKey[ServiceA]("service-a")
  val behaviorA = Behaviors.empty[ServiceA]

  trait ServiceB
  val ServiceKeyB = ServiceKey[ServiceB]("service-b")
  val behaviorB = Behaviors.empty[ServiceB]

  case object Stop extends ServiceA with ServiceB
  val stoppableBehavior = Behaviors.receive[Any] { (_, message) =>
    message match {
      case Stop => Behaviors.stopped
      case _    => Behaviors.same
    }
  }

}

class LocalReceptionistSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  import LocalReceptionistSpec._

  "A local receptionist" must {

    "unregister services when they terminate" in {
      val receptionist = spawn(LocalReceptionist.behavior)
      val regProbe = TestProbe[Any]("regProbe")

      val serviceA = spawn(stoppableBehavior.narrow[ServiceA])
      receptionist ! Register(ServiceKeyA, serviceA, regProbe.ref)
      regProbe.expectMessage(Registered(ServiceKeyA, serviceA))

      val serviceB = spawn(stoppableBehavior.narrow[ServiceB])
      receptionist ! Register(ServiceKeyB, serviceB, regProbe.ref)
      regProbe.expectMessage(Registered(ServiceKeyB, serviceB))

      val serviceC = spawn(stoppableBehavior)
      receptionist ! Register(ServiceKeyA, serviceC, regProbe.ref)
      receptionist ! Register(ServiceKeyB, serviceC, regProbe.ref)
      regProbe.expectMessage(Registered(ServiceKeyA, serviceC))
      regProbe.expectMessage(Registered(ServiceKeyB, serviceC))

      receptionist ! Find(ServiceKeyA, regProbe.ref)
      regProbe.expectMessage(Listing(ServiceKeyA, Set(serviceA, serviceC)))
      receptionist ! Find(ServiceKeyB, regProbe.ref)
      regProbe.expectMessage(Listing(ServiceKeyB, Set(serviceB, serviceC)))

      serviceC ! Stop

      eventually {
        receptionist ! Find(ServiceKeyA, regProbe.ref)
        regProbe.expectMessage(Listing(ServiceKeyA, Set(serviceA)))
        receptionist ! Find(ServiceKeyB, regProbe.ref)
        regProbe.expectMessage(Listing(ServiceKeyB, Set(serviceB)))
      }
    }

    "unregister programatically" in {
      val subProbe = TestProbe[Any]()
      val receptionist = spawn(LocalReceptionist.behavior)
      receptionist ! Subscribe(ServiceKeyA, subProbe.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set.empty[ActorRef[ServiceA]]))
      val serviceA = TestProbe[ServiceA]()
      receptionist ! Register(ServiceKeyA, serviceA.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set(serviceA.ref)))
      receptionist ! Deregister(ServiceKeyA, serviceA.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set.empty[ActorRef[ServiceA]]))
    }

    "unregister per service key, not service actor" in {
      val subProbe = TestProbe[Any]()
      val receptionist = spawn(LocalReceptionist.behavior)

      // subscribe to 2 keys
      receptionist ! Subscribe(ServiceKeyA, subProbe.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set.empty[ActorRef[ServiceA]]))
      receptionist ! Subscribe(ServiceKeyB, subProbe.ref)
      subProbe.expectMessage(Listing(ServiceKeyB, Set.empty[ActorRef[ServiceB]]))

      // register same service for both 2 keys
      val service = TestProbe[AnyRef]()
      receptionist ! Register(ServiceKeyA, service.ref)
      receptionist ! Register(ServiceKeyB, service.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set(service.ref.narrow[ServiceKeyA.Protocol])))
      subProbe.expectMessage(Listing(ServiceKeyB, Set(service.ref.narrow[ServiceKeyB.Protocol])))

      // unregister one of the service keys for the service
      receptionist ! Deregister(ServiceKeyA, service.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set.empty[ActorRef[ServiceA]]))
      receptionist ! Find(ServiceKeyB, subProbe.ref)
      subProbe.expectMessage(Listing(ServiceKeyB, Set(service.ref.narrow[ServiceKeyB.Protocol])))
    }

    "unregister and re-register same service actor" in {
      val subProbe = TestProbe[Any]()
      val receptionist = spawn(LocalReceptionist.behavior)

      receptionist ! Subscribe(ServiceKeyA, subProbe.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set.empty[ActorRef[ServiceA]]))

      val serviceA = TestProbe[ServiceA]()
      receptionist ! Register(ServiceKeyA, serviceA.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set(serviceA.ref)))

      receptionist ! Deregister(ServiceKeyA, serviceA.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set.empty[ActorRef[ServiceA]]))

      receptionist ! Register(ServiceKeyA, serviceA.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set(serviceA.ref)))
    }

    "support subscribing to service changes" in {
      val receptionist = spawn(LocalReceptionist.behavior)
      val regProbe = TestProbe[Registered]("regProbe")

      val aSubscriber = TestProbe[Listing]("aUser")
      receptionist ! Subscribe(ServiceKeyA, aSubscriber.ref)

      aSubscriber.expectMessage(Listing(ServiceKeyA, Set.empty[ActorRef[ServiceA]]))

      val serviceA: ActorRef[ServiceA] = spawn(stoppableBehavior)
      receptionist ! Register(ServiceKeyA, serviceA, regProbe.ref)
      regProbe.expectMessage(Registered(ServiceKeyA, serviceA))

      aSubscriber.expectMessage(Listing(ServiceKeyA, Set(serviceA)))

      val serviceA2: ActorRef[ServiceA] = spawn(stoppableBehavior)
      receptionist ! Register(ServiceKeyA, serviceA2, regProbe.ref)
      regProbe.expectMessage(Registered(ServiceKeyA, serviceA2))

      aSubscriber.expectMessage(Listing(ServiceKeyA, Set(serviceA, serviceA2)))

      serviceA ! Stop
      aSubscriber.expectMessage(Listing(ServiceKeyA, Set(serviceA2)))
      serviceA2 ! Stop
      aSubscriber.expectMessage(Listing(ServiceKeyA, Set.empty[ActorRef[ServiceA]]))
    }

    "support subscribing to different services with the same subscriber" in {
      val subProbe = TestProbe[Any]()
      val receptionist = spawn(LocalReceptionist.behavior)
      receptionist ! Subscribe(ServiceKeyA, subProbe.ref)
      receptionist ! Subscribe(ServiceKeyB, subProbe.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set.empty[ActorRef[ServiceA]]))
      subProbe.expectMessage(Listing(ServiceKeyB, Set.empty[ActorRef[ServiceB]]))
      val serviceA = TestProbe[ServiceA]()
      receptionist ! Register(ServiceKeyA, serviceA.ref)
      subProbe.expectMessage(Listing(ServiceKeyA, Set(serviceA.ref)))
      val serviceB = TestProbe[ServiceB]()
      receptionist ! Register(ServiceKeyB, serviceB.ref)
      subProbe.expectMessage(Listing(ServiceKeyB, Set(serviceB.ref)))
    }

    "work with ask" in {
      val receptionist = spawn(LocalReceptionist.behavior)
      val serviceA = spawn(behaviorA)
      val f: Future[Registered] = receptionist.ask(Register(ServiceKeyA, serviceA, _))
      f.futureValue should be(Registered(ServiceKeyA, serviceA))
    }

    "be present in the system" in {
      val probe = TestProbe[Receptionist.Listing]()
      system.receptionist ! Find(ServiceKeyA, probe.ref)
      val listing: Listing = probe.receiveMessage()
      listing.isForKey(ServiceKeyA) should ===(true)
      listing.serviceInstances(ServiceKeyA) should be(Set())
    }
  }
}

class LocalReceptionistBehaviorSpec extends AnyWordSpec with Matchers with LogCapturing {
  import LocalReceptionistSpec._

  def assertEmpty(inboxes: TestInbox[_]*): Unit = {
    inboxes.foreach(i => withClue(s"inbox $i had messages")(i.hasMessages should be(false)))
  }

  "A local receptionist behavior" must {

    "register a service" in {
      val testkit = BehaviorTestKit(LocalReceptionist.behavior)
      val a = TestInbox[ServiceA]("a")
      val r = TestInbox[Registered]("r")
      testkit.run(Register(ServiceKeyA, a.ref, r.ref))
      testkit.retrieveEffect() // watching however that is implemented
      r.receiveMessage() should be(Registered(ServiceKeyA, a.ref))
      val q = TestInbox[Listing]("q")
      testkit.run(Find(ServiceKeyA, q.ref))
      testkit.retrieveAllEffects() should be(Nil)
      q.receiveMessage() should be(Listing(ServiceKeyA, Set(a.ref)))
      assertEmpty(a, r, q)
    }

    "register two services" in {
      val testkit = BehaviorTestKit(LocalReceptionist.behavior)
      val a = TestInbox[ServiceA]("a")
      val r = TestInbox[Registered]("r")
      testkit.run(Register(ServiceKeyA, a.ref, r.ref))
      r.receiveMessage() should be(Registered(ServiceKeyA, a.ref))
      val b = TestInbox[ServiceB]("b")
      testkit.run(Register(ServiceKeyB, b.ref, r.ref))
      r.receiveMessage() should be(Registered(ServiceKeyB, b.ref))
      val q = TestInbox[Listing]("q")
      testkit.run(Find(ServiceKeyA, q.ref))
      q.receiveMessage() should be(Listing(ServiceKeyA, Set(a.ref)))
      testkit.run(Find(ServiceKeyB, q.ref))
      q.receiveMessage() should be(Listing(ServiceKeyB, Set(b.ref)))
      assertEmpty(a, b, r, q)
    }

    "register two services with the same key" in {
      val testkit = BehaviorTestKit(LocalReceptionist.behavior)
      val a1 = TestInbox[ServiceA]("a1")
      val r = TestInbox[Registered]("r")
      testkit.run(Register(ServiceKeyA, a1.ref, r.ref))
      r.receiveMessage() should be(Registered(ServiceKeyA, a1.ref))
      val a2 = TestInbox[ServiceA]("a2")
      testkit.run(Register(ServiceKeyA, a2.ref, r.ref))
      r.receiveMessage() should be(Registered(ServiceKeyA, a2.ref))
      val q = TestInbox[Listing]("q")
      testkit.run(Find(ServiceKeyA, q.ref))
      q.receiveMessage() should be(Listing(ServiceKeyA, Set(a1.ref, a2.ref)))
      testkit.run(Find(ServiceKeyB, q.ref))
      q.receiveMessage() should be(Listing(ServiceKeyB, Set.empty[ActorRef[ServiceB]]))
      assertEmpty(a1, a2, r, q)
    }

  }
}
