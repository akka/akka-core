/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.{ ActorPath, ActorSystem }
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.internal.routing.GroupRouterImpl
import akka.actor.typed.internal.routing.RoutingLogics
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.adapter._

class RoutersSpec extends ScalaTestWithActorTestKit("""
    akka.loglevel=debug
  """) with AnyWordSpecLike with Matchers with LogCapturing {

  // needed for the event filter
  implicit val classicSystem: ActorSystem = system.toClassic

  def compileOnlyApiCoverage(): Unit = {
    Routers.group(ServiceKey[String]("key")).withRandomRouting().withRoundRobinRouting()

    Routers.pool(10)(Behaviors.empty[Any]).withRandomRouting()
    Routers.pool(10)(Behaviors.empty[Any]).withRoundRobinRouting()
    Routers.pool(10)(Behaviors.empty[Any]).withConsistentHashingRouting(1, (msg: Any) => msg.toString)
  }

  "The router pool" must {

    "create n children and route messages to" in {
      val childCounter = new AtomicInteger(0)
      case class Ack(msg: String, recipient: Int)
      val probe = createTestProbe[AnyRef]()
      val pool = spawn(Routers.pool[String](4)(Behaviors.setup { _ =>
        val id = childCounter.getAndIncrement()
        probe.ref ! s"started $id"
        Behaviors.receiveMessage { msg =>
          probe.ref ! Ack(msg, id)
          Behaviors.same
        }
      }))

      // ordering of these msgs is not guaranteed
      val expectedStarted = (0 to 3).map { n =>
        s"started $n"
      }.toSet
      probe.receiveMessages(4).toSet should ===(expectedStarted)

      // send one message at a time and see we rotate over all children, note that we don't necessarily
      // know what order the logic is rotating over the children, so we just check we reach all of them
      val sent = (0 to 8).map { n =>
        val msg = s"message-$n"
        pool ! msg
        msg
      }

      val acks = (0 to 8).map(_ => probe.expectMessageType[Ack])
      val (recipients, messages) = acks.foldLeft[(Set[Int], Set[String])]((Set.empty, Set.empty)) {
        case ((recipients, messages), ack) =>
          (recipients + ack.recipient, messages + ack.msg)
      }
      recipients should ===(Set(0, 1, 2, 3))
      messages should ===(sent.toSet)
    }

    "keep routing to the rest of the children if some children stops" in {
      val probe = createTestProbe[String]()
      val pool = spawn(Routers.pool[String](4)(Behaviors.receiveMessage {
        case "stop" =>
          Behaviors.stopped
        case msg =>
          probe.ref ! msg
          Behaviors.same
      }))

      LoggingTestKit.debug("Pool child stopped").withOccurrences(2).expect {
        pool ! "stop"
        pool ! "stop"
      }

      // there is a race here where the child stopped but the router did not see that message yet, and may
      // deliver messages to it, which will end up in dead letters.
      // this test protects against that by waiting for the log entry to show up

      val responses = (0 to 4).map { n =>
        val msg = s"message-$n"
        pool ! msg
        probe.expectMessageType[String]
      }

      (responses should contain).allOf("message-0", "message-1", "message-2", "message-3", "message-4")
    }

    "stops if all children stops" in {
      val probe = createTestProbe()
      val pool = spawn(Routers.pool[String](4)(Behaviors.receiveMessage { _ =>
        Behaviors.stopped
      }))

      LoggingTestKit.info("Last pool child stopped, stopping pool").expect {
        (0 to 3).foreach { _ =>
          pool ! "stop"
        }
        probe.expectTerminated(pool)
      }
    }

    "support broadcast" in {
      trait Cmd
      case object ReplyWithAck extends Cmd
      case object BCast extends Cmd

      def behavior(replyTo: ActorRef[AnyRef]) = Behaviors.setup[Cmd] { ctx =>
        Behaviors.receiveMessagePartial[Cmd] {
          case ReplyWithAck | BCast =>
            val reply = ctx.self.path
            replyTo ! reply
            Behaviors.same
        }
      }

      val probe = testKit.createTestProbe[AnyRef]()
      val pool = testKit.spawn(Routers.pool(4)(behavior(probe.ref)).withBroadcastPredicate(_ eq BCast))
      pool ! BCast
      val msgs = probe.receiveMessages(4).map { m =>
        m should be(an[ActorPath])
        m.asInstanceOf[ActorPath]
      }
      msgs should equal(msgs.distinct)
      probe.expectNoMessage()

      pool ! ReplyWithAck
      probe.expectMessageType[ActorPath]
      probe.expectNoMessage()
    }

  }

  "The router group" must {

    val receptionistDelayMs = 250

    "route messages across routees registered to the receptionist" in {
      val serviceKey = ServiceKey[String]("group-routing-1")
      val probe = createTestProbe[String]()
      val routeeBehavior: Behavior[String] = Behaviors.receiveMessage { msg =>
        probe.ref ! msg
        Behaviors.same
      }

      (0 to 3).foreach { n =>
        val ref = spawn(routeeBehavior, s"group-1-routee-$n")
        system.receptionist ! Receptionist.register(serviceKey, ref)
      }

      val group = spawn(Routers.group(serviceKey), "group-router-1")

      // ok to do right away
      (0 to 3).foreach { n =>
        val msg = s"message-$n"
        group ! msg
        probe.expectMessage(msg)
      }

      testKit.stop(group)
    }

    "publish Dropped messages when there are no routees available" in {
      val serviceKey = ServiceKey[String]("group-routing-2")
      val group = spawn(Routers.group(serviceKey), "group-router-2")
      val probe = createDroppedMessageProbe()

      (0 to 3).foreach { n =>
        val msg = s"message-$n"
        group ! msg
        probe.receiveMessage()
      }

      testKit.stop(group)
    }

    "handle a changing set of routees" in {
      val serviceKey = ServiceKey[String]("group-routing-3")
      val probe = createTestProbe[String]()
      val routeeBehavior: Behavior[String] = Behaviors.receiveMessage {
        case "stop" =>
          Behaviors.stopped
        case msg =>
          probe.ref ! msg
          Behaviors.same
      }

      val ref1 = spawn(routeeBehavior, s"group-3-routee-1")
      system.receptionist ! Receptionist.register(serviceKey, ref1)

      val ref2 = spawn(routeeBehavior, s"group-3-routee-2")
      system.receptionist ! Receptionist.register(serviceKey, ref2)

      val ref3 = spawn(routeeBehavior, s"group-3-routee-3")
      system.receptionist ! Receptionist.register(serviceKey, ref3)

      val group = spawn(Routers.group(serviceKey), "group-router-3")

      // give the group a little time to get a listing from the receptionist
      Thread.sleep(receptionistDelayMs)

      (0 to 3).foreach { n =>
        val msg = s"message-$n"
        group ! msg
        probe.expectMessage(msg)
      }

      ref2 ! "stop"

      // give the group a little time to get an updated listing from the receptionist
      Thread.sleep(receptionistDelayMs)

      (0 to 3).foreach { n =>
        val msg = s"message-$n"
        group ! msg
        probe.expectMessage(msg)
      }

      testKit.stop(group)

    }

    "not route to unreachable when there are reachable" in {
      val serviceKey = ServiceKey[String]("group-routing-4")
      val router = spawn(Behaviors.setup[String](context =>
        new GroupRouterImpl(context, serviceKey, false, new RoutingLogics.RoundRobinLogic[String], true)))

      val reachableProbe = createTestProbe[String]()
      val unreachableProbe = createTestProbe[String]()
      router
        .unsafeUpcast[Any] ! Receptionist.Listing(serviceKey, Set(reachableProbe.ref), Set(unreachableProbe.ref), false)
      router ! "one"
      router ! "two"
      reachableProbe.expectMessage("one")
      reachableProbe.expectMessage("two")
    }

    "route to unreachable when there are no reachable" in {
      val serviceKey = ServiceKey[String]("group-routing-4")
      val router = spawn(Behaviors.setup[String](context =>
        new GroupRouterImpl(context, serviceKey, false, new RoutingLogics.RoundRobinLogic[String], true)))

      val unreachableProbe = createTestProbe[String]()
      router.unsafeUpcast[Any] ! Receptionist.Listing(
        serviceKey,
        Set.empty[ActorRef[String]],
        Set(unreachableProbe.ref),
        true)
      router ! "one"
      router ! "two"
      unreachableProbe.expectMessage("one")
      unreachableProbe.expectMessage("two")
    }
  }

}
