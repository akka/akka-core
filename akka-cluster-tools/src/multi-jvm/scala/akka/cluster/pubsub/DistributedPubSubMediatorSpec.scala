/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.pubsub

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSubMediator.Internal.Delta
import akka.cluster.pubsub.DistributedPubSubMediator.Internal.Status
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._

object DistributedPubSubMediatorSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.testkit.auto-down-unreachable-after = 0s
    akka.cluster.pub-sub.max-delta-elements = 500
    """))

  object TestChatUser {
    final case class Whisper(path: String, msg: Any)
    final case class Talk(path: String, msg: Any)
    final case class TalkToOthers(path: String, msg: Any)
    final case class Shout(topic: String, msg: Any)
    final case class ShoutToGroups(topic: String, msg: Any)
    final case class JoinGroup(topic: String, group: String)
    final case class ExitGroup(topic: String, group: String)
  }

  class TestChatUser(mediator: ActorRef, testActor: ActorRef) extends Actor {
    import DistributedPubSubMediator._
    import TestChatUser._

    def receive = {
      case Whisper(path, msg)        => mediator ! Send(path, msg, localAffinity = true)
      case Talk(path, msg)           => mediator ! SendToAll(path, msg)
      case TalkToOthers(path, msg)   => mediator ! SendToAll(path, msg, allButSelf = true)
      case Shout(topic, msg)         => mediator ! Publish(topic, msg)
      case ShoutToGroups(topic, msg) => mediator ! Publish(topic, msg, true)
      case JoinGroup(topic, group)   => mediator ! Subscribe(topic, Some(group), self)
      case ExitGroup(topic, group)   => mediator ! Unsubscribe(topic, Some(group), self)
      case msg                       => testActor ! msg
    }
  }

  //#publisher
  class Publisher extends Actor {
    import DistributedPubSubMediator.Publish
    // activate the extension
    val mediator = DistributedPubSub(context.system).mediator

    def receive = {
      case in: String =>
        val out = in.toUpperCase
        mediator ! Publish("content", out)
    }
  }
  //#publisher

  //#subscriber
  class Subscriber extends Actor with ActorLogging {
    import DistributedPubSubMediator.{ Subscribe, SubscribeAck }
    val mediator = DistributedPubSub(context.system).mediator
    // subscribe to the topic named "content"
    mediator ! Subscribe("content", self)

    def receive = {
      case s: String =>
        log.info("Got {}", s)
      case SubscribeAck(Subscribe("content", None, `self`)) =>
        log.info("subscribing")
    }
  }
  //#subscriber

  //#sender
  class Sender extends Actor {
    import DistributedPubSubMediator.Send
    // activate the extension
    val mediator = DistributedPubSub(context.system).mediator

    def receive = {
      case in: String =>
        val out = in.toUpperCase
        mediator ! Send(path = "/user/destination", msg = out, localAffinity = true)
    }
  }
  //#sender

  //#send-destination
  class Destination extends Actor with ActorLogging {
    import DistributedPubSubMediator.Put
    val mediator = DistributedPubSub(context.system).mediator
    // register to the path
    mediator ! Put(self)

    def receive = {
      case s: String =>
        log.info("Got {}", s)
    }
  }
  //#send-destination

}

class DistributedPubSubMediatorMultiJvmNode1 extends DistributedPubSubMediatorSpec
class DistributedPubSubMediatorMultiJvmNode2 extends DistributedPubSubMediatorSpec
class DistributedPubSubMediatorMultiJvmNode3 extends DistributedPubSubMediatorSpec

class DistributedPubSubMediatorSpec
    extends MultiNodeSpec(DistributedPubSubMediatorSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import DistributedPubSubMediator._
  import DistributedPubSubMediatorSpec._
  import DistributedPubSubMediatorSpec.TestChatUser._

  override def initialParticipants = roles.size

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system).join(node(to).address)
      createMediator()
    }
    enterBarrier(from.name + "-joined")
  }

  def createMediator(): ActorRef = DistributedPubSub(system).mediator
  def mediator: ActorRef = DistributedPubSub(system).mediator

  var chatUsers: Map[String, ActorRef] = Map.empty

  def createChatUser(name: String): ActorRef = {
    val a = system.actorOf(Props(classOf[TestChatUser], mediator, testActor), name)
    chatUsers += (name -> a)
    a
  }

  def chatUser(name: String): ActorRef = chatUsers(name)

  def awaitCount(expected: Int): Unit = {
    awaitAssert {
      mediator ! Count
      val actual = expectMsgType[Int]
      actual should ===(expected)
    }
  }

  def awaitCountSubscribers(expected: Int, topic: String): Unit = {
    awaitAssert {
      mediator ! CountSubscribers(topic)
      val actual = expectMsgType[Int]
      actual should ===(expected)
    }
  }

  "A DistributedPubSubMediator" must {

    "startup 2 node cluster" in within(15 seconds) {
      join(first, first)
      join(second, first)
      enterBarrier("after-1")
    }

    "keep track of added users" in within(15 seconds) {
      runOn(first) {
        val u1 = createChatUser("u1")
        mediator ! Put(u1)

        val u2 = createChatUser("u2")
        mediator ! Put(u2)

        awaitCount(2)

        // send to actor at same node
        u1 ! Whisper("/user/u2", "hello")
        expectMsg("hello")
        lastSender should ===(u2)
      }
      enterBarrier("2-registered")

      runOn(second) {
        val u3 = createChatUser("u3")
        mediator ! Put(u3)
      }

      runOn(first, second) {
        awaitCount(3)
      }
      enterBarrier("3-registered")

      runOn(second) {
        val u4 = createChatUser("u4")
        mediator ! Put(u4)
      }

      runOn(first, second) {
        awaitCount(4)
      }
      enterBarrier("4-registered")

      runOn(first) {
        // send to actor on another node
        chatUser("u1") ! Whisper("/user/u4", "hi there")
      }

      runOn(second) {
        expectMsg("hi there")
        lastSender.path.name should ===("u4")
      }

      enterBarrier("after-2")
    }

    "replicate users to new node" in within(20 seconds) {
      join(third, first)

      runOn(third) {
        val u5 = createChatUser("u5")
        mediator ! Put(u5)
      }

      awaitCount(5)
      enterBarrier("5-registered")

      runOn(third) {
        chatUser("u5") ! Whisper("/user/u4", "go")
      }

      runOn(second) {
        expectMsg("go")
        lastSender.path.name should ===("u4")
      }

      enterBarrier("after-3")
    }

    "keep track of removed users" in within(15 seconds) {
      runOn(first) {
        val u6 = createChatUser("u6")
        mediator ! Put(u6)
      }
      awaitCount(6)
      enterBarrier("6-registered")

      runOn(first) {
        mediator ! Remove("/user/u6")
      }
      awaitCount(5)

      enterBarrier("after-4")
    }

    "remove terminated users" in within(5 seconds) {
      runOn(second) {
        chatUser("u3") ! PoisonPill
      }

      awaitCount(4)
      enterBarrier("after-5")
    }

    "publish" in within(15 seconds) {
      runOn(first, second) {
        val u7 = createChatUser("u7")
        mediator ! Put(u7)
      }
      awaitCount(6)
      enterBarrier("7-registered")

      runOn(third) {
        chatUser("u5") ! Talk("/user/u7", "hi")
      }

      runOn(first, second) {
        expectMsg("hi")
        lastSender.path.name should ===("u7")
      }
      runOn(third) {
        expectNoMessage(2.seconds)
      }

      enterBarrier("after-6")
    }

    "publish to topic" in within(15 seconds) {
      runOn(first) {
        val s8 = Subscribe("topic1", createChatUser("u8"))
        mediator ! s8
        expectMsg(SubscribeAck(s8))
        val s9 = Subscribe("topic1", createChatUser("u9"))
        mediator ! s9
        expectMsg(SubscribeAck(s9))
      }
      runOn(second) {
        val s10 = Subscribe("topic1", createChatUser("u10"))
        mediator ! s10
        expectMsg(SubscribeAck(s10))
      }
      // one topic on two nodes
      awaitCount(8)
      enterBarrier("topic1-registered")

      runOn(third) {
        chatUser("u5") ! Shout("topic1", "hello all")
      }

      runOn(first) {
        val names = receiveWhile(messages = 2) {
          case "hello all" => lastSender.path.name
        }
        names.toSet should ===(Set("u8", "u9"))
      }
      runOn(second) {
        expectMsg("hello all")
        lastSender.path.name should ===("u10")
      }
      runOn(third) {
        expectNoMessage(2.seconds)
      }

      enterBarrier("after-7")
    }

    "demonstrate usage of Publish" in within(15 seconds) {
      def later(): Unit = {
        awaitCount(10)
      }

      //#start-subscribers
      runOn(first) {
        system.actorOf(Props[Subscriber](), "subscriber1")
      }
      runOn(second) {
        system.actorOf(Props[Subscriber](), "subscriber2")
        system.actorOf(Props[Subscriber](), "subscriber3")
      }
      //#start-subscribers

      //#publish-message
      runOn(third) {
        val publisher = system.actorOf(Props[Publisher](), "publisher")
        later()
        // after a while the subscriptions are replicated
        publisher ! "hello"
      }
      //#publish-message

      enterBarrier("after-8")
    }

    "demonstrate usage of Send" in within(15 seconds) {
      def later(): Unit = {
        awaitCount(12)
      }

      //#start-send-destinations
      runOn(first) {
        system.actorOf(Props[Destination](), "destination")
      }
      runOn(second) {
        system.actorOf(Props[Destination](), "destination")
      }
      //#start-send-destinations

      //#send-message
      runOn(third) {
        val sender = system.actorOf(Props[Sender](), "sender")
        later()
        // after a while the destinations are replicated
        sender ! "hello"
      }
      //#send-message

      enterBarrier("after-8")
    }

    "send-all to all other nodes" in within(15 seconds) {
      runOn(first, second, third) { // create the user on all nodes
        val u11 = createChatUser("u11")
        mediator ! Put(u11)
      }
      awaitCount(15)
      enterBarrier("11-registered")

      runOn(third) {
        chatUser("u5") ! TalkToOthers("/user/u11", "hi") // sendToAll to all other nodes
      }

      runOn(first, second) {
        expectMsg("hi")
        lastSender.path.name should ===("u11")
      }
      runOn(third) {
        expectNoMessage(2.seconds) // sender() node should not receive a message
      }

      enterBarrier("after-11")
    }

    "send one message to each group" in within(20 seconds) {
      runOn(first) {
        val u12 = createChatUser("u12")
        u12 ! JoinGroup("topic2", "group1")
        expectMsg(SubscribeAck(Subscribe("topic2", Some("group1"), u12)))
      }
      runOn(second) {
        val u12 = createChatUser("u12")
        u12 ! JoinGroup("topic2", "group2")
        expectMsg(SubscribeAck(Subscribe("topic2", Some("group2"), u12)))

        val u13 = createChatUser("u13")
        u13 ! JoinGroup("topic2", "group2")
        expectMsg(SubscribeAck(Subscribe("topic2", Some("group2"), u13)))
      }
      awaitCount(19)
      enterBarrier("12-registered")

      runOn(first) {
        chatUser("u12") ! ShoutToGroups("topic2", "hi")
      }

      runOn(first, second) {
        expectMsg("hi")
        expectNoMessage(2.seconds) // each group receive only one message
      }
      enterBarrier("12-published")

      runOn(first) {
        val u12 = chatUser("u12")
        u12 ! ExitGroup("topic2", "group1")
        expectMsg(UnsubscribeAck(Unsubscribe("topic2", Some("group1"), u12)))
      }

      runOn(second) {
        val u12 = chatUser("u12")
        u12 ! ExitGroup("topic2", "group2")
        expectMsg(UnsubscribeAck(Unsubscribe("topic2", Some("group2"), u12)))
        val u13 = chatUser("u13")
        u13 ! ExitGroup("topic2", "group2")
        expectMsg(UnsubscribeAck(Unsubscribe("topic2", Some("group2"), u13)))
      }
      enterBarrier("after-12")

    }

    "transfer delta correctly" in {
      val firstAddress = node(first).address
      val secondAddress = node(second).address
      val thirdAddress = node(third).address

      runOn(first) {
        mediator ! Status(versions = Map.empty, isReplyToStatus = false)
        val deltaBuckets = expectMsgType[Delta].buckets
        deltaBuckets.size should ===(3)
        deltaBuckets.find(_.owner == firstAddress).get.content.size should ===(10)
        deltaBuckets.find(_.owner == secondAddress).get.content.size should ===(9)
        deltaBuckets.find(_.owner == thirdAddress).get.content.size should ===(2)
      }
      enterBarrier("verified-initial-delta")

      // this test is configured with max-delta-elements = 500
      val many = 1010
      runOn(first) {
        for (i <- 0 until many)
          mediator ! Put(createChatUser("u" + (1000 + i)))

        mediator ! Status(versions = Map.empty, isReplyToStatus = false)
        val deltaBuckets1 = expectMsgType[Delta].buckets
        deltaBuckets1.map(_.content.size).sum should ===(500)

        mediator ! Status(versions = deltaBuckets1.map(b => b.owner -> b.version).toMap, isReplyToStatus = false)
        val deltaBuckets2 = expectMsgType[Delta].buckets
        deltaBuckets1.map(_.content.size).sum should ===(500)

        mediator ! Status(versions = deltaBuckets2.map(b => b.owner -> b.version).toMap, isReplyToStatus = false)
        val deltaBuckets3 = expectMsgType[Delta].buckets

        deltaBuckets3.map(_.content.size).sum should ===(10 + 9 + 2 + many - 500 - 500)
      }

      enterBarrier("verified-delta-with-many")
      within(10.seconds) {
        awaitCount(19 + many)
      }

      enterBarrier("after-13")
    }

    "remove entries when node is removed" in within(30 seconds) {
      mediator ! Count
      val countBefore = expectMsgType[Int]

      runOn(first) {
        testConductor.exit(third, 0).await
      }

      enterBarrier("third-shutdown")

      // third had 2 entries u5 and u11, and those should be removed everywhere
      runOn(first, second) {
        awaitCount(countBefore - 2)
      }

      enterBarrier("after-14")
    }

    "receive proper unsubscribeAck message" in within(15 seconds) {
      runOn(first) {
        val user = createChatUser("u111")
        val topic = "sample-topic1"
        val s1 = Subscribe(topic, user)
        mediator ! s1
        expectMsg(SubscribeAck(s1))
        val uns = Unsubscribe(topic, user)
        mediator ! uns
        expectMsg(UnsubscribeAck(uns))
      }
      enterBarrier("after-14")
    }

    "get topics after simple publish" in within(15 seconds) {
      runOn(first) {
        val s1 = Subscribe("topic_a1", createChatUser("u14"))
        mediator ! s1

        expectMsg(SubscribeAck(s1))

        val s2 = Subscribe("topic_a1", createChatUser("u15"))
        mediator ! s2
        expectMsg(SubscribeAck(s2))

        val s3 = Subscribe("topic_a2", createChatUser("u16"))
        mediator ! s3
        expectMsg(SubscribeAck(s3))
      }

      runOn(second) {
        val s3 = Subscribe("topic_a1", createChatUser("u17"))
        mediator ! s3
        expectMsg(SubscribeAck(s3))
      }

      enterBarrier("topics-registered")

      runOn(first) {
        mediator ! GetTopics
        expectMsgPF() {
          case CurrentTopics(topics)
              if topics.contains("topic_a1")
              && topics.contains("topic_a2") =>
            true
        }
      }

      runOn(second) {
        // topics will eventually be replicated
        awaitAssert {
          mediator ! GetTopics
          val topics = expectMsgType[CurrentTopics].topics
          topics should contain("topic_a1")
          topics should contain("topic_a2")
        }
      }

      enterBarrier("after-get-topics")

    }

    "remove topic subscribers when they terminate" in within(15 seconds) {
      runOn(first) {
        val s1 = Subscribe("topic_b1", createChatUser("u18"))
        mediator ! s1
        expectMsg(SubscribeAck(s1))

        awaitCountSubscribers(1, "topic_b1")

        chatUser("u18") ! PoisonPill

        awaitCountSubscribers(0, "topic_b1")
      }

      enterBarrier("after-15")
    }
  }

}
