/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.pattern.ask
import akka.testkit._

object ActorSelectionSpec {

  final case class Create(child: String)

  trait Query
  final case class SelectString(path: String) extends Query
  final case class SelectPath(path: ActorPath) extends Query
  final case class GetSender(to: ActorRef) extends Query
  final case class Forward(path: String, msg: Any) extends Query

  val p = Props[Node]()

  class Node extends Actor {
    def receive = {
      case Create(name)       => sender() ! context.actorOf(p, name)
      case SelectString(path) => sender() ! context.actorSelection(path)
      case SelectPath(path)   => sender() ! context.actorSelection(path)
      case GetSender(ref)     => ref ! sender()
      case Forward(path, msg) => context.actorSelection(path).forward(msg)
      case msg                => sender() ! msg
    }
  }

}

class ActorSelectionSpec extends AkkaSpec with DefaultTimeout {
  import ActorSelectionSpec._

  val c1 = system.actorOf(p, "c1")
  val c2 = system.actorOf(p, "c2")
  val c21 = Await.result((c2 ? Create("c21")).mapTo[ActorRef], timeout.duration)

  val sysImpl = system.asInstanceOf[ActorSystemImpl]

  val user = sysImpl.guardian
  val syst = sysImpl.systemGuardian
  val root = sysImpl.lookupRoot

  def empty(path: String) =
    new EmptyLocalActorRef(sysImpl.provider, path match {
      case RelativeActorPath(elems) => sysImpl.lookupRoot.path / elems
      case _                        => throw new RuntimeException()
    }, system.eventStream)

  val idProbe = TestProbe()

  def identify(selection: ActorSelection): Option[ActorRef] = {
    selection.tell(Identify(selection), idProbe.ref)
    val result = idProbe.expectMsgPF() {
      case ActorIdentity(`selection`, ref) => ref
    }
    val asked = Await.result((selection ? Identify(selection)).mapTo[ActorIdentity], timeout.duration)
    asked.ref should ===(result)
    asked.correlationId should ===(selection)

    implicit val ec = system.dispatcher
    val resolved =
      Await.result(selection.resolveOne(timeout.duration).mapTo[ActorRef].recover { case _ => null }, timeout.duration)
    Option(resolved) should ===(result)

    result
  }

  def identify(path: String): Option[ActorRef] = identify(system.actorSelection(path))
  def identify(path: ActorPath): Option[ActorRef] = identify(system.actorSelection(path))

  def askNode(node: ActorRef, query: Query): Option[ActorRef] = {
    Await.result(node ? query, timeout.duration) match {
      case ref: ActorRef             => Some(ref)
      case selection: ActorSelection => identify(selection)
      case _                         => throw new RuntimeException()
    }
  }

  "An ActorSystem" must {

    "select actors by their path" in {
      identify(c1.path) should ===(Some(c1))
      identify(c2.path) should ===(Some(c2))
      identify(c21.path) should ===(Some(c21))
      identify(system / "c1") should ===(Some(c1))
      identify(system / "c2") should ===(Some(c2))
      identify(system / "c2" / "c21") should ===(Some(c21))
      identify(system.child("c2").child("c21")) should ===(Some(c21)) // test Java API
      identify(system / Seq("c2", "c21")) should ===(Some(c21))

      import scala.jdk.CollectionConverters._
      identify(system.descendant(Seq("c2", "c21").asJava)) // test Java API
    }

    "select actors by their string path representation" in {
      identify(c1.path.toString) should ===(Some(c1))
      identify(c2.path.toString) should ===(Some(c2))
      identify(c21.path.toString) should ===(Some(c21))

      identify(c1.path.toStringWithoutAddress) should ===(Some(c1))
      identify(c2.path.toStringWithoutAddress) should ===(Some(c2))
      identify(c21.path.toStringWithoutAddress) should ===(Some(c21))
    }

    "take actor incarnation into account when comparing actor references" in {
      val name = "abcdefg"
      val a1 = system.actorOf(p, name)
      watch(a1)
      a1 ! PoisonPill
      expectMsgType[Terminated].actor should ===(a1)

      // not equal because it's terminated
      identify(a1.path) should ===(None)

      // wait till path is freed
      awaitCond {
        system.asInstanceOf[ExtendedActorSystem].provider.resolveActorRef(a1.path) != a1
      }

      val a2 = system.actorOf(p, name)
      a2.path should ===(a1.path)
      a2.path.toString should ===(a1.path.toString)
      a2 should not be (a1)
      a2.toString should not be (a1.toString)

      watch(a2)
      a2 ! PoisonPill
      expectMsgType[Terminated].actor should ===(a2)
    }

    "select actors by their root-anchored relative path" in {
      identify(c1.path.toStringWithoutAddress) should ===(Some(c1))
      identify(c2.path.toStringWithoutAddress) should ===(Some(c2))
      identify(c21.path.toStringWithoutAddress) should ===(Some(c21))
    }

    "select actors by their relative path" in {
      identify(c1.path.elements.mkString("/")) should ===(Some(c1))
      identify(c2.path.elements.mkString("/")) should ===(Some(c2))
      identify(c21.path.elements.mkString("/")) should ===(Some(c21))
    }

    "select system-generated actors" in {
      identify("/user") should ===(Some(user))
      identify("/system") should ===(Some(syst))
      identify(syst.path) should ===(Some(syst))
      identify(syst.path.toStringWithoutAddress) should ===(Some(syst))
      identify("/") should ===(Some(root))
      identify("") should ===(Some(root))
      identify(RootActorPath(root.path.address)) should ===(Some(root))
      identify("..") should ===(Some(root))
      identify(root.path) should ===(Some(root))
      identify(root.path.toStringWithoutAddress) should ===(Some(root))
      identify("user") should ===(Some(user))
      identify("system") should ===(Some(syst))
      identify("user/") should ===(Some(user))
      identify("system/") should ===(Some(syst))
    }

    "return ActorIdentity(None), respectively, for non-existing paths, and deadLetters" in {
      identify("a/b/c") should ===(None)
      identify("a/b/c") should ===(None)
      identify("akka://all-systems/Nobody") should ===(None)
      identify("akka://all-systems/user") should ===(None)
      identify(system / "hallo") should ===(None)
      identify("foo://user") should ===(None)
      identify("/deadLetters") should ===(None)
      identify("deadLetters") should ===(None)
      identify("deadLetters/") should ===(None)
    }

  }

  "An ActorContext" must {

    val all = Seq(c1, c2, c21)

    "select actors by their path" in {
      def check(looker: ActorRef, pathOf: ActorRef, result: ActorRef): Unit = {
        askNode(looker, SelectPath(pathOf.path)) should ===(Some(result))
      }
      for {
        looker <- all
        target <- all
      } check(looker, target, target)
    }

    "select actors by their string path representation" in {
      def check(looker: ActorRef, pathOf: ActorRef, result: ActorRef): Unit = {
        askNode(looker, SelectString(pathOf.path.toStringWithoutAddress)) should ===(Some(result))
        // with trailing /
        askNode(looker, SelectString(pathOf.path.toStringWithoutAddress + "/")) should ===(Some(result))
      }
      for {
        looker <- all
        target <- all
      } check(looker, target, target)
    }

    "select actors by their root-anchored relative path" in {
      def check(looker: ActorRef, pathOf: ActorRef, result: ActorRef): Unit = {
        askNode(looker, SelectString(pathOf.path.toStringWithoutAddress)) should ===(Some(result))
        askNode(looker, SelectString(pathOf.path.elements.mkString("/", "/", "/"))) should ===(Some(result))
      }
      for {
        looker <- all
        target <- all
      } check(looker, target, target)
    }

    "select actors by their relative path" in {
      def check(looker: ActorRef, result: ActorRef, elems: String*): Unit = {
        askNode(looker, SelectString(elems.mkString("/"))) should ===(Some(result))
        askNode(looker, SelectString(elems.mkString("", "/", "/"))) should ===(Some(result))
      }
      check(c1, user, "..")
      for {
        looker <- Seq(c1, c2)
        target <- all
      } check(looker, target, Seq("..") ++ target.path.elements.drop(1): _*)
      check(c21, user, "..", "..")
      check(c21, root, "..", "..", "..")
      check(c21, root, "..", "..", "..", "..")
    }

    "find system-generated actors" in {
      def check(target: ActorRef): Unit = {
        for (looker <- all) {
          askNode(looker, SelectPath(target.path)) should ===(Some(target))
          askNode(looker, SelectString(target.path.toString)) should ===(Some(target))
          askNode(looker, SelectString(target.path.toString + "/")) should ===(Some(target))
        }
        if (target != root)
          askNode(c1, SelectString("../.." + target.path.elements.mkString("/", "/", "/"))) should ===(Some(target))
      }
      for (target <- Seq(root, syst, user)) check(target)
    }

    "return deadLetters or ActorIdentity(None), respectively, for non-existing paths" in {
      import scala.jdk.CollectionConverters._

      def checkOne(looker: ActorRef, query: Query, result: Option[ActorRef]): Unit = {
        val lookup = askNode(looker, query)
        lookup should ===(result)
      }
      def check(looker: ActorRef): Unit = {
        for ((l, r) <- Seq(
               SelectString("a/b/c") -> None,
               SelectString("akka://all-systems/Nobody") -> None,
               SelectPath(system / "hallo") -> None,
               SelectPath(looker.path.child("hallo")) -> None, // test Java API
               SelectPath(looker.path.descendant(Seq("a", "b").asJava)) -> None) // test Java API
             ) checkOne(looker, l, r)
      }
      for (looker <- all) check(looker)
    }

  }

  "An ActorSelection" must {

    "send messages directly" in {
      ActorSelection(c1, "") ! GetSender(testActor)
      expectMsg(system.deadLetters)
      lastSender should ===(c1)
    }

    "send messages to string path" in {
      system.actorSelection("/user/c2/c21") ! GetSender(testActor)
      expectMsg(system.deadLetters)
      lastSender should ===(c21)
    }

    "send messages to actor path" in {
      system.actorSelection(system / "c2" / "c21") ! GetSender(testActor)
      expectMsg(system.deadLetters)
      lastSender should ===(c21)
    }

    "send messages with correct sender" in {
      implicit val sender = c1
      ActorSelection(c21, "../../*") ! GetSender(testActor)
      val actors = Set() ++ receiveWhile(messages = 2) {
          case `c1` => lastSender
        }
      actors should ===(Set(c1, c2))
      expectNoMessage()
    }

    "drop messages which cannot be delivered" in {
      implicit val sender = c2
      ActorSelection(c21, "../../*/c21") ! GetSender(testActor)
      val actors = receiveWhile(messages = 2) {
        case `c2` => lastSender
      }
      actors should ===(Seq(c21))
      expectNoMessage()
    }

    "resolve one actor with explicit timeout" in {
      val s = system.actorSelection(system / "c2")
      // Java and Scala API
      Await.result(s.resolveOne(1.second.dilated), timeout.duration) should ===(c2)
    }

    "resolve one actor with implicit timeout" in {
      val s = system.actorSelection(system / "c2")
      // Scala API; implicit timeout from DefaultTimeout trait
      Await.result(s.resolveOne(), timeout.duration) should ===(c2)
    }

    "resolve non-existing with Failure" in {
      intercept[ActorNotFound] {
        Await.result(system.actorSelection(system / "none").resolveOne(1.second.dilated), timeout.duration)
      }
    }

    "compare equally" in {
      ActorSelection(c21, "../*/hello") should ===(ActorSelection(c21, "../*/hello"))
      ActorSelection(c21, "../*/hello").## should ===(ActorSelection(c21, "../*/hello").##)
      ActorSelection(c2, "../*/hello") should not be ActorSelection(c21, "../*/hello")
      ActorSelection(c2, "../*/hello").## should not be ActorSelection(c21, "../*/hello").##
      ActorSelection(c21, "../*/hell") should not be ActorSelection(c21, "../*/hello")
      ActorSelection(c21, "../*/hell").## should not be ActorSelection(c21, "../*/hello").##
    }

    "print nicely" in {
      ActorSelection(c21, "../*/hello").toString should ===(
        s"ActorSelection[Anchor(akka://ActorSelectionSpec/user/c2/c21#${c21.path.uid}), Path(/../*/hello)]")
    }

    "have a stringly serializable path" in {
      system.actorSelection(system / "c2").toSerializationFormat should ===("akka://ActorSelectionSpec/user/c2")
      system.actorSelection(system / "c2" / "c21").toSerializationFormat should ===(
        "akka://ActorSelectionSpec/user/c2/c21")
      ActorSelection(c2, "/").toSerializationFormat should ===("akka://ActorSelectionSpec/user/c2")
      ActorSelection(c2, "../*/hello").toSerializationFormat should ===("akka://ActorSelectionSpec/user/c2/../*/hello")
      ActorSelection(c2, "/../*/hello").toSerializationFormat should ===("akka://ActorSelectionSpec/user/c2/../*/hello")
    }

    "send ActorSelection targeted to missing actor to deadLetters" in {
      val p = TestProbe()
      system.eventStream.subscribe(p.ref, classOf[DeadLetter])
      system.actorSelection("/user/missing").tell("boom", testActor)
      val d = p.expectMsgType[DeadLetter]
      d.message should ===("boom")
      d.sender should ===(testActor)
      d.recipient.path.toStringWithoutAddress should ===("/user/missing")
    }

    "identify actors with wildcard selection correctly" in {
      val creator = TestProbe()
      implicit def self: ActorRef = creator.ref
      val top = system.actorOf(p, "a")
      val b1 = Await.result((top ? Create("b1")).mapTo[ActorRef], timeout.duration)
      val b2 = Await.result((top ? Create("b2")).mapTo[ActorRef], timeout.duration)
      val c = Await.result((b2 ? Create("c")).mapTo[ActorRef], timeout.duration)
      val d = Await.result((c ? Create("d")).mapTo[ActorRef], timeout.duration)

      val probe = TestProbe()
      system.actorSelection("/user/a/*").tell(Identify(1), probe.ref)
      probe
        .receiveN(2)
        .map {
          case ActorIdentity(1, r) => r
          case _                   => throw new IllegalArgumentException()
        }
        .toSet should ===(Set[Option[ActorRef]](Some(b1), Some(b2)))
      probe.expectNoMessage()

      system.actorSelection("/user/a/b1/*").tell(Identify(2), probe.ref)
      probe.expectMsg(ActorIdentity(2, None))

      system.actorSelection("/user/a/*/c").tell(Identify(3), probe.ref)
      probe.expectMsg(ActorIdentity(3, Some(c)))
      probe.expectNoMessage()

      system.actorSelection("/user/a/b2/*/d").tell(Identify(4), probe.ref)
      probe.expectMsg(ActorIdentity(4, Some(d)))
      probe.expectNoMessage()

      system.actorSelection("/user/a/*/*/d").tell(Identify(5), probe.ref)
      probe.expectMsg(ActorIdentity(5, Some(d)))
      probe.expectNoMessage()

      system.actorSelection("/user/a/*/c/*").tell(Identify(6), probe.ref)
      probe.expectMsg(ActorIdentity(6, Some(d)))
      probe.expectNoMessage()

      system.actorSelection("/user/a/b2/*/d/e").tell(Identify(7), probe.ref)
      probe.expectMsg(ActorIdentity(7, None))
      probe.expectNoMessage()

      system.actorSelection("/user/a/*/c/d/e").tell(Identify(8), probe.ref)
      probe.expectNoMessage()
    }

    "forward to selection" in {
      c2.tell(Forward("c21", "hello"), testActor)
      expectMsg("hello")
      lastSender should ===(c21)
    }

  }

}
