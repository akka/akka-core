/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.javadsl

import org.scalatest.matchers.should.Matchers._

import akka.actor.Props
import akka.actor.typed.javadsl.Adapter
import akka.persistence._
import akka.persistence.testkit._
import akka.testkit.EventFilter
import scala.jdk.CollectionConverters._

trait CommonTestKitTests extends JavaDslUtils {

  final lazy val testKit = new PersistenceTestKit(system)
  import testKit._

  def specificTests(): Unit

  "PersistenceTestKit" should {

    "work with typed actors" in {
      val expectedId = randomPid()
      val pid = randomPid()
      val act = Adapter.spawn(system, eventSourcedBehavior(pid), pid)
      act ! Cmd(expectedId)

      testKit.expectNextPersisted(pid, Evt(expectedId))
      testKit.expectNothingPersisted(pid)
    }

    "work with tagged events" in {
      val expectedId = randomPid()
      val pid = randomPid()
      var act =
        Adapter.spawn(system, eventSourcedBehavior(pid, true, Some(Adapter.toTyped[Any](testActor))), randomPid())
      expectMsg(Recovered)
      act ! Cmd(expectedId)
      testKit.expectNextPersisted(pid, Evt(expectedId))
      act ! Passivate
      expectMsg(Stopped)

      act = Adapter.spawn(system, eventSourcedBehavior(pid, true, Some(Adapter.toTyped[Any](testActor))), randomPid())
      val expectedId2 = randomPid()
      act ! Cmd(expectedId2)
      expectMsg(Recovered)
      testKit.expectNextPersisted(pid, Evt(expectedId2))
      testKit.expectNothingPersisted(pid)
    }

    "expect next valid message" in {

      val pid = randomPid()

      val a = system.actorOf(Props(classOf[A], pid, None))

      a ! B(1)
      a ! B(2)

      expectNextPersisted(pid, B(1))

      assertThrows[AssertionError] {
        expectNextPersisted(pid, B(3))
      }

      expectNextPersisted(pid, B(2))

      assertThrows[AssertionError] {
        expectNextPersisted(pid, B(3))
      }

    }

    "expect next N valid messages in order" in {

      val pid = randomPid()

      val a = system.actorOf(Props(classOf[A], pid, None))

      a ! B(1)
      a ! B(2)

      assertThrows[AssertionError] {
        receivePersisted(pid, 3, classOf[B])
      }
      assertThrows[AssertionError] {
        receivePersisted(pid, 2, classOf[C])
      }

      val li = receivePersisted(pid, 2, classOf[B])
      (li should contain).theSameElementsInOrderAs(List(B(1), B(2)))
    }

    "successfully set and execute custom policy" in {

      val pid = randomPid()

      val err = new Exception("BOOM!")

      val newPolicy = new EventStorage.JournalPolicies.PolicyType {
        override def tryProcess(persistenceId: String, processingUnit: JournalOperation): ProcessingResult = {
          processingUnit match {
            case WriteEvents(msgs) =>
              val ex = msgs.exists({
                case B(666) => true
                case _      => false
              })
              if (ex) {
                ProcessingSuccess
              } else {
                StorageFailure(err)
              }
            case _ => ProcessingSuccess
          }
        }
      }

      withPolicy(newPolicy)

      val a = system.actorOf(Props(classOf[A], pid, None))

      EventFilter.error(err.getMessage, occurrences = 1).intercept {
        a ! B(1)
      }

      watch(a)
      expectTerminated(a)

      val aa = system.actorOf(Props(classOf[A], pid, None))

      aa ! B(666)

      expectNextPersisted(pid, B(666))

      resetPolicy()

    }

    "reject next persisted" in {

      val pid = randomPid()

      val a = system.actorOf(Props(classOf[A], pid, None))

      //consecutive calls should stack
      rejectNextPersisted()
      rejectNextPersisted()

      a ! B(1)

      assertThrows[AssertionError] {
        expectNextPersisted(pid, B(1))
      }

      a ! B(2)

      assertThrows[AssertionError] {
        expectNextPersisted(pid, B(2))
      }

      a ! B(3)

      expectNextPersisted(pid, B(3))

    }

    "reject next persisted with custom exception" in {

      val pid = randomPid()

      val a = system.actorOf(Props(classOf[A], pid, None))

      val err = new Exception("Custom ERROR!")

      rejectNextPersisted(err)

      EventFilter.error(err.getMessage, occurrences = 1).intercept {
        a ! B(1)
      }

      a ! B(2)

      expectNextPersisted(pid, B(2))

    }

    "fail next persisted" in {

      val pid = randomPid()

      val a = system.actorOf(Props(classOf[A], pid, None))

      //consecutive calls should stack
      failNextPersisted()
      failNextPersisted()

      a ! B(1)

      watch(a)
      expectTerminated(a)

      val b = system.actorOf(Props(classOf[A], pid, None))

      b ! B(2)

      watch(b)
      expectTerminated(b)

      val c = system.actorOf(Props(classOf[A], pid, None))

      c ! B(3)

      expectNextPersisted(pid, B(3))

    }

    "fail next persisted with custom exception" in {

      val pid = randomPid()

      val a = system.actorOf(Props(classOf[A], pid, None))

      val err = new Exception("Custom ERROR!")

      failNextPersisted(err)

      EventFilter.error(err.getMessage, occurrences = 1).intercept {
        a ! B(1)
      }

      watch(a)
      expectTerminated(a)

    }

    "expect nothingPersisted fails" in {

      val pid = randomPid()

      val a = system.actorOf(Props(classOf[A], pid, None))

      expectNothingPersisted(pid)

      a ! B(1)

      assertThrows[AssertionError] {
        expectNothingPersisted(pid)
      }

    }

    "expect no message persisted" in {

      val pid = randomPid()

      val a = system.actorOf(Props(classOf[A], pid, None))

      expectNothingPersisted(pid)

      a ! B(1)

      expectNextPersisted(pid, B(1))

      expectNothingPersisted(pid)

    }

    "fail recovery" in {

      val pid = randomPid()

      failNextNOps(1)

      val a = system.actorOf(Props(classOf[A], pid, None))

      watch(a)

      expectTerminated(a)

      system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((List.empty, 0))

    }

    "recover persisted messages" in {

      val preload = List(B(1), B(2), B(3)).map(e => e: Any).asJava
      val pid = randomPid()

      persistForRecovery(pid, preload)

      system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((preload.asScala, 0))

    }

    "fail to recover persisted messages for any actor" in {

      val preload = List(B(1), B(2), B(3)).map(e => e: Any).asJava
      val pid = randomPid()

      persistForRecovery(pid, preload)

      failNextRead()

      val a = system.actorOf(Props(classOf[A], pid, Some(testActor)))

      watch(a)
      expectTerminated(a)

    }

    "fail to recover persisted messages for any actor with custom error" in {

      val preload = List(B(1), B(2), B(3)).map(e => e: Any).asJava
      val pid = randomPid()

      val err = new Exception("BOOM!")

      persistForRecovery(pid, preload)

      failNextRead(err)

      EventFilter.error(err.getMessage, occurrences = 1).intercept {
        val a = system.actorOf(Props(classOf[A], pid, Some(testActor)))
        watch(a)
        expectTerminated(a)
      }

    }

    "fail to recover persisted messages for actor with particular persistenceId" in {

      val preload = List(B(1), B(2), B(3)).map(e => e: Any).asJava
      val pid = randomPid()

      persistForRecovery(pid, preload)

      failNextRead(pid)

      val a = system.actorOf(Props(classOf[A], pid, Some(testActor)))

      watch(a)
      expectTerminated(a)

    }

    "recover persisted messages when fail for different persistenceId is set" in {

      val preload = List(B(1), B(2), B(3)).map(e => e: Any).asJava
      val pid = randomPid()

      persistForRecovery(pid, preload)

      val otherPid = randomPid()

      failNextRead(otherPid)

      system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((preload.asScala, 0))

    }

    "persist and return persisted messages" in {

      val pid = randomPid()

      val saved = List(B(1), B(2), B(3)).map(e => e: Any).asJava

      persistForRecovery(pid, saved)

      val li = persistedInStorage(pid).asScala

      (li should contain).theSameElementsInOrderAs(saved.asScala)

    }

    "fail next delete for any actor" in {

      val pid = randomPid()

      val preload = List(B(1)).map(e => e: Any).asJava
      persistForRecovery(pid, preload)

      val a = system.actorOf(Props(classOf[A], pid, Some(testActor)))

      failNextDelete()

      a ! DeleteAllMessages

      expectMsg((preload.asScala, 0))
      expectMsgPF() { case DeleteMessagesFailure(ExpectedFailure, _) => }

      a ! DeleteAllMessages

      expectMsgPF() { case DeleteMessagesSuccess(_) => }

    }

    "fail next delete for any actor with custom exception" in {

      val pid = randomPid()

      val err = new Exception("BOOM!")

      val preload = List(B(1)).map(e => e: Any).asJava
      persistForRecovery(pid, preload)

      val a = system.actorOf(Props(classOf[A], pid, Some(testActor)))

      failNextDelete(err)

      a ! DeleteAllMessages

      expectMsg((preload.asScala, 0))
      expectMsgPF() { case DeleteMessagesFailure(e, _) if e.getMessage == err.getMessage => }

      a ! DeleteAllMessages

      expectMsgPF() { case DeleteMessagesSuccess(_) => }

    }

    "fail next delete for particular persistence id" in {

      val pid = randomPid()

      val preload = List(B(1)).map(e => e: Any).asJava
      persistForRecovery(pid, preload)

      val a = system.actorOf(Props(classOf[A], pid, Some(testActor)))

      failNextDelete(pid)

      a ! DeleteAllMessages

      expectMsg((preload.asScala, 0))
      expectMsgPF() { case DeleteMessagesFailure(ExpectedFailure, _) => }

      a ! DeleteAllMessages

      expectMsgPF() { case DeleteMessagesSuccess(_) => }

    }

    "not fail next delete for other persistence id" in {

      val pid = randomPid()

      persistForRecovery(pid, List(1).map(e => e: Any).asJava)

      val a = system.actorOf(Props(classOf[A], pid, Some(testActor)))

      val other = randomPid()

      failNextDelete(other)

      a ! DeleteAllMessages

      expectMsg((List(1), 0))
      expectMsgPF() { case DeleteMessagesSuccess(_) => }

    }

    "clear all" in {

      val pid = randomPid()

      persistForRecovery(pid, List(B(1), B(2), B(3)).map(e => e: Any).asJava)

      system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((List(B(1), B(2), B(3)), 0))

      clearAll()

      val aa = system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((List.empty, 0))

      aa ! AskMessageSeqNum

      expectMsg(0L)

    }

    "clear all for particular persistence id" in {

      val pid = randomPid()

      persistForRecovery(pid, List(B(1), B(2), B(3)).map(e => e: Any).asJava)

      system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((List(B(1), B(2), B(3)), 0))

      clearByPersistenceId(pid)

      val aa = system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((List.empty, 0))

      aa ! AskMessageSeqNum

      expectMsg(0L)

    }

    "preserve all for other persistence id" in {

      val pid = randomPid()

      persistForRecovery(pid, List(B(1), B(2), B(3)).map(e => e: Any).asJava)

      system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((List(B(1), B(2), B(3)), 0))

      clearByPersistenceId(randomPid())

      system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((List(B(1), B(2), B(3)), 0))

    }

    "clear all preserving seq nums" in {

      val pid = randomPid()

      persistForRecovery(pid, List(B(1), B(2), B(3)).map(e => e: Any).asJava)

      system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((List(B(1), B(2), B(3)), 0))

      clearAllPreservingSeqNumbers()

      val aa = system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((List.empty, 0))

      aa ! AskMessageSeqNum

      expectMsg(3L)

    }

    "clear all preserving seq num for particular persistence id" in {

      val pid = randomPid()

      persistForRecovery(pid, List(B(1), B(2), B(3)).map(e => e: Any).asJava)

      system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((List(B(1), B(2), B(3)), 0))

      clearByIdPreservingSeqNumbers(pid)

      val aa = system.actorOf(Props(classOf[A], pid, Some(testActor)))

      expectMsg((List.empty, 0))

      aa ! AskMessageSeqNum

      expectMsg(3L)

    }

    specificTests()

  }

}
