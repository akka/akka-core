/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestKit
import akka.testkit.TestProbe
import akka.util.JavaVersion
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

object VirtualThreadDispatcherSpec {
  final case class ThreadInfo(virtual: Boolean, name: String)

  object ThreadInfoActor {
    def props() = Props(new ThreadInfoActor)
  }
  private class ThreadInfoActor extends Actor {
    override def receive: Receive = {
      case "give-me-info" =>
        sender() ! reflectiveVirtualThreadInfo()
    }
  }

  private def reflectiveVirtualThreadInfo(): ThreadInfo = {
    val thread = Thread.currentThread()
    // can't use methods directly or test won't compile on jdk < 21
    val isVirtualMethod = thread.getClass.getMethod("isVirtual")
    val isVirtual = isVirtualMethod.invoke(thread).asInstanceOf[Boolean]
    ThreadInfo(isVirtual, thread.getName)
  }
}

class VirtualThreadDispatcherSpec extends AnyWordSpec with Matchers {
  import VirtualThreadDispatcherSpec._

  "The virtual thread support" should {

    "run tasks on virtual threads" in {
      if (JavaVersion.majorVersion < 21) {
        // loom not available yet here
        pending
      } else {
        implicit val system: ActorSystem = ActorSystem(
          classOf[VirtualThreadDispatcherSpec].getSimpleName,
          ConfigFactory.parseString("""
              my-vt-dispatcher {
                type = "Dispatcher"
                executor = virtual-thread-executor
              }
            """).withFallback(ConfigFactory.load()))

        try {
          val vtDispatcher = system.dispatchers.lookup("my-vt-dispatcher")
          vtDispatcher shouldBe a[BatchingExecutor]

          val threadIsVirtualProbe = TestProbe()
          vtDispatcher.execute(() => {
            threadIsVirtualProbe.ref ! reflectiveVirtualThreadInfo()
          })
          val info = threadIsVirtualProbe.expectMsgType[ThreadInfo]
          info.virtual shouldBe true
          info.name should endWith("my-vt-dispatcher")
        } finally {
          TestKit.shutdownActorSystem(system)
        }

      }
    }

    "can be used as default dispatcher" in {
      if (JavaVersion.majorVersion < 21) {
        // loom not available yet here
        pending
      } else {
        // not necessarily a good idea because of the virtual thread per task overhead, but to know it works
        // and to cover running actors on it
        implicit val system: ActorSystem = ActorSystem(
          classOf[VirtualThreadDispatcherSpec].getSimpleName,
          ConfigFactory.parseString("""
              akka.actor.default-dispatcher.executor="virtual-thread-executor"
            """).withFallback(ConfigFactory.load()))
        try {
          val echo = system.actorOf(ThreadInfoActor.props())
          val responseProbe = TestProbe()
          echo.tell("give-me-info", responseProbe.ref)
          val info = responseProbe.expectMsgType[ThreadInfo]
          info.virtual shouldBe true
          info.name should endWith("akka.actor.default-dispatcher")
        } finally {
          TestKit.shutdownActorSystem(system)
        }
      }
    }

    "can be configured with a fallback for work on all JVMs" in {
      implicit val system: ActorSystem = ActorSystem(
        classOf[VirtualThreadDispatcherSpec].getSimpleName,
        ConfigFactory.parseString("""
              my-vt-dispatcher {
                type = "Dispatcher"
                executor = virtual-thread-executor
                virtual-thread-executor {
                  fallback="fork-join-executor"
                }
              }
            """).withFallback(ConfigFactory.load()))

      try {
        val dispatcher = system.dispatchers.lookup("my-vt-dispatcher")
        val threadInfoProbe = TestProbe()
        dispatcher.execute(() => {
          threadInfoProbe.ref ! "ok"
        })
        threadInfoProbe.expectMsg("ok")
      } finally {
        TestKit.shutdownActorSystem(system)
      }
    }

    "nested works as expected" in {
      if (JavaVersion.majorVersion < 21) {
        // loom not available yet here
        pending
      } else {
        implicit val system: ActorSystem = ActorSystem(
          classOf[VirtualThreadDispatcherSpec].getSimpleName,
          ConfigFactory.parseString("""
              my-vt-dispatcher {
                type = "Dispatcher"
                executor = virtual-thread-executor
                virtual-thread-executor {
                  fallback="fork-join-executor"
                }
              }
            """).withFallback(ConfigFactory.load()))

        try {
          implicit val dispatcher: ExecutionContext = system.dispatchers.lookup("my-vt-dispatcher")
          val threadInfoProbe = TestProbe()
          // only available on JDK 19+ so we can't use without reflection
          def currentThreadId(): Long =
            classOf[Thread].getMethod("threadId").invoke(Thread.currentThread()).asInstanceOf[Long]

          Future {
            threadInfoProbe.ref ! (("parent before", currentThreadId()))
            Future {
              threadInfoProbe.ref ! (("child before", currentThreadId()))
              Thread.sleep(300)
              threadInfoProbe.ref ! (("child after", currentThreadId()))
            }
            Thread.sleep(100)
            threadInfoProbe.ref ! (("parent after", currentThreadId()))
          }
          val all = threadInfoProbe.receiveN(4)
          val items = all.collect { case (evt: String, threadId: Long) => (evt, threadId) }
          items.map(_._2).toSet should have size (2) // two different virtual threads
          items.map(_._1) shouldEqual (Seq("parent before", "child before", "parent after", "child after"))
        } finally {
          TestKit.shutdownActorSystem(system)
        }
      }
    }
  }

}
