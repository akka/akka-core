/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

import com.typesafe.config.ConfigFactory

import akka.Done
import akka.actor.{ Actor, ActorSystem, PoisonPill, Props }
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.stream.ActorMaterializerSpec.ActorWithMaterializer
import akka.stream.impl.{ PhasedFusingActorMaterializer, StreamSupervisor }
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.testkit.{ StreamSpec, TestPublisher }
import akka.testkit.{ ImplicitSender, TestProbe }
import akka.testkit.TestKit

object IndirectMaterializerCreation extends ExtensionId[IndirectMaterializerCreation] with ExtensionIdProvider {
  def createExtension(system: ExtendedActorSystem): IndirectMaterializerCreation =
    new IndirectMaterializerCreation(system)

  def lookup: ExtensionId[IndirectMaterializerCreation] = this
}

@nowarn
class IndirectMaterializerCreation(ex: ExtendedActorSystem) extends Extension {
  // extension instantiation blocked on materializer (which has Await.result inside)
  implicit val mat: ActorMaterializer = ActorMaterializer()(ex)

  def futureThing(n: Int): Future[Int] = {
    Source.single(n).runWith(Sink.head)
  }

}

@nowarn
class ActorMaterializerSpec extends StreamSpec with ImplicitSender {

  "ActorMaterializer" must {

    "not suffer from deadlock" in {
      val n = 4
      implicit val deadlockSystem = ActorSystem(
        "ActorMaterializerSpec-deadlock",
        ConfigFactory.parseString(s"""
          akka.actor.default-dispatcher {
            executor = "fork-join-executor"
            fork-join-executor {
              parallelism-min = $n
              parallelism-factor = 0.5
              parallelism-max = $n
            }
          }
          # undo stream testkit specific dispatcher and run "normally"
          akka.actor.default-mailbox.mailbox-type = "akka.dispatch.UnboundedMailbox"
          akka.stream.materializer.dispatcher = "akka.actor.default-dispatcher"
          """))
      try {
        import deadlockSystem.dispatcher

        // tricky part here is that the concurrent access is to the extension
        // so the threads are indirectly blocked and not waiting for the Await.result(ask) directly.
        val result = Future.sequence((1 to (n + 1)).map(n =>
          Future {
            IndirectMaterializerCreation(deadlockSystem).mat
          }))

        // with starvation these fail
        result.futureValue.size should ===(n + 1)

      } finally {
        TestKit.shutdownActorSystem(deadlockSystem)
      }
    }

    "report shutdown status properly" in {
      val m = ActorMaterializer.create(system)

      m.isShutdown should ===(false)
      m.shutdown()
      m.isShutdown should ===(true)
    }

    "properly shut down actors associated with it" in {
      val m = ActorMaterializer.create(system)

      val f = Source.fromPublisher(TestPublisher.probe[Int]()(system)).runFold(0)(_ + _)(m)

      m.shutdown()

      an[AbruptTerminationException] should be thrownBy Await.result(f, 3.seconds)
    }

    "refuse materialization after shutdown" in {
      val m = ActorMaterializer.create(system)
      m.shutdown()
      (the[IllegalStateException] thrownBy {
        Source(1 to 5).runWith(Sink.ignore)(m)
      } should have).message("Trying to materialize stream after materializer has been shutdown")
    }

    "refuse materialization when shutdown while materializing" in {
      val m = ActorMaterializer.create(system)

      (the[IllegalStateException] thrownBy {
        Source(1 to 5)
          .mapMaterializedValue { _ =>
            // shutdown while materializing
            m.shutdown()
            Thread.sleep(100)
          }
          .runWith(Sink.ignore)(m)
      } should have).message("Materializer shutdown while materializing stream")
    }

    "shut down the supervisor actor it encapsulates" in {
      val m = ActorMaterializer.create(system).asInstanceOf[PhasedFusingActorMaterializer]

      Source.maybe[Any].to(Sink.ignore).run()(m)
      m.supervisor ! StreamSupervisor.GetChildren
      expectMsgType[StreamSupervisor.Children]
      m.shutdown()

      m.supervisor ! StreamSupervisor.GetChildren
      expectNoMessage(1.second)
    }

    "terminate if ActorContext it was created from terminates" in {
      val p = TestProbe()

      val a = system.actorOf(Props(new ActorWithMaterializer(p)).withDispatcher("akka.test.stream-dispatcher"))

      p.expectMsg("hello")
      a ! PoisonPill
      p.expectMsgType[Try[Done]].isFailure should ===(true)
    }

    "report correctly if it has been shut down from the side" in {
      val sys = ActorSystem()
      val m = ActorMaterializer.create(sys)
      Await.result(sys.terminate(), Duration.Inf)
      m.isShutdown should ===(true)
    }
  }

}

object ActorMaterializerSpec {

  @nowarn("msg=deprecated")
  class ActorWithMaterializer(p: TestProbe) extends Actor {
    private val settings: ActorMaterializerSettings =
      ActorMaterializerSettings(context.system).withDispatcher("akka.test.stream-dispatcher")
    implicit val mat: ActorMaterializer = ActorMaterializer(settings)(context)

    Source
      .repeat("hello")
      .take(1)
      .concat(Source.maybe)
      .map(p.ref ! _)
      .runWith(Sink.onComplete(signal => {
        p.ref ! signal
      }))

    def receive = Actor.emptyBehavior
  }
}
