/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.Done
import akka.stream.ActorAttributes
import akka.stream.Materializer
import akka.stream.Supervision
import akka.stream.SystemMaterializer
import akka.stream.impl.PhasedFusingActorMaterializer
import akka.stream.impl.StreamSupervisor
import akka.stream.impl.StreamSupervisor.Children
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.Utils._
import akka.testkit.TestLatch
import akka.testkit.TestProbe

object UnfoldResourceAsyncSourceSpec {

  class ResourceDummy[T](
      values: Seq[T],
      // these can be used to control when the resource creates, reads first element and completes closing
      createFuture: Future[Done] = Future.successful(Done),
      firstReadFuture: Future[Done] = Future.successful(Done),
      closeFuture: Future[Done] = Future.successful(Done))(implicit ec: ExecutionContext) {
    private val iterator = values.iterator
    private val createdP = Promise[Done]()
    private val closedP = Promise[Done]()
    private val firstReadP = Promise[Done]()

    // these can be used to observe when the resource calls has happened
    val created: Future[Done] = createdP.future
    val firstElementRead: Future[Done] = firstReadP.future
    val closed: Future[Done] = closedP.future

    def create: Future[ResourceDummy[T]] = {
      createdP.trySuccess(Done)
      createFuture.map(_ => this)
    }

    def read: Future[Option[T]] = {
      if (!firstReadP.isCompleted) firstReadP.trySuccess(Done)
      firstReadFuture.map { _ =>
        if (iterator.hasNext) Some(iterator.next())
        else None
      }
    }

    def close(): Future[Done] = {
      closedP.trySuccess(Done)
      closeFuture
    }
  }
}

class UnfoldResourceAsyncSourceSpec extends StreamSpec(UnboundedMailboxConfig) {

  import UnfoldResourceAsyncSourceSpec._
  import system.dispatcher

  "Unfold Resource Async Source" must {
    "unfold data from a resource" in {
      val createPromise = Promise[Done]()
      val closePromise = Promise[Done]()

      val values = 0 to 1000
      val resource =
        new ResourceDummy[Int](values, createFuture = createPromise.future, closeFuture = closePromise.future)

      val probe = TestSubscriber.probe[Int]()
      Source
        .unfoldResourceAsync[Int, ResourceDummy[Int]](() => resource.create, _.read, _.close())
        .runWith(Sink.fromSubscriber(probe))

      probe.request(1)
      resource.created.futureValue
      probe.expectNoMessage(200.millis)
      createPromise.success(Done)

      values.foreach { n =>
        resource.firstElementRead.futureValue
        probe.expectNext() should ===(n)
        probe.request(1)
      }

      resource.closed.futureValue
      closePromise.success(Done)

      probe.expectComplete()
    }

    "close resource successfully right after open" in {
      val probe = TestSubscriber.probe[Int]()
      val firstRead = Promise[Done]()
      val resource = new ResourceDummy[Int](1 :: Nil, firstReadFuture = firstRead.future)

      Source
        .unfoldResourceAsync[Int, ResourceDummy[Int]](() => resource.create, _.read, _.close())
        .runWith(Sink.fromSubscriber(probe))

      probe.request(1L)
      resource.firstElementRead.futureValue
      // we cancel before we complete first read (racy)
      probe.cancel()
      Thread.sleep(100)
      firstRead.success(Done)

      resource.closed.futureValue
    }

    "fail when create throws exception" in {
      val probe = TestSubscriber.probe[Unit]()
      Source
        .unfoldResourceAsync[Unit, Unit](() => throw TE("create failed"), _ => ???, _ => ???)
        .runWith(Sink.fromSubscriber(probe))

      probe.ensureSubscription()
      probe.expectError(TE("create failed"))
    }

    "fail when create returns failed future" in {
      val probe = TestSubscriber.probe[Unit]()
      Source
        .unfoldResourceAsync[Unit, Unit](() => Future.failed(TE("create failed")), _ => ???, _ => ???)
        .runWith(Sink.fromSubscriber(probe))

      probe.ensureSubscription()
      probe.expectError(TE("create failed"))
    }

    "fail when close throws exception" in {
      val probe = TestSubscriber.probe[Unit]()
      Source
        .unfoldResourceAsync[Unit, Unit](
          () => Future.successful(()),
          _ => Future.successful[Option[Unit]](None),
          _ => throw TE(""))
        .runWith(Sink.fromSubscriber(probe))
      probe.ensureSubscription()
      probe.request(1L)
      probe.expectError()
    }

    "fail when close returns failed future" in {
      val probe = TestSubscriber.probe[Unit]()
      Source
        .unfoldResourceAsync[Unit, Unit](
          () => Future.successful(()),
          _ => Future.successful[Option[Unit]](None),
          _ => Future.failed(TE("")))
        .runWith(Sink.fromSubscriber(probe))
      probe.ensureSubscription()
      probe.request(1L)
      probe.expectError()
    }

    "continue when Strategy is Resume and read throws" in {
      val result = Source
        .unfoldResourceAsync[Int, Iterator[Any]](
          () => Future.successful(List[Any](1, 2, TE("read-error"), 3).iterator),
          iterator =>
            if (iterator.hasNext) {
              iterator.next() match {
                case n: Int     => Future.successful(Some(n))
                case e: TE      => throw e
                case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
              }
            } else Future.successful(None),
          _ => Future.successful(Done))
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
        .runWith(Sink.seq)

      result.futureValue should ===(Seq(1, 2, 3))
    }

    "continue when Strategy is Resume and read returns failed future" in {
      val result = Source
        .unfoldResourceAsync[Int, Iterator[Any]](
          () => Future.successful(List[Any](1, 2, TE("read-error"), 3).iterator),
          iterator =>
            if (iterator.hasNext) {
              iterator.next() match {
                case n: Int     => Future.successful(Some(n))
                case e: TE      => Future.failed(e)
                case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
              }
            } else Future.successful(None),
          _ => Future.successful(Done))
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
        .runWith(Sink.seq)

      result.futureValue should ===(Seq(1, 2, 3))
    }

    "close and open stream again when Strategy is Restart and read throws" in {
      @volatile var failed = false
      val startCount = new AtomicInteger(0)

      val result = Source
        .unfoldResourceAsync[Int, Iterator[Int]](
          () =>
            Future.successful {
              startCount.incrementAndGet()
              List(1, 2, 3).iterator
            },
          reader =>
            if (!failed) {
              failed = true
              throw TE("read-error")
            } else if (reader.hasNext) Future.successful(Some(reader.next()))
            else Future.successful(None),
          _ => Future.successful(Done))
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
        .runWith(Sink.seq)

      result.futureValue should ===(Seq(1, 2, 3))
      startCount.get should ===(2)
    }

    "close and open stream again when Strategy is Restart and read returns failed future" in {
      @volatile var failed = false
      val startCount = new AtomicInteger(0)

      val result = Source
        .unfoldResourceAsync[Int, Iterator[Int]](
          () =>
            Future.successful {
              startCount.incrementAndGet()
              List(1, 2, 3).iterator
            },
          reader =>
            if (!failed) {
              failed = true
              Future.failed(TE("read-error"))
            } else if (reader.hasNext) Future.successful(Some(reader.next()))
            else Future.successful(None),
          _ => Future.successful(Done))
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
        .runWith(Sink.seq)

      result.futureValue should ===(Seq(1, 2, 3))
      startCount.get should ===(2)
    }

    "fail stream when restarting and close throws" in {
      val out = TestSubscriber.probe[Int]()
      Source
        .unfoldResourceAsync[Int, Iterator[Int]](
          () => Future.successful(List(1, 2, 3).iterator),
          _ => throw TE("read-error"),
          _ => throw new TE("close-error"))
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
        .runWith(Sink.fromSubscriber(out))

      out.request(1)
      out.expectError().getMessage should ===("close-error")
    }

    "fail stream when restarting and close returns failed future" in {
      val out = TestSubscriber.probe[Int]()
      Source
        .unfoldResourceAsync[Int, Iterator[Int]](
          () => Future.successful(List(1, 2, 3).iterator),
          _ => throw TE("read-error"),
          _ => Future.failed(new TE("close-error")))
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
        .runWith(Sink.fromSubscriber(out))

      out.request(1)
      out.expectError().getMessage should ===("close-error")
    }

    "fail stream when restarting and start throws" in {
      val startCounter = new AtomicInteger(0)
      val out = TestSubscriber.probe[Int]()
      Source
        .unfoldResourceAsync[Int, Iterator[Int]](
          () =>
            if (startCounter.incrementAndGet() < 2) Future.successful(List(1, 2, 3).iterator)
            else throw TE("start-error"),
          _ => throw TE("read-error"),
          _ => Future.successful(Done))
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
        .runWith(Sink.fromSubscriber(out))

      out.request(1)
      out.expectError().getMessage should ===("start-error")
    }

    "fail stream when restarting and start returns failed future" in {
      val startCounter = new AtomicInteger(0)
      val out = TestSubscriber.probe[Int]()
      Source
        .unfoldResourceAsync[Int, Iterator[Int]](
          () =>
            if (startCounter.incrementAndGet() < 2) Future.successful(List(1, 2, 3).iterator)
            else Future.failed(TE("start-error")),
          _ => throw TE("read-error"),
          _ => Future.successful(Done))
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
        .runWith(Sink.fromSubscriber(out))

      out.request(1)
      out.expectError().getMessage should ===("start-error")
    }

    "use dedicated blocking-io-dispatcher by default" in {
      Source
        .unfoldResourceAsync[String, Unit](
          () => Promise[Unit]().future, // never complete
          _ => ???,
          _ => ???)
        .runWith(Sink.ignore)

      SystemMaterializer(system).materializer
        .asInstanceOf[PhasedFusingActorMaterializer]
        .supervisor
        .tell(StreamSupervisor.GetChildren, testActor)
      val ref = expectMsgType[Children].children.find(_.path.toString contains "unfoldResourceSourceAsync").get
      assertDispatcher(ref, ActorAttributes.IODispatcher.dispatcher)
    }

    "close resource when stream is abruptly terminated" in {
      import system.dispatcher
      val closeLatch = TestLatch(1)
      val mat = Materializer(system)
      val p = Source
        .unfoldResourceAsync[String, Unit](
          () => Future.successful(()),
          // a slow trickle of elements that never ends
          _ => akka.pattern.after(100.millis, system.scheduler)(Future.successful(Some("element"))),
          _ =>
            Future.successful {
              closeLatch.countDown()
              Done
            })
        .runWith(Sink.asPublisher(false))(mat)
      val c = TestSubscriber.manualProbe[String]()
      p.subscribe(c)

      mat.shutdown()

      Await.ready(closeLatch, remainingOrDefault)
    }

    // these two reproduces different aspects of #24839
    "close resource when stream is quickly cancelled" in {
      val closePromise = Promise[Done]()
      Source
        .unfoldResourceAsync[String, Unit](
          // delay it a bit to give cancellation time to come upstream
          () => akka.pattern.after(100.millis, system.scheduler)(Future.successful(())),
          _ => Future.successful(Some("whatever")),
          _ => closePromise.success(Done).future)
        .runWith(Sink.cancelled)

      closePromise.future.futureValue should ===(Done)
    }

    "close resource when stream is quickly cancelled reproducer 2" in {
      val closed = Promise[Done]()
      Source
        .unfoldResourceAsync[String, Iterator[String]]({ () =>
          Future(Iterator("a", "b", "c"))
        }, { m =>
          Future(if (m.hasNext) Some(m.next()) else None)
        }, { _ =>
          closed.success(Done).future
        })
        .map(m => println(s"Elem=> $m"))
        .runWith(Sink.cancelled)

      closed.future.futureValue // will timeout if bug is still here
    }

    "close the resource when reading an element returns a failed future" in {
      val closeProbe = TestProbe()
      val probe = TestSubscriber.probe[Unit]()
      Source
        .unfoldResourceAsync[Unit, Unit](() => Future.successful(()), _ => Future.failed(TE("read failed")), { _ =>
          closeProbe.ref ! "closed"
          Future.successful(Done)
        })
        .runWith(Sink.fromSubscriber(probe))
      probe.ensureSubscription()
      probe.request(1L)
      probe.expectError()
      closeProbe.expectMsg("closed")
    }

    "close the resource when reading an element throws" in {
      val closeProbe = TestProbe()
      val probe = TestSubscriber.probe[Unit]()
      Source
        .unfoldResourceAsync[Unit, Unit](() => Future.successful(()), _ => throw TE("read failed"), { _ =>
          closeProbe.ref ! "closed"
          Future.successful(Done)
        })
        .runWith(Sink.fromSubscriber(probe))
      probe.ensureSubscription()
      probe.request(1L)
      probe.expectError()
      closeProbe.expectMsg("closed")
    }
  }

}
