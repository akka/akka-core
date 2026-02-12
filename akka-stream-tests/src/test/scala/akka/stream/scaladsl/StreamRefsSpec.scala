/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable
import scala.concurrent.{ Await, Future }
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import com.typesafe.config._
import akka.{ Done, NotUsed }
import akka.actor.{ Actor, ActorIdentity, ActorLogging, ActorRef, ActorSystem, ActorSystemImpl, Identify, Props }
import akka.actor.Status.Failure
import akka.pattern._
import akka.stream._
import akka.stream.impl.streamref.{ SinkRefImpl, SourceRefImpl }
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.Utils.TE
import akka.stream.testkit.scaladsl._
import akka.testkit.{ AkkaSpec, TestKit, TestProbe }
import akka.util.ByteString
import org.scalatest.concurrent.Eventually

object StreamRefsSpec {

  object DataSourceActor {
    def props(): Props = Props(new DataSourceActor()).withDispatcher("akka.test.stream-dispatcher")
  }

  case class Command(cmd: String, probe: ActorRef)

  class DataSourceActor() extends Actor with ActorLogging {

    import context.dispatcher
    import context.system

    def receive = {
      case "give" =>
        /*
         * Here we're able to send a source to a remote recipient
         *
         * For them it's a Source; for us it is a Sink we run data "into"
         */
        val source: Source[String, NotUsed] = Source(List("hello", "world"))
        val ref: SourceRef[String] = source.runWith(StreamRefs.sourceRef())

        sender() ! ref

      case "give-nothing-watch" =>
        val source: Source[String, NotUsed] = Source.future(Future.never.mapTo[String])
        val (done: Future[Done], ref: SourceRef[String]) =
          source.watchTermination()(Keep.right).toMat(StreamRefs.sourceRef())(Keep.both).run()

        sender() ! ref

        import context.dispatcher
        done.pipeTo(sender())

      case "give-only-one-watch" =>
        val source: Source[String, NotUsed] = Source.single("hello").concat(Source.future(Future.never))
        val (done: Future[Done], ref: SourceRef[String]) =
          source.watchTermination()(Keep.right).toMat(StreamRefs.sourceRef())(Keep.both).run()

        sender() ! ref

        import context.dispatcher
        done.pipeTo(sender())

      case "give-infinite" =>
        val source: Source[String, NotUsed] = Source.fromIterator(() => Iterator.from(1)).map("ping-" + _)
        val (_: NotUsed, ref: SourceRef[String]) = source.toMat(StreamRefs.sourceRef())(Keep.both).run()

        sender() ! ref

      case "give-fail" =>
        val ref = Source.failed[String](new Exception("Booooom!") with NoStackTrace).runWith(StreamRefs.sourceRef())
        sender() ! ref

      case "give-complete-asap" =>
        val ref = Source.empty.runWith(StreamRefs.sourceRef())
        sender() ! ref

      case "give-maybe" =>
        val ((maybe, termination), sourceRef) =
          Source.maybe[String].watchTermination()(Keep.both).toMat(StreamRefs.sourceRef())(Keep.both).run()
        sender() ! (maybe -> sourceRef)
        termination.pipeTo(sender())

      case "give-subscribe-timeout" =>
        val ref = Source
          .repeat("is anyone there?")
          .toMat(StreamRefs.sourceRef())(Keep.right) // attributes like this so they apply to the Sink.sourceRef
          .withAttributes(StreamRefAttributes.subscriptionTimeout(500.millis))
          .run()
        sender() ! ref

      case Command("receive", probe) =>
        /*
         * We write out code, knowing that the other side will stream the data into it.
         *
         * For them it's a Sink; for us it's a Source.
         */
        val sink =
          StreamRefs.sinkRef[String]().to(Sink.actorRef(probe, "<COMPLETE>", f => "<FAILED>: " + f.getMessage)).run()
        sender() ! sink

      case Command("receive-one-cancel", probe) =>
        // will shutdown the stream after the first element using a kill switch
        val (sink, done) =
          StreamRefs
            .sinkRef[String]()
            .viaMat(KillSwitches.single)(Keep.both)
            .alsoToMat(Sink.head)(Keep.both)
            .mapMaterializedValue {
              case ((sink, ks), firstF) =>
                // shutdown the stream after first element
                firstF.foreach(_ => ks.shutdown())(context.dispatcher)
                sink
            }
            .watchTermination()(Keep.both)
            .to(Sink.actorRef(probe, "<COMPLETE>", f => "<FAILED>: " + f.getMessage))
            .run()
        sender() ! sink
        done.pipeTo(sender())

      case "receive-ignore" =>
        val sink =
          StreamRefs.sinkRef[String]().to(Sink.ignore).run()
        sender() ! sink

      case Command("receive-subscribe-timeout", probe) =>
        val sink = StreamRefs
          .sinkRef[String]()
          .withAttributes(StreamRefAttributes.subscriptionTimeout(500.millis))
          .to(Sink.actorRef(probe, "<COMPLETE>", f => "<FAILED>: " + f.getMessage))
          .run()
        sender() ! sink

      case Command("receive-32", probe) =>
        val (sink, driver) = StreamRefs.sinkRef[String]().toMat(TestSink()(context.system))(Keep.both).run()

        import context.dispatcher
        Future {
          driver.ensureSubscription()
          driver.request(2)
          driver.expectNext()
          driver.expectNext()
          driver.expectNoMessage(100.millis)
          driver.request(30)
          driver.expectNextN(30)

          "<COMPLETED>"
        }.pipeTo(probe)

        sender() ! sink

    }
  }

  // -------------------------

  final case class SourceMsg(dataSource: SourceRef[String])
  final case class BulkSourceMsg(dataSource: SourceRef[ByteString])
  final case class SinkMsg(dataSink: SinkRef[String])
  final case class BulkSinkMsg(dataSink: SinkRef[ByteString])

  def config(): Config = {
    ConfigFactory.parseString("""
    akka {
      loglevel = DEBUG

      actor {
        provider = remote

        default-mailbox.mailbox-type = "akka.dispatch.UnboundedMailbox"
      }
      remote {
        artery.canonical.port = 0
        artery.canonical.hostname = localhost
        use-unsafe-remote-features-outside-cluster = on
      }
    }
  """).withFallback(ConfigFactory.load())
  }

  object SnitchActor {
    def props(probe: ActorRef) = Props(new SnitchActor(probe))
  }
  class SnitchActor(probe: ActorRef) extends Actor {
    def receive = {
      case msg => probe ! msg
    }
  }
}

class StreamRefsSpec extends AkkaSpec(StreamRefsSpec.config()) with Eventually {
  import StreamRefsSpec._

  val remoteSystem = ActorSystem("RemoteSystem", StreamRefsSpec.config())

  override protected def beforeTermination(): Unit =
    TestKit.shutdownActorSystem(remoteSystem)

  // obtain the remoteActor ref via selection in order to use _real_ remoting in this test
  val remoteActor = {
    val probe = TestProbe()(remoteSystem)
    val it = remoteSystem.actorOf(DataSourceActor.props(), "remoteActor")
    val remoteAddress = remoteSystem.asInstanceOf[ActorSystemImpl].provider.getDefaultAddress
    system.actorSelection(it.path.toStringWithAddress(remoteAddress)).tell(Identify("hi"), probe.ref)
    probe.expectMsgType[ActorIdentity].ref.get
  }

  "A SourceRef" must {

    "send messages via remoting" in {
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give", remoteProbe.ref)
      val sourceRef = remoteProbe.expectMsgType[SourceRef[String]]

      val localProbe = TestProbe()
      sourceRef.runWith(Sink.actorRef(localProbe.ref, "<COMPLETE>", ex => s"<FAILED> ${ex.getMessage}"))

      localProbe.expectMsg(5.seconds, "hello")
      localProbe.expectMsg("world")
      localProbe.expectMsg("<COMPLETE>")
    }

    "fail when remote source failed" in {
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give-fail", remoteProbe.ref)
      val sourceRef = remoteProbe.expectMsgType[SourceRef[String]]

      val localProbe = TestProbe()
      sourceRef.runWith(Sink.actorRef(localProbe.ref, "<COMPLETE>", t => "<FAILED>: " + t.getMessage))

      val f = localProbe.expectMsgType[String]
      f should include("Remote stream (")
      // actor name here, for easier identification
      f should include("failed, reason: Booooom!")
    }

    "complete properly when remote source is empty" in {
      val remoteProbe = TestProbe()(remoteSystem)
      // this is a special case since it makes sure that the remote stage is still there when we connect to it

      remoteActor.tell("give-complete-asap", remoteProbe.ref)
      val sourceRef = remoteProbe.expectMsgType[SourceRef[String]]

      val localProbe = TestProbe()
      sourceRef.runWith(Sink.actorRef(localProbe.ref, "<COMPLETE>", _ => "<FAILED>"))

      localProbe.expectMsg("<COMPLETE>")
    }

    "respect back-pressure from (implied by target Sink)" in {
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give-infinite", remoteProbe.ref)
      val sourceRef = remoteProbe.expectMsgType[SourceRef[String]]

      val probe = sourceRef.runWith(TestSink())

      probe.ensureSubscription()
      probe.expectNoMessage(100.millis)

      probe.request(1)
      probe.expectNext("ping-1")
      probe.expectNoMessage(100.millis)

      probe.request(20)
      probe.expectNextN((1 to 20).map(i => "ping-" + (i + 1)))
      probe.cancel()

      // since no demand anyway
      probe.expectNoMessage(100.millis)

      // should not cause more pulling, since we issued a cancel already
      probe.request(10)
      probe.expectNoMessage(100.millis)
    }

    "receive timeout if subscribing too late to the source ref" in {
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give-subscribe-timeout", remoteProbe.ref)
      val remoteSource: SourceRef[String] = remoteProbe.expectMsgType[SourceRef[String]]

      // not materializing it, awaiting the timeout...

      Thread.sleep(800) // the timeout is 500ms

      val probe = remoteSource.runWith(TestSink[String]()(system))

      //      val failure = p.expectMsgType[Failure]
      //      failure.cause.getMessage should include("[SourceRef-0] Remote side did not subscribe (materialize) handed out Sink reference")

      // the local "remote sink" should cancel, since it should notice the origin target actor is dead
      probe.ensureSubscription()
      val ex = probe.expectError()
      ex.getMessage should include("has terminated unexpectedly ")
    }

    // bug #24626
    "not receive subscription timeout when got subscribed" in {
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give-subscribe-timeout", remoteProbe.ref)
      val remoteSource: SourceRef[String] = remoteProbe.expectMsgType[SourceRef[String]]
      // materialize directly and start consuming, timeout is 500ms
      val eventualStrings: Future[immutable.Seq[String]] = remoteSource
        .throttle(1, 100.millis, 1, ThrottleMode.Shaping)
        .take(60) // 60 * 100 millis - data flowing for 6 seconds - both 500ms and 5s timeouts should have passed
        .runWith(Sink.seq)

      Await.result(eventualStrings, 8.seconds)
    }

    // bug #24934
    "not receive timeout while data is being sent" in {
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give-infinite", remoteProbe.ref)
      val remoteSource: SourceRef[String] = remoteProbe.expectMsgType[SourceRef[String]]

      val done =
        remoteSource
          .throttle(1, 200.millis)
          .takeWithin(5.seconds) // which is > than the subscription timeout (so we make sure the timeout was cancelled)
          .runWith(Sink.ignore)

      Await.result(done, 8.seconds)
    }

    "pass cancellation upstream across remoting after elements passed through" in {
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give-only-one-watch", remoteProbe.ref)
      val sourceRef = remoteProbe.expectMsgType[SourceRef[String]]

      val localProbe = TestProbe()
      val ks =
        sourceRef
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.actorRef(localProbe.ref, "<COMPLETE>", ex => s"<FAILED> ${ex.getMessage}"))
          .run()

      localProbe.expectMsg("hello")
      ks.shutdown()
      localProbe.expectMsg("<COMPLETE>")
      remoteProbe.expectMsg(Done)
    }

    // FIXME https://github.com/akka/akka-core/issues/30844
    "pass cancellation upstream across remoting before elements has been emitted" in {
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give-nothing-watch", remoteProbe.ref)
      val sourceRef = remoteProbe.expectMsgType[SourceRef[String]]

      val localProbe = TestProbe()
      val ks =
        sourceRef
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.actorRef(localProbe.ref, "<COMPLETE>", ex => s"<FAILED> ${ex.getMessage}"))
          .run()

      ks.shutdown()
      localProbe.expectMsg("<COMPLETE>")
      remoteProbe.expectMsg(Done)
    }

    "pass failure upstream across remoting before elements has been emitted" in {
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give-nothing-watch", remoteProbe.ref)
      val sourceRef = remoteProbe.expectMsgType[SourceRef[String]]

      val localProbe = TestProbe()
      val ks =
        sourceRef
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.actorRef(localProbe.ref, "<COMPLETE>", ex => s"<FAILED> ${ex.getMessage}"))
          .run()

      ks.abort(TE("det gick åt skogen"))
      localProbe.expectMsg("<FAILED> det gick åt skogen")
      remoteProbe.expectMsgType[Failure].cause shouldBe a[RemoteStreamRefActorTerminatedException]
    }

    "pass failure upstream across remoting after elements passed through" in {
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give-only-one-watch", remoteProbe.ref)
      val sourceRef = remoteProbe.expectMsgType[SourceRef[String]]

      val localProbe = TestProbe()
      val ks =
        sourceRef
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.actorRef(localProbe.ref, "<COMPLETE>", ex => s"<FAILED> ${ex.getMessage}"))
          .run()

      localProbe.expectMsg("hello")
      ks.abort(TE("det gick åt pipan"))
      localProbe.expectMsg("<FAILED> det gick åt pipan")
      remoteProbe.expectMsgType[Failure].cause shouldBe a[RemoteStreamRefActorTerminatedException]
    }

    "handle concurrent cancel and failure" in {
      // this is not possible to deterministically trigger but what we try to
      // do is have a cancel in the SourceRef and a complete on the SinkRef side happen
      // concurrently before they have managed to tell each other about it
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give-maybe", remoteProbe.ref)
      // this is somewhat weird, but we are local to the remote system with the remoteProbe so promise
      // is not sent across the wire
      val (remoteControl, sourceRef) = remoteProbe.expectMsgType[(Promise[Option[String]], SourceRef[String])]

      val localProbe = TestProbe()
      val ks =
        sourceRef
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.actorRef(localProbe.ref, "<COMPLETE>", ex => s"<FAILED> ${ex.getMessage}"))
          .run()

      // "concurrently"
      ks.shutdown()
      // note that while unlikely the killswitch shutdown can already have reached the source.maybe and in that case the
      // remoteControl is already completed here
      remoteControl.trySuccess(None)

      // since it is a race we can only confirm that it either completes or fails both sides
      // if it didn't work
      val localComplete = localProbe.expectMsgType[String]
      localComplete should startWith("<COMPLETE>").or(startWith("<FAILED>"))
      val remoteCompleted = remoteProbe.expectMsgType[AnyRef]
      remoteCompleted match {
        case Done       =>
        case Failure(_) =>
        case _          => fail()
      }
    }

    "not die to a slow and eager subscriber" in {
      import akka.stream.impl.streamref.StreamRefsProtocol._

      // GIVEN: remoteActor delivers 2 elements "hello", "world"
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("give", remoteProbe.ref)
      val sourceRefImpl = remoteProbe.expectMsgType[SourceRefImpl[String]]

      val sourceRefStageProbe = TestProbe("sourceRefStageProbe")

      // WHEN: SourceRefStage sends a first CumulativeDemand with enough demand to consume the whole stream
      sourceRefStageProbe.send(sourceRefImpl.initialPartnerRef, CumulativeDemand(10))

      // THEN: stream established with OnSubscribeHandshake
      val onSubscribeHandshake = sourceRefStageProbe.expectMsgType[OnSubscribeHandshake]
      val sinkRefStageActorRef = watch(onSubscribeHandshake.targetRef)

      // THEN: all elements are streamed to SourceRefStage
      sourceRefStageProbe.expectMsg(SequencedOnNext(0, "hello"))
      sourceRefStageProbe.expectMsg(SequencedOnNext(1, "world"))
      sourceRefStageProbe.expectMsg(RemoteStreamCompleted(2))

      // WHEN: SinkRefStage receives another CumulativeDemand, due to latency in network or slowness of sourceRefStage
      sourceRefStageProbe.send(sinkRefStageActorRef, CumulativeDemand(10))

      // THEN: SinkRefStage should not terminate
      expectNoMessage()

      // WHEN: SourceRefStage terminates
      system.stop(sourceRefStageProbe.ref)

      // THEN: SinkRefStage should terminate
      expectTerminated(sinkRefStageActorRef)
    }

  }

  "A SinkRef" must {

    "receive elements via remoting" in {
      val remoteProbe = TestProbe()(remoteSystem)
      val elementProbe = TestProbe()(remoteSystem)
      remoteActor.tell(Command("receive", elementProbe.ref), remoteProbe.ref)
      val remoteSink: SinkRef[String] = remoteProbe.expectMsgType[SinkRef[String]]

      Source("hello" :: "world" :: Nil).to(remoteSink).run()

      elementProbe.expectMsg("hello")
      elementProbe.expectMsg("world")
      elementProbe.expectMsg("<COMPLETE>")
    }

    "fail origin if remote Sink gets a failure" in {
      val remoteProbe = TestProbe()(remoteSystem)
      val elementProbe = TestProbe()(remoteSystem)
      remoteActor.tell(Command("receive", elementProbe.ref), remoteProbe.ref)
      val remoteSink: SinkRef[String] = remoteProbe.expectMsgType[SinkRef[String]]

      val remoteFailureMessage = "Booom!"
      Source.failed(new Exception(remoteFailureMessage)).to(remoteSink).run()

      val f = elementProbe.expectMsgType[String]
      f should include(s"Remote stream (")
      // actor name ere, for easier identification
      f should include(s"failed, reason: $remoteFailureMessage")
    }

    "receive hundreds of elements via remoting" in {
      val remoteProbe = TestProbe()(remoteSystem)
      val elementProbe = TestProbe()(remoteSystem)
      remoteActor.tell(Command("receive", elementProbe.ref), remoteProbe.ref)
      val remoteSink: SinkRef[String] = remoteProbe.expectMsgType[SinkRef[String]]

      val msgs = (1 to 100).toList.map(i => s"payload-$i")

      Source(msgs).runWith(remoteSink)

      msgs.foreach(t => elementProbe.expectMsg(t))
      elementProbe.expectMsg("<COMPLETE>")
    }

    "receive timeout if subscribing too late to the sink ref" in {
      val remoteProbe = TestProbe()(remoteSystem)
      val elementProbe = TestProbe()(remoteSystem)
      remoteActor.tell(Command("receive-subscribe-timeout", elementProbe.ref), remoteProbe.ref)
      val remoteSink: SinkRef[String] = remoteProbe.expectMsgType[SinkRef[String]]

      // not materializing it, awaiting the timeout...
      Thread.sleep(800) // the timeout is 500ms

      val probe = TestSource[String]()(system).to(remoteSink).run()

      val failure = elementProbe.expectMsgType[String]
      failure should include("Remote side did not subscribe (materialize) handed out Sink reference")

      // the local "remote sink" should cancel, since it should notice the origin target actor is dead
      probe.expectCancellation()
    }

    // bug #24626
    "not receive timeout if subscribing is already done to the sink ref" in {
      val remoteProbe = TestProbe()(remoteSystem)
      val elementProbe = TestProbe()(remoteSystem)
      remoteActor.tell(Command("receive-subscribe-timeout", elementProbe.ref), remoteProbe.ref)
      val remoteSink: SinkRef[String] = remoteProbe.expectMsgType[SinkRef[String]]
      Source
        .repeat("whatever")
        .throttle(1, 100.millis)
        .take(10) // the timeout is 500ms, so this makes sure we run more time than that
        .runWith(remoteSink)

      (0 to 9).foreach { _ =>
        elementProbe.expectMsg("whatever")
      }
      elementProbe.expectMsg("<COMPLETE>")
    }

    // bug #24934
    "not receive timeout while data is being sent" in {
      val remoteProbe = TestProbe()(remoteSystem)
      remoteActor.tell("receive-ignore", remoteProbe.ref)
      val remoteSink: SinkRef[String] = remoteProbe.expectMsgType[SinkRef[String]]

      val done =
        Source
          .repeat("hello-24934")
          .throttle(1, 300.millis)
          .takeWithin(5.seconds) // which is > than the subscription timeout (so we make sure the timeout was cancelled)
          .alsoToMat(Sink.last)(Keep.right)
          .to(remoteSink)
          .run()

      Await.result(done, 7.seconds)
    }

    "respect back -pressure from (implied by origin Sink)" in {
      val remoteProbe = TestProbe()(remoteSystem)
      val elementProbe = TestProbe()(remoteSystem)
      remoteActor.tell(Command("receive-32", elementProbe.ref), remoteProbe.ref)
      val sinkRef = remoteProbe.expectMsgType[SinkRef[String]]

      Source.repeat("hello").runWith(sinkRef)

      // if we get this message, it means no checks in the request/expect semantics were broken, good!
      elementProbe.expectMsg("<COMPLETED>")
    }

    "trigger local shutdown on remote shutdown" in {
      val remoteProbe = TestProbe()(remoteSystem)
      val elementProbe = TestProbe()(remoteSystem)
      remoteActor.tell(Command("receive-one-cancel", elementProbe.ref), remoteProbe.ref)
      val remoteSink: SinkRef[String] = remoteProbe.expectMsgType[SinkRef[String]]

      val done =
        Source.single("hello").concat(Source.future(Future.never)).watchTermination()(Keep.right).to(remoteSink).run()

      elementProbe.expectMsg("hello")
      elementProbe.expectMsg("<COMPLETE>")
      remoteProbe.expectMsg(Done)
      Await.result(done, 5.seconds) shouldBe Done
    }

    "not allow materializing multiple times" in {
      val remoteProbe = TestProbe()(remoteSystem)
      val elementProbe = TestProbe()(remoteSystem)
      remoteActor.tell(Command("receive", elementProbe.ref), remoteProbe.ref)
      val sinkRef = remoteProbe.expectMsgType[SinkRef[String]]

      val p1: TestPublisher.Probe[String] = TestSource[String]().to(sinkRef).run()
      val p2: TestPublisher.Probe[String] = TestSource[String]().to(sinkRef).run()

      p1.ensureSubscription()
      p1.expectRequest()

      // will be cancelled immediately, since it's 2nd:
      p2.ensureSubscription()
      p2.expectCancellation()
    }

  }

  "The StreamRefResolver" must {

    "serialize and deserialize SourceRefs" in {
      val probe = TestProbe()
      val ref = system.actorOf(StreamRefsSpec.SnitchActor.props(probe.ref))
      val sourceRef = SourceRefImpl[String](ref)
      val resolver = StreamRefResolver(system)
      val result = resolver.resolveSourceRef(resolver.toSerializationFormat(sourceRef))
      result.asInstanceOf[SourceRefImpl[String]].initialPartnerRef ! "ping"
      probe.expectMsg("ping")
    }

    "serialize and deserialize SinkRefs" in {
      val probe = TestProbe()
      val ref = system.actorOf(StreamRefsSpec.SnitchActor.props(probe.ref))
      val sinkRef = SinkRefImpl[String](ref)
      val resolver = StreamRefResolver(system)
      val result = resolver.resolveSinkRef(resolver.toSerializationFormat(sinkRef))
      result.asInstanceOf[SinkRefImpl[String]].initialPartnerRef ! "ping"
      probe.expectMsg("ping")
    }

  }

}
