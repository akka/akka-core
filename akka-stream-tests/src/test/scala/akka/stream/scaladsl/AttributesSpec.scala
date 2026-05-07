/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.{ CompletionStage, TimeUnit }

import scala.annotation.nowarn

import com.typesafe.config.ConfigFactory

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.dispatch.Dispatchers
import akka.stream._
import akka.stream.ActorAttributes.Dispatcher
import akka.stream.Attributes._
import akka.stream.javadsl
import akka.stream.snapshot.MaterializerState
import akka.stream.stage._
import akka.stream.testkit._
import akka.testkit.TestKit

object AttributesSpec {

  class AttributesSource(_initialAttributes: Attributes = Attributes.none)
      extends GraphStageWithMaterializedValue[SourceShape[Any], Attributes] {
    val out = Outlet[Any]("out")
    override protected def initialAttributes: Attributes = _initialAttributes
    override val shape = SourceShape.of(out)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Attributes) = {
      val logic = new GraphStageLogic(shape) {
        setHandler(out, new OutHandler {
          def onPull(): Unit = {}
        })
      }
      (logic, inheritedAttributes)
    }

  }

  class AttributesFlow(_initialAttributes: Attributes = Attributes.none)
      extends GraphStageWithMaterializedValue[FlowShape[Any, Any], Attributes] {

    val in = Inlet[Any]("in")
    val out = Outlet[Any]("out")

    override protected def initialAttributes: Attributes = _initialAttributes
    override val shape = FlowShape(in, out)
    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Attributes) = {
      val logic = new GraphStageLogic(shape) {

        setHandlers(in, out, new InHandler with OutHandler {
          override def onPush(): Unit = push(out, grab(in))
          override def onPull(): Unit = pull(in)
        })
      }

      (logic, inheritedAttributes)
    }
  }

  class AttributesSink(_initialAttributes: Attributes = Attributes.none)
      extends GraphStageWithMaterializedValue[SinkShape[Any], Attributes] {

    val in = Inlet[Any]("in")

    override protected def initialAttributes: Attributes = _initialAttributes
    override val shape = SinkShape(in)
    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Attributes) = {
      val logic = new GraphStageLogic(shape) {
        override def preStart(): Unit = {
          pull(in)
        }
        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            grab(in)
            pull(in)
          }
        })
      }

      (logic, inheritedAttributes)
    }
  }

  class ThreadNameSnitchingStage(initialDispatcher: Option[String]) extends GraphStage[SourceShape[String]] {
    def this(initialDispatcher: String) = this(Some(initialDispatcher))
    val out = Outlet[String]("out")
    override val shape = SourceShape.of(out)
    override protected def initialAttributes: Attributes =
      initialDispatcher.fold(Attributes.none)(name => ActorAttributes.dispatcher(name))
    def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(out, new OutHandler {
        def onPull(): Unit = {
          push(out, Thread.currentThread.getName)
          completeStage()
        }
      })
    }

  }

  def whateverAttribute(label: String): Attributes = Attributes(WhateverAttribute(label))
  case class WhateverAttribute(label: String) extends Attribute
}

@nowarn // tests deprecated APIs
class AttributesSpec
    extends StreamSpec(
      ConfigFactory
        .parseString("""
    my-dispatcher {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor {
        fixed-pool-size = 1
      }
      throughput = 1
    }
  """)
        // we need to revert to the regular mailbox or else the test suite will complain
        // about using non-test worthy dispatchers
        .withFallback(Utils.UnboundedMailboxConfig)) {

  import AttributesSpec._

  val settings = ActorMaterializerSettings(system).withInputBuffer(initialSize = 2, maxSize = 16)

  implicit val materializer: ActorMaterializer = ActorMaterializer(settings)

  "an attributes instance" must {

    val attributes = Attributes.name("a") and Attributes.name("b") and Attributes.inputBuffer(1, 2)

    "give access to the least specific attribute" in {
      val name = attributes.getFirst[Name]
      name should ===(Some(Attributes.Name("a")))
    }

    "give access to the most specific attribute value" in {
      val name = attributes.get[Name]
      name should ===(Some(Attributes.Name("b")))
    }

    "return a mandatory value without allocating a some" in {
      val attributes = Attributes.inputBuffer(2, 2)

      attributes.mandatoryAttribute[InputBuffer] should ===(InputBuffer(2, 2))
    }

  }

  "attributes on a graph stage" must {

    "be appended with addAttributes" in {
      val attributes =
        Source
          .fromGraph(
            new AttributesSource()
              .addAttributes(Attributes.name("new-name"))
              .addAttributes(Attributes.name("re-added")) // adding twice at same level replaces
              .addAttributes(whateverAttribute("other-thing")))
          .toMat(Sink.head)(Keep.left)
          .run()

      val name = attributes.get[Name]
      name shouldBe Some(Name("re-added"))
      val whatever = attributes.get[WhateverAttribute]
      whatever shouldBe Some(WhateverAttribute("other-thing"))
    }

    "be replaced withAttributes directly on a stage" in {
      val attributes =
        Source
          .fromGraph(
            new AttributesSource()
              .withAttributes(Attributes.name("new-name") and whateverAttribute("other-thing"))
              .withAttributes(Attributes.name("re-added")) // we loose all previous attributes for same level
          )
          .toMat(Sink.head)(Keep.left)
          .run()

      val name = attributes.get[Name]
      name shouldBe Some(Name("re-added"))
      val whatever = attributes.get[WhateverAttribute]
      whatever shouldBe empty
    }

    "be overridable on a module basis" in {
      val attributes =
        Source
          .fromGraph(new AttributesSource().withAttributes(Attributes.name("new-name")))
          .toMat(Sink.head)(Keep.left)
          .run()

      val name = attributes.get[Name]
      name shouldBe Some(Name("new-name"))
    }

    "keep the outermost attribute as the least specific" in {
      val attributes = Source
        .fromGraph(new AttributesSource(Attributes.name("original-name")))
        .map(identity)
        .addAttributes(Attributes.name("whole-graph"))
        .toMat(Sink.head)(Keep.left)
        .run()

      val mostSpecificName = attributes.get[Name]
      mostSpecificName shouldBe Some(Name("original-name"))

      val leastSpecificName = attributes.getFirst[Name]
      leastSpecificName shouldBe Some(Name("whole-graph"))
    }
  }

  "attributes on a source" must {

    "make the attributes on fromGraph(single-source-stage) Source behave the same as the stage itself" in {
      val attributes =
        Source
          .fromGraph(new AttributesSource(Attributes.name("original-name") and whateverAttribute("whatever"))
            .withAttributes(Attributes.name("new-name")))
          .toMat(Sink.head)(Keep.left)
          .run()

      val mostSpecificName = attributes.get[Name]
      mostSpecificName shouldBe Some(Name("new-name"))

      val leastSpecificName = attributes.getFirst[Name]
      leastSpecificName shouldBe Some(Name("new-name"))
    }

    "make the attributes on Source.fromGraph source behave the same as the stage itself" in {
      val attributes =
        Source
          .fromGraph(new AttributesSource(Attributes.name("original-name")))
          .withAttributes(Attributes.name("replaced")) // this actually replaces now
          .toMat(Sink.head)(Keep.left)
          .withAttributes(Attributes.name("whole-graph"))
          .run()

      val mostSpecificName = attributes.get[Name]
      mostSpecificName shouldBe Some(Name("replaced"))
      val mostSpecificWhatever = attributes.get[WhateverAttribute]
      mostSpecificWhatever shouldBe None

      val leastSpecificName = attributes.getFirst[Name]
      leastSpecificName shouldBe Some(Name("whole-graph"))
      val leastSpecificWhatever = attributes.getFirst[WhateverAttribute]
      leastSpecificWhatever shouldBe None
    }

    "not replace stage specific attributes with attributes on surrounding composite source" in {
      val attributes = Source
        .fromGraph(new AttributesSource(Attributes.name("original-name")))
        .map(identity)
        .addAttributes(Attributes.name("composite-graph"))
        .toMat(Sink.head)(Keep.left)
        .run()

      // still the original as the attribute was added on the composite source
      val mostSpecificName = attributes.get[Name]
      mostSpecificName shouldBe Some(Name("original-name"))

      val leastSpecificName = attributes.getFirst[Name]
      leastSpecificName shouldBe Some(Name("composite-graph"))
    }

    "make the attributes on Sink.fromGraph source behave the same as the stage itself" in {
      val attributes =
        Source.maybe
          .toMat(
            Sink
              .fromGraph(new AttributesSink(Attributes.name("original-name")))
              .withAttributes(Attributes.name("replaced")) // this actually replaces now
          )(Keep.right)
          .withAttributes(Attributes.name("whole-graph"))
          .run()

      val mostSpecificName = attributes.get[Name]
      mostSpecificName shouldBe Some(Name("replaced"))

      val leastSpecificName = attributes.getFirst[Name]
      leastSpecificName shouldBe Some(Name("whole-graph"))
    }

    "use the initial attributes for dispatcher" in {
      val dispatcher =
        Source.fromGraph(new ThreadNameSnitchingStage("my-dispatcher")).runWith(Sink.head).futureValue

      dispatcher should startWith("AttributesSpec-my-dispatcher")
    }

    "use an explicit attribute on the stage to select dispatcher" in {
      val dispatcher =
        Source
          .fromGraph(
            // directly on stage
            new ThreadNameSnitchingStage(ActorAttributes.IODispatcher.dispatcher)
              .addAttributes(ActorAttributes.dispatcher("my-dispatcher")))
          .runWith(Sink.head)
          .futureValue

      dispatcher should startWith("AttributesSpec-my-dispatcher")
    }

    "use the most specific dispatcher when another one is defined on a surrounding composed graph" in {
      val dispatcher =
        Source
          .fromGraph(new ThreadNameSnitchingStage(ActorAttributes.IODispatcher.dispatcher))
          .map(identity)
          // this is now for the composed source -> flow graph
          .addAttributes(ActorAttributes.dispatcher("my-dispatcher"))
          .runWith(Sink.head)
          .futureValue

      dispatcher should startWith(s"AttributesSpec-${Dispatchers.DefaultBlockingDispatcherId}")
    }

    "not change dispatcher from one defined on a surrounding graph" in {
      val dispatcher =
        Source
          .fromGraph(new ThreadNameSnitchingStage(ActorAttributes.IODispatcher.dispatcher))
          // this already introduces an async boundary here
          .map(identity)
          // this is now just for map since there already is one in-between stage and map
          .async // potential sugar .async("my-dispatcher")
          .addAttributes(ActorAttributes.dispatcher("my-dispatcher"))
          .runWith(Sink.head)
          .futureValue

      dispatcher should startWith(s"AttributesSpec-${Dispatchers.DefaultBlockingDispatcherId}")
    }

    "change dispatcher when defined directly on top of the async boundary" in {
      val dispatcher =
        Source
          .fromGraph(new ThreadNameSnitchingStage(ActorAttributes.IODispatcher.dispatcher))
          .async
          .withAttributes(ActorAttributes.dispatcher("my-dispatcher"))
          .runWith(Sink.head)
          .futureValue

      dispatcher should startWith("AttributesSpec-my-dispatcher")
    }

    "change dispatcher when defined on the async call" in {
      val dispatcher =
        Source
          .fromGraph(new ThreadNameSnitchingStage(ActorAttributes.IODispatcher.dispatcher))
          .async("my-dispatcher")
          .runWith(Sink.head)
          .futureValue

      dispatcher should startWith("AttributesSpec-my-dispatcher")
    }

    // We reverted fix #30076 for this since it had too many side effects, we need something specifically for fromPublisher()
    "get input buffer size from surrounding .addAttributes for Source.fromPublisher" in pendingUntilFixed {
      val materializer = Materializer(system) // for isolation
      try {
        val pub = TestPublisher.probe()
        Source.fromPublisher(pub).withAttributes(Attributes.inputBuffer(1, 1)).run()(materializer)

        val streamSnapshot = awaitAssert {
          val snapshot = MaterializerState.streamSnapshots(materializer).futureValue
          snapshot should have size (1) // just the one island in this case
          snapshot.head
        }

        val logics = streamSnapshot.activeInterpreters.head.logics
        val inputBoundary = logics.find(_.label.startsWith("BatchingActorInputBoundary")).get
        inputBoundary.label should include("fill=0/1,") // dodgy but see no other way to inspect from snapshot

      } finally {
        materializer.shutdown()
      }
    }
  }

  "attributes on a Flow" must {

    "make the attributes on fromGraph(flow-stage) Flow behave the same as the stage itself" in {
      val attributes =
        Source.empty
          .viaMat(Flow
            .fromGraph(new AttributesFlow(Attributes.name("original-name")))
            .withAttributes(Attributes.name("replaced")) // this actually replaces now
          )(Keep.right)
          .withAttributes(Attributes.name("source-flow"))
          .toMat(Sink.ignore)(Keep.left)
          .withAttributes(Attributes.name("whole-graph"))
          .run()

      val name = attributes.get[Name]
      name shouldBe Some(Name("replaced"))
      val firstName = attributes.getFirst[Name]
      firstName shouldBe Some(Name("whole-graph"))
    }

    "handle attributes on a composed flow" in {
      val attributes =
        Source.empty
          .viaMat(
            Flow
              .fromGraph(new AttributesFlow(Attributes.name("original-name")))
              .map(identity)
              .withAttributes(Attributes.name("replaced"))
              .addAttributes(whateverAttribute("whatever"))
              .withAttributes(Attributes.name("replaced-again"))
              .addAttributes(whateverAttribute("replaced")))(Keep.right)
          .toMat(Sink.ignore)(Keep.left)
          .run()

      // this verifies that the old docs on flow.withAttributes was in fact incorrect
      // there is no sealing going on here
      val name = attributes.get[Name]
      name shouldBe Some(Name("original-name"))
      val whatever = attributes.get[WhateverAttribute]
      whatever shouldBe Some(WhateverAttribute("replaced"))

      val firstName = attributes.getFirst[Name]
      firstName shouldBe Some(Name("replaced-again"))
      val firstWhatever = attributes.getFirst[WhateverAttribute]
      firstWhatever shouldBe Some(WhateverAttribute("replaced"))
    }

    "get input buffer size from surrounding .addAttributes (closest)" in {
      val materializer = Materializer(system) // for isolation
      try {
        val (sourcePromise, complete) = Source.maybe
          .viaMat(
            Flow[Int]
              .map { n =>
                // something else than identity so it's not optimized away
                n
              }
              .async(Dispatchers.DefaultBlockingDispatcherId)
              .addAttributes(Attributes.inputBuffer(1, 1)))(Keep.left)
          .toMat(Sink.ignore)(Keep.both)
          .run()(materializer)

        val snapshot = awaitAssert {
          val snapshot = MaterializerState.streamSnapshots(materializer).futureValue
          snapshot should have size (2) // two stream "islands", one on blocking dispatcher and one on default
          snapshot
        }

        val islandByDispatcher =
          snapshot.groupBy(_.activeInterpreters.head.logics.head.attributes.mandatoryAttribute[Dispatcher])

        val logicsOnBlocking =
          islandByDispatcher(Dispatcher(Dispatchers.DefaultBlockingDispatcherId)).head.activeInterpreters.head.logics
        val blockingInputBoundary = logicsOnBlocking.find(_.label.startsWith("BatchingActorInputBoundary")).get
        blockingInputBoundary.label should include("fill=0/1,") // dodgy but see no other way to inspect from snapshot

        sourcePromise.success(None)
        complete.futureValue // block until stream completes
      } finally {
        materializer.shutdown()
      }
    }

    "get input buffer size from surrounding .addAttributes (wrapping)" in {
      val materializer = Materializer(system) // for isolation
      try {
        val (sourcePromise, complete) = Source.maybe
          .viaMat(Flow[Int]
            .map { n =>
              // something else than identity so it's not optimized away
              n
            }
            .async(Dispatchers.DefaultBlockingDispatcherId))(Keep.left)
          .addAttributes(Attributes.inputBuffer(1, 1))
          .toMat(Sink.ignore)(Keep.both)
          .run()(SystemMaterializer(system).materializer)

        val snapshot = awaitAssert {
          val snapshot = MaterializerState.streamSnapshots(system).futureValue
          snapshot should have size (2) // two stream "islands", one on blocking dispatcher and one on default
          snapshot
        }

        val islandByDispatcher =
          snapshot.groupBy(_.activeInterpreters.head.logics.head.attributes.mandatoryAttribute[Dispatcher])

        val logicsOnBlocking =
          islandByDispatcher(Dispatcher(Dispatchers.DefaultBlockingDispatcherId)).head.activeInterpreters.head.logics
        val blockingInputBoundary = logicsOnBlocking.find(_.label.startsWith("BatchingActorInputBoundary")).get
        blockingInputBoundary.label should include("fill=0/1,") // dodgy but see no other way to inspect from snapshot

        sourcePromise.success(None)
        complete.futureValue // block until stream completes
      } finally {
        materializer.shutdown()
      }
    }

    "get input buffer size from async(dispatcher, inputBufferSize)" in {
      val materializer = Materializer(system) // for isolation
      try {
        val (sourcePromise, complete) = Source.maybe
          .viaMat(Flow[Int]
            .map { n =>
              // something else than identity so it's not optimized away
              n
            }
            .async(Dispatchers.DefaultBlockingDispatcherId, 1))(Keep.left)
          .toMat(Sink.ignore)(Keep.both)
          .run()(SystemMaterializer(system).materializer)

        val snapshot = awaitAssert {
          val snapshot = MaterializerState.streamSnapshots(system).futureValue
          snapshot should have size (2) // two stream "islands", one on blocking dispatcher and one on default
          snapshot
        }

        val islandByDispatcher =
          snapshot.groupBy(_.activeInterpreters.head.logics.head.attributes.mandatoryAttribute[Dispatcher])

        val logicsOnBlocking =
          islandByDispatcher(Dispatcher(Dispatchers.DefaultBlockingDispatcherId)).head.activeInterpreters.head.logics
        val blockingInputBoundary = logicsOnBlocking.find(_.label.startsWith("BatchingActorInputBoundary")).get
        blockingInputBoundary.label should include("fill=0/1,") // dodgy but see no other way to inspect from snapshot

        println(blockingInputBoundary)
        /* snapshot.foreach(streamSnapshot =>
          println("RUNNING:\n\t" +
            streamSnapshot.activeInterpreters.head.logics.map(l => l.label -> l.attributes).mkString("\n\t"))
        ) */

        sourcePromise.success(None)
        complete.futureValue // block until stream completes
      } finally {
        materializer.shutdown()
      }
    }

  }

  "attributes on a Sink" must {
    "make the attributes on fromGraph(sink-stage) Sink behave the same as the stage itself" in {
      val attributes =
        Source.empty
          .toMat(
            Sink
              .fromGraph(new AttributesSink(Attributes.name("original-name")))
              .withAttributes(Attributes.name("replaced")) // this actually replaces now
          )(Keep.right)
          .withAttributes(Attributes.name("whole-graph"))
          .run()

      val mostSpecificName = attributes.get[Name]
      mostSpecificName shouldBe Some(Name("replaced"))

      val leastSpecificName = attributes.getFirst[Name]
      leastSpecificName shouldBe Some(Name("whole-graph"))
    }

  }

  "attributes in the javadsl source" must {

    "not change dispatcher from one defined on a surrounding graph" in {
      val dispatcherF =
        javadsl.Source
          .fromGraph(new ThreadNameSnitchingStage(ActorAttributes.IODispatcher.dispatcher))
          // this already introduces an async boundary here
          .detach
          // this is now just for map since there already is one in-between stage and map
          .async
          .addAttributes(ActorAttributes.dispatcher("my-dispatcher"))
          .runWith(javadsl.Sink.head[String](), materializer)

      val dispatcher = dispatcherF.toCompletableFuture.get(remainingOrDefault.toMillis, TimeUnit.MILLISECONDS)

      dispatcher should startWith(s"AttributesSpec-${Dispatchers.DefaultBlockingDispatcherId}")
    }

    "change dispatcher when defined directly on top of the async boundary" in {
      val dispatcherF =
        javadsl.Source
          .fromGraph(new ThreadNameSnitchingStage(ActorAttributes.IODispatcher.dispatcher))
          .async
          .withAttributes(ActorAttributes.dispatcher("my-dispatcher"))
          .runWith(javadsl.Sink.head[String](), materializer)

      val dispatcher = dispatcherF.toCompletableFuture.get(remainingOrDefault.toMillis, TimeUnit.MILLISECONDS)

      dispatcher should startWith("AttributesSpec-my-dispatcher")
    }

    "make the attributes on Source.fromGraph source behave the same as the stage itself" in {
      val attributes: Attributes =
        javadsl.Source
          .fromGraph(new AttributesSource(Attributes.name("original-name")))
          .withAttributes(Attributes.name("replaced")) // this actually replaces now
          .toMat(javadsl.Sink.ignore(), javadsl.Keep.left[Attributes, CompletionStage[Done]])
          .withAttributes(Attributes.name("whole-graph"))
          .run(materializer)

      val mostSpecificName = attributes.get[Name]
      mostSpecificName shouldBe Some(Name("replaced"))

      val leastSpecificName = attributes.getFirst[Name]
      leastSpecificName shouldBe Some(Name("whole-graph"))
    }

    "make the attributes on Flow.fromGraph source behave the same as the stage itself" in {
      val attributes: Attributes =
        javadsl.Source
          .empty[Any]()
          .viaMat(
            javadsl.Flow
              .fromGraph(new AttributesFlow(Attributes.name("original-name")))
              .withAttributes(Attributes.name("replaced")) // this actually replaces now
            ,
            javadsl.Keep.right[NotUsed, Attributes])
          .withAttributes(Attributes.name("source-flow"))
          .toMat(javadsl.Sink.ignore(), javadsl.Keep.left[Attributes, CompletionStage[Done]])
          .withAttributes(Attributes.name("whole-graph"))
          .run(materializer)

      val mostSpecificName = attributes.get[Name]
      mostSpecificName should contain(Name("replaced"))

      val leastSpecificName = attributes.getFirst[Name]
      leastSpecificName should contain(Name("whole-graph"))
    }

    "make the attributes on Sink.fromGraph source behave the same as the stage itself" in {
      val attributes: Attributes =
        javadsl.Source
          .empty[Any]()
          .toMat(
            javadsl.Sink
              .fromGraph(new AttributesSink(Attributes.name("original-name")))
              .withAttributes(Attributes.name("replaced")) // this actually replaces now
            ,
            javadsl.Keep.right[NotUsed, Attributes])
          .withAttributes(Attributes.name("whole-graph"))
          .run(materializer)

      // most specific
      val mostSpecificName = attributes.get[Name]
      mostSpecificName should contain(Name("replaced"))

      // least specific
      val leastSpecificName = attributes.getFirst[Name]
      leastSpecificName should contain(Name("whole-graph"))
    }

  }

  "attributes on the materializer" should {

    "be defaults and not used when more specific attributes are found" in {

      // dispatcher set on the materializer
      val myDispatcherMaterializer = ActorMaterializer(settings.withDispatcher("my-dispatcher"))

      try {
        val dispatcher =
          Source
            .fromGraph(new ThreadNameSnitchingStage(ActorAttributes.IODispatcher.dispatcher))
            .runWith(Sink.head)(myDispatcherMaterializer)
            .futureValue

        // should not override stage specific dispatcher
        dispatcher should startWith("AttributesSpec-akka.actor.default-blocking-io-dispatcher")

      } finally {
        myDispatcherMaterializer.shutdown()
      }

    }

  }

  "the default dispatcher attributes" must {

    val config = ConfigFactory
      .parseString(s"""
        my-dispatcher {
          type = Dispatcher
          executor = "thread-pool-executor"
          thread-pool-executor {
            fixed-pool-size = 1
          }
          throughput = 1
        }
        my-io-dispatcher = $${my-dispatcher}
        akka.stream.materializer.dispatcher = "my-dispatcher"
        akka.stream.materializer.blocking-io-dispatcher = "my-io-dispatcher"
      """)
      // we need to revert to the regular mailbox or else the test suite will complain
      // about using non-test worthy dispatchers
      .withFallback(Utils.UnboundedMailboxConfig)
      .resolve()

    "allow for specifying a custom default dispatcher" in {

      val system = ActorSystem("AttributesSpec-default-dispatcher-override", config)
      try {

        val mat = ActorMaterializer()(system)
        val threadName =
          Source.fromGraph(new ThreadNameSnitchingStage(None)).runWith(Sink.head)(mat)

        threadName.futureValue should startWith("AttributesSpec-default-dispatcher-override-my-dispatcher-")

      } finally {
        TestKit.shutdownActorSystem(system)
      }
    }

    "use the default-io-dispatcher by default" in {
      import ActorAttributes._

      val threadName =
        Source.fromGraph(new ThreadNameSnitchingStage(None).addAttributes(Attributes(IODispatcher))).runWith(Sink.head)

      threadName.futureValue should startWith("AttributesSpec-akka.actor.default-blocking-io-dispatcher")
    }

    "allow for specifying a custom default io-dispatcher" in {
      import ActorAttributes._

      val system = ActorSystem("AttributesSpec-io-dispatcher-override", config)
      try {

        val mat = ActorMaterializer()(system)
        val threadName =
          Source
            .fromGraph(new ThreadNameSnitchingStage(None).addAttributes(Attributes(IODispatcher)))
            .runWith(Sink.head)(mat)

        threadName.futureValue should startWith("AttributesSpec-io-dispatcher-override-my-io-dispatcher-")

      } finally {
        TestKit.shutdownActorSystem(system)
      }
    }
  }

}
