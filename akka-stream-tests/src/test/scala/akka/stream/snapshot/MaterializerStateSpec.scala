/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.snapshot

import java.net.InetSocketAddress
import javax.net.ssl.SSLContext

import scala.concurrent.Promise

import akka.stream.{ FlowShape, Materializer }
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Merge, Partition, Sink, Source, Tcp }
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.AkkaSpec

class MaterializerStateSpec extends AkkaSpec() {

  "The MaterializerSnapshotting" must {

    "snapshot a running stream" in {
      implicit val mat = Materializer(system)
      try {
        Source.maybe[Int].map(_.toString).zipWithIndex.runWith(Sink.seq)

        awaitAssert({
          val snapshot = MaterializerState.streamSnapshots(mat).futureValue

          snapshot should have size (1)
          snapshot.head.activeInterpreters should have size (1)
          snapshot.head.activeInterpreters.head.logics should have size (4) // all 4 operators
        }, remainingOrDefault)
      } finally {
        mat.shutdown()
      }
    }

    "snapshot a running stream on the default dispatcher" in {
      val promise = Promise[Int]()
      Source.future(promise.future).map(_.toString).zipWithIndex.runWith(Sink.seq)

      awaitAssert({
        val snapshot = MaterializerState.streamSnapshots(system).futureValue

        snapshot should have size (1)
        snapshot.head.activeInterpreters should have size (1)
        snapshot.head.activeInterpreters.head.logics should have size (4) // all 4 operators
      }, remainingOrDefault)
      promise.success(1)
    }

    "snapshot a running stream that includes a TLSActor" in {
      Source.never
        .via(Tcp(system).outgoingConnectionWithTls(InetSocketAddress.createUnresolved("akka.io", 443), () => {
          val engine = SSLContext.getDefault.createSSLEngine("akka.io", 443)
          engine.setUseClientMode(true)
          engine
        }))
        .runWith(Sink.seq)

      val snapshots = MaterializerState.streamSnapshots(system).futureValue
      snapshots.size should be(2)
      snapshots.toString should include("TLS-")
    }

    "snapshot a stream that has a stopped stage" in {
      implicit val mat = Materializer(system)
      try {
        val probe = TestSink[String]()(system)
        val out = Source
          .single("one")
          .concat(Source.maybe[String]) // make sure we leave it running
          .runWith(probe)
        out.requestNext("one")
        awaitAssert({
          val snapshot = MaterializerState.streamSnapshots(mat).futureValue
          snapshot should have size (1)
          snapshot.head.activeInterpreters should have size (1)
          snapshot.head.activeInterpreters.head.stoppedLogics should have size (2) // Source.single and a detach
        }, remainingOrDefault)

      } finally {
        mat.shutdown()
      }
    }

    "snapshot a more complicated graph" in {
      implicit val mat = Materializer(system)
      try {
        // snapshot before anything is running
        MaterializerState.streamSnapshots(mat).futureValue

        val graph = Flow.fromGraph(GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._
          val partition = b.add(Partition[String](4, {
            case "green" => 0
            case "red"   => 1
            case "blue"  => 2
            case _       => 3
          }))
          val merge = b.add(Merge[String](4, eagerComplete = false))
          val discard = b.add(Sink.ignore.async)
          val one = b.add(Source.single("purple"))

          partition.out(0) ~> merge.in(0)
          partition.out(1).via(Flow[String].map(_.toUpperCase()).async) ~> merge.in(1)
          partition.out(2).groupBy(2, identity).mergeSubstreams ~> merge.in(2)
          partition.out(3) ~> discard

          one ~> merge.in(3)

          FlowShape(partition.in, merge.out)
        })

        val callMeMaybe =
          Source.maybe[String].viaMat(graph)(Keep.left).toMat(Sink.ignore)(Keep.left).run()

        // just check that we can snapshot without errors
        MaterializerState.streamSnapshots(mat).futureValue
        callMeMaybe.success(Some("green"))
        MaterializerState.streamSnapshots(mat).futureValue
        Thread.sleep(100) // just to give it a bigger chance to cover different states of shutting down
        MaterializerState.streamSnapshots(mat).futureValue

      } finally {
        mat.shutdown()
      }
    }

  }

}
